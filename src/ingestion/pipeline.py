"""Pipeline de ingestão de uma tabela: JDBC → qualidade → criptografia → JSON gzip na raw → catálogo.

Sem dependência de lib utils externa.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion import catalog, dates
from ingestion.config import IngestionConfig
from ingestion.connectors import get_connector
from ingestion.crypto import encrypt_columns
from ingestion.quality import run_checks
from ingestion.secrets import get_secret, get_secret_field

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunParams:
    env: str
    start_date: str = ""
    end_date: str = ""
    ingestion_type: str = dates.NORMAL

    @property
    def ongoing(self) -> bool:
        return not self.start_date


@dataclass
class TableResult:
    key: str
    partitions: list[str] = field(default_factory=list)
    seconds: float = 0.0
    error: str = ""

    @property
    def ok(self) -> bool:
        return not self.error


def days_to_process(config: IngestionConfig, params: RunParams) -> list[str]:
    days = dates.date_list(params.start_date, params.end_date)
    # Carga full não filtra por data: uma leitura só, na partição do dia mais recente
    return days[:1] if config.load_mode == "full" else days


def read_source(spark: SparkSession, config: IngestionConfig, jdbc: dict, day: str, params: RunParams) -> DataFrame:
    connector = get_connector(config.source_type)
    window = dates.window(day) if config.load_mode == "incremental" else None
    query = connector.build_query(config, window, historical=params.ingestion_type == dates.HISTORICAL)

    reader = (spark.read.format("jdbc")
              .options(**jdbc)
              .option("dbtable", query)
              .option("fetchsize", str(config.fetch_size)))

    # Leitura paralela pela coluna de partição (só com filtro por dia, que limita os bounds)
    if config.parallel_queries > 1 and config.parallel_column and window is not None:
        lower, upper = dates.parallel_bounds(day)
        log.info("Leitura paralela: %s queries por %s", config.parallel_queries, config.parallel_column)
        reader = (reader.option("numPartitions", str(config.parallel_queries))
                  .option("partitionColumn", config.parallel_column)
                  .option("lowerBound", lower)
                  .option("upperBound", upper))

    df = reader.load()
    return df.select(*[F.col(f"`{c}`").alias(c.lower()) for c in df.columns])


def process_day(spark: SparkSession, config: IngestionConfig, jdbc: dict, secret_key: str | None,
                day: str, params: RunParams) -> tuple[str, DataFrame]:
    label = f"{config.key} {day}"
    partition = dates.partition_value(day, params.ingestion_type, params.ongoing)
    log.info("[%s] leitura início (partição %s=%s)", label, config.target.partition_column, partition)

    df = read_source(spark, config, jdbc, day, params)
    if config.quality:
        df = df.persist()  # evita reler a origem: as regras e a escrita usam o mesmo dado
    try:
        run_checks(df, config.quality, label)

        # Criptografia (LGPD): o dado sensível nunca é gravado em claro na raw
        if config.encryption:
            df = encrypt_columns(df, config.encryption.columns, secret_key)
            log.info("[%s] criptografia aplicada em %s", label, ", ".join(config.encryption.columns))

        path = f"{config.target.path}{config.target.partition_column}={partition}"
        # overwrite da partição: reexecutar o mesmo dia não duplica dados (cada dia grava numa partição só)
        (df.write.mode("overwrite")
         .option("compression", config.target.compression)
         .option("ignoreNullFields", "false")
         .json(path))
        log.info("[%s] escrita fim: %s", label, path)
        return partition, df
    finally:
        if config.quality:
            df.unpersist()


def run_table(spark: SparkSession, config: IngestionConfig, params: RunParams) -> TableResult:
    result = TableResult(config.key)
    started = time.monotonic()
    try:
        connector = get_connector(config.source_type)
        jdbc = connector.jdbc_options(config.server, get_secret(config.server.secret_id))
        secret_key = (get_secret_field(config.encryption.secret_arn, config.encryption.secret_field)
                      if config.encryption else None)

        days = days_to_process(config, params)
        log.info("[%s] %s (%s) — dias: %s", config.key, config.load_mode, connector.name, ", ".join(days))

        schema = None
        for chunk in dates.chunks(days, config.parallel_days):
            with ThreadPoolExecutor(max_workers=len(chunk)) as pool:
                futures = [pool.submit(process_day, spark, config, jdbc, secret_key, d, params) for d in chunk]
                for fut in futures:
                    partition, df = fut.result()
                    result.partitions.append(partition)
                    schema = df.schema

        if schema is not None:
            catalog.ensure_table(spark, config, schema, result.partitions)
    except Exception as e:  # noqa: BLE001 — uma tabela com erro não derruba as outras
        log.exception("[%s] falhou", config.key)
        result.error = f"{type(e).__name__}: {e}"
    result.seconds = time.monotonic() - started
    return result
