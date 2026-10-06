"""Tabela da raw no Glue Data Catalog / Athena (substitui o TableUtils.createAthenaTable da lib utils).

JSON gzip lido pelo OpenX JSON SerDe. A tabela só é recriada quando o schema muda;
caso contrário, apenas as partições novas são registradas.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ingestion.config import IngestionConfig

log = logging.getLogger(__name__)

# Formato de timestamp que o Spark grava no JSON (yyyy-MM-dd'T'HH:mm:ss.SSSXXX) em notação Joda (SerDe)
JSON_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"


def _columns(schema: StructType, partition_column: str) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in schema.fields if f.name != partition_column]


def create_table_sql(config: IngestionConfig, schema: StructType) -> str:
    t = config.target
    cols = ",\n  ".join(f"`{name}` {dtype}" for name, dtype in _columns(schema, t.partition_column))
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS `{t.database}`.`{t.table}` (\n  {cols}\n)\n"
        f"PARTITIONED BY (`{t.partition_column}` string)\n"
        "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n"
        f"WITH SERDEPROPERTIES ('ignore.malformed.json' = 'false', \"timestamp.formats\" = \"{JSON_TIMESTAMP_FORMAT}\")\n"
        "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n"
        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"
        f"LOCATION '{t.path}'\n"
        f"TBLPROPERTIES ('contract.id' = '{config.contract_id}', 'contract.version' = '{config.contract_version}')"
    )


def ensure_table(spark: SparkSession, config: IngestionConfig, schema: StructType, partitions: list[str]) -> None:
    t = config.target
    full_name = f"`{t.database}`.`{t.table}`"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{t.database}`")

    expected = [(n.lower(), d) for n, d in _columns(schema, t.partition_column)]
    recreate = True
    if spark.catalog.tableExists(t.table, t.database):
        current = [(n.lower(), d) for n, d in _columns(spark.table(f"{t.database}.{t.table}").schema, t.partition_column)]
        recreate = current != expected
        if recreate:
            log.info("Schema de %s mudou: recriando a tabela", full_name)
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")  # EXTERNAL: os dados no S3 ficam

    if recreate:
        spark.sql(create_table_sql(config, schema))
        spark.sql(f"MSCK REPAIR TABLE {full_name}")
        log.info("Tabela %s criada/atualizada no catálogo", full_name)
        return

    for value in partitions:
        spark.sql(
            f"ALTER TABLE {full_name} ADD IF NOT EXISTS PARTITION (`{t.partition_column}` = '{value}') "
            f"LOCATION '{t.path}{t.partition_column}={value}/'"
        )
    log.info("Partições registradas em %s: %s", full_name, ", ".join(partitions))
