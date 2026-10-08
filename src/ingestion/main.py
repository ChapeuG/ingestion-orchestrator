"""Ponto de entrada do orquestrador.

    spark-submit --packages <drivers> jobs/main.py \\
        --config s3://<bucket>/ingestion-config/<dataset>/<tabela>.ingestion.yml --env prd \\
        --raw-bucket s3://datalake-{env}-raw

--config aceita um arquivo ou um prefixo (todas as tabelas *.ingestion.yml abaixo dele).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from ingestion import dates
from ingestion.config import ConfigError, IngestionConfig, parse_config
from ingestion.storage import list_configs, read_text

log = logging.getLogger("ingestion")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dl-ingestion", description="Ingestão JDBC → raw guiada por data contract")
    p.add_argument("--config", required=True,
                   help="ingestion.yml ou prefixo (s3://bucket/ingestion-config/ ou pasta local)")
    p.add_argument("--env", required=True, help="Ambiente: escolhe o servidor e resolve {env} (dev | hml | prd)")
    p.add_argument("--start-date", default="", help="yyyy-MM-dd. Vazio = D-1 (carga do dia)")
    p.add_argument("--end-date", default="", help="yyyy-MM-dd. Vazio = start-date (ou D-1)")
    p.add_argument("--ingestion-type", default=dates.NORMAL, choices=[dates.NORMAL, dates.HISTORICAL])
    p.add_argument("--tables", default="", help="Filtra tabelas: tabela ou dataset.tabela, separadas por vírgula")
    p.add_argument("--raw-bucket", default=os.environ.get("DL_RAW_BUCKET", ""),
                   help="Bucket da raw (ex: s3://datalake-{env}-raw). Destino = <bucket>/<dataset>/<tabela>/. "
                        "Default: $DL_RAW_BUCKET. Obrigatório, exceto com --validate-only")
    p.add_argument("--validate-only", action="store_true", help="Só valida os yml (não abre Spark nem conexões)")
    return p


def load_configs(uri: str, env: str, tables: str = "", raw_bucket: str = "") -> list[IngestionConfig]:
    """Lê e valida todos os yml antes de começar (um yml inválido não deixa carga pela metade)."""
    wanted = {t.strip().lower() for t in tables.split(",") if t.strip()}
    configs, errors = [], []
    for path in list_configs(uri):
        try:
            cfg = parse_config(read_text(path), env, origin=path, raw_bucket=raw_bucket)
        except ConfigError as e:
            errors.append(str(e))
            continue
        if not wanted or cfg.table.lower() in wanted or cfg.key.lower() in wanted:
            configs.append(cfg)
    if errors:
        raise ConfigError("\n".join(errors))
    if not configs:
        raise ConfigError(f"Nenhum ingestion.yml encontrado em {uri}" + (f" para {sorted(wanted)}" if wanted else ""))
    return configs


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = build_parser().parse_args(argv)

    if not args.raw_bucket and not args.validate_only:
        log.error("Informe --raw-bucket (ou $DL_RAW_BUCKET): o destino é <bucket>/<dataset>/<tabela>/")
        return 2
    try:
        configs = load_configs(args.config, args.env, args.tables, args.raw_bucket)
    except (ConfigError, FileNotFoundError) as e:
        log.error("Configuração inválida:\n%s", e)
        return 2
    log.info("%d tabela(s) válidas: %s", len(configs), ", ".join(c.key for c in configs))
    if args.validate_only:
        return 0

    from pyspark.sql import SparkSession

    from ingestion.pipeline import RunParams, run_table

    spark = (SparkSession.builder.appName("ingestion-orchestrator")
             .config("spark.sql.session.timeZone", "UTC")
             .enableHiveSupport()
             .getOrCreate())
    params = RunParams(env=args.env, start_date=args.start_date, end_date=args.end_date,
                       ingestion_type=args.ingestion_type)

    results = [run_table(spark, cfg, params) for cfg in configs]
    for r in results:
        status = "OK " if r.ok else "ERRO"
        log.info("%s %-40s %6.1fs partições=%s %s", status, r.key, r.seconds, ",".join(r.partitions), r.error)
    failed = [r for r in results if not r.ok]
    if failed:
        log.error("%d de %d tabela(s) falharam", len(failed), len(results))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
