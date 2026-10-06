"""Validação do ingestion.yml, conectores, datas e regras de qualidade (sem Spark)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from ingestion import dates
from ingestion.config import ConfigError, QualityRule, parse_config
from ingestion.connectors import CONNECTORS, get_connector
from ingestion.main import load_configs
from ingestion.quality import evaluate

EXAMPLES = Path(__file__).resolve().parent.parent / "examples" / "ingestion-config"
ACTOR = (EXAMPLES / "sakila" / "actor.ingestion.yml").read_text(encoding="utf-8")
CLIENTE = (EXAMPLES / "cartoes" / "cliente.ingestion.yml").read_text(encoding="utf-8")


def _edit(text: str, fn) -> str:
    data = yaml.safe_load(text)
    fn(data)
    return yaml.safe_dump(data)


# ── Configuração ───────────────────────────────────────────────────────

def test_examples_are_valid():
    actor = parse_config(ACTOR, "prd")
    assert actor.key == "sakila.actor"
    assert actor.source_type == "postgres"
    assert actor.server.secret_id.endswith("prd/sakila/postgres")
    assert actor.target.path == "s3://datalake-prd-raw/sakila/actor/"
    assert actor.target.partition_column == "dt_ingestao"  # informado no contrato (rawPartitionColumn)
    assert actor.incremental_columns == ("last_update",)
    assert actor.encryption is None
    assert [r.check for r in actor.quality] == ["unique", "rowCount"]

    cliente = parse_config(CLIENTE, "prd")
    assert cliente.source_type == "oracle"
    assert cliente.encryption.columns == ("NU_CPF",)
    assert cliente.parallel_queries == 4


def test_env_selects_server_and_resolves_placeholder():
    assert parse_config(ACTOR, "dev").target.path == "s3://datalake-dev-raw/sakila/actor/"
    with pytest.raises(ConfigError, match="ambiente 'hml'"):
        parse_config(ACTOR, "hml")


def test_placeholder_blocks_execution_only_for_used_env():
    text = _edit(ACTOR, lambda d: d["source"]["servers"]["dev"].update(secretId="PREENCHER"))
    parse_config(text, "prd")  # dev incompleto não impede prd
    with pytest.raises(ConfigError, match="PREENCHER"):
        parse_config(text, "dev")


def test_invalid_configs():
    with pytest.raises(ConfigError, match="apiVersion"):
        parse_config(_edit(ACTOR, lambda d: d.update(apiVersion="v0")), "prd")
    with pytest.raises(ConfigError, match="conector 'db2'"):
        parse_config(_edit(ACTOR, lambda d: d["source"].update(type="db2")), "prd")
    with pytest.raises(ConfigError, match="incrementalColumns"):
        parse_config(_edit(ACTOR, lambda d: d["load"].update(incrementalColumns=[])), "prd")
    with pytest.raises(ConfigError, match="fora de columns"):
        parse_config(_edit(ACTOR, lambda d: d["quality"][0].update(columns=["xpto"])), "prd")
    with pytest.raises(ConfigError, match="partitionColumn é obrigatório"):
        parse_config(_edit(ACTOR, lambda d: d["target"].pop("partitionColumn")), "prd")
    with pytest.raises(ConfigError, match="partitionColumn 'Dt-Carga' inválida"):
        parse_config(_edit(ACTOR, lambda d: d["target"].update(partitionColumn="Dt-Carga")), "prd")
    with pytest.raises(ConfigError, match="conflita"):
        parse_config(_edit(ACTOR, lambda d: d["target"].update(partitionColumn="last_update")), "prd")
    with pytest.raises(ConfigError, match="PREENCHER"):
        parse_config(_edit(ACTOR, lambda d: d["target"].update(partitionColumn="PREENCHER")), "prd")
    with pytest.raises(ConfigError, match="secretArn"):
        parse_config(_edit(CLIENTE, lambda d: d.pop("encryption")), "prd")


def test_load_configs_from_folder_and_filter():
    assert {c.key for c in load_configs(str(EXAMPLES), "prd")} == {"sakila.actor", "cartoes.cliente"}
    assert [c.key for c in load_configs(str(EXAMPLES), "prd", tables="actor")] == ["sakila.actor"]
    # actor tem dev, cliente não: a pasta inteira falha antes de começar qualquer carga
    with pytest.raises(ConfigError, match="cliente"):
        load_configs(str(EXAMPLES), "dev")


# ── Conectores ─────────────────────────────────────────────────────────

def test_registered_connectors():
    assert set(CONNECTORS) == {"postgres", "oracle", "mysql", "sqlserver"}


def test_query_incremental_postgres():
    cfg = parse_config(ACTOR, "prd")
    sql = get_connector("postgres").build_query(cfg, dates.window("2026-10-05"))
    assert "t.actor_id,\n          t.first_name" in sql
    assert 'CURRENT_TIMESTAMP AS "@timestamp"' in sql
    assert "FROM public.actor t" in sql
    assert "WHERE t.last_update BETWEEN '2026-10-05 00:00:00.000' AND '2026-10-05 23:59:59.999'" in sql
    assert sql.startswith("(") and sql.endswith(") tab")


def test_query_oracle_or_columns_and_historical():
    cfg = parse_config(CLIENTE, "prd")
    conn = get_connector("oracle")
    sql = conn.build_query(cfg, dates.window("2026-10-05"))
    assert "TO_TIMESTAMP('2026-10-05 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF')" in sql
    assert "OR t.DH_ATLZ_RGST BETWEEN" in sql
    historical = conn.build_query(cfg, dates.window("2026-10-05"), historical=True)
    assert "DH_ATLZ_RGST" not in historical.split("WHERE")[1]


def test_query_full_has_no_filter():
    cfg = parse_config(_edit(ACTOR, lambda d: d["load"].update(mode="full", incrementalColumns=[])), "prd")
    assert "WHERE" not in get_connector("postgres").build_query(cfg, None)


def test_dialect_quoting():
    cfg = parse_config(ACTOR, "prd")
    assert "AS `@timestamp`" in get_connector("mysql").build_query(cfg, None)
    assert "AS [@timestamp]" in get_connector("sqlserver").build_query(cfg, None)
    assert "CAST('x' AS DATETIME2)" == get_connector("sqlserver").timestamp_literal("x")


def test_jdbc_options_priority():
    cfg = parse_config(CLIENTE, "prd")
    conn = get_connector("oracle")
    opts = conn.jdbc_options(cfg.server, {"username": "u", "password": "p"})
    assert opts["url"] == "jdbc:oracle:thin:@//cartoes-prd.example.internal:1521/CARTOESPRD"
    assert opts["user"] == "u" and opts["driver"] == "oracle.jdbc.OracleDriver"
    assert opts["oracle.jdbc.mapDateToTimestamp"] == "true"
    assert conn.jdbc_options(cfg.server, {"url": "jdbc:oracle:thin:@x"})["url"] == "jdbc:oracle:thin:@x"
    assert conn.jdbc_options(cfg.server, {"host": "h2", "port": 2, "dbname": "d2"})["url"].endswith("@//h2:2/d2")


# ── Datas (mesma regra do processor Scala) ─────────────────────────────

TODAY = date(2026, 10, 6)


def test_date_list():
    assert dates.date_list(today=TODAY) == ["2026-10-05"]
    assert dates.date_list("2026-10-01", "2026-10-03", today=TODAY) == ["2026-10-03", "2026-10-02", "2026-10-01"]
    assert dates.date_list("2026-10-01", today=TODAY) == ["2026-10-01"]
    with pytest.raises(ValueError):
        dates.date_list("2026-10-03", "2026-10-01")


def test_partition_value():
    assert dates.partition_value("2026-10-05", dates.NORMAL, ongoing=True, today=TODAY) == "20261006"
    assert dates.partition_value("2026-10-01", dates.NORMAL, ongoing=False, today=TODAY) == "20261002"
    assert dates.partition_value("2026-10-01", dates.HISTORICAL, ongoing=False, today=TODAY) == "20261001"


def test_parallel_bounds_and_chunks():
    assert dates.parallel_bounds("2026-10-31") == ("2026-10-31 00:00:00.000", "2026-11-01 01:00:00.000")
    assert dates.chunks(["a", "b", "c"], 2) == [["a", "b"], ["c"]]


# ── Qualidade ──────────────────────────────────────────────────────────

METRICS = {"rowCount": 10, "nulls": {"a": 0, "b": 2}, "duplicates": {("id",): 0, ("a", "b"): 3}}


def test_quality_evaluate():
    assert evaluate(QualityRule("notNull", columns=("A",)), METRICS).passed
    assert not evaluate(QualityRule("notNull", columns=("a", "b")), METRICS).passed
    assert evaluate(QualityRule("unique", columns=("id",)), METRICS).passed
    assert not evaluate(QualityRule("unique", columns=("a", "b")), METRICS).passed
    assert evaluate(QualityRule("rowCount", bounds={"mustBeGreaterThan": 0}), METRICS).passed
    res = evaluate(QualityRule("rowCount", bounds={"mustBeGreaterThan": 0, "mustBeLessThan": 5}), METRICS)
    assert not res.passed and "< 5" in res.detail
