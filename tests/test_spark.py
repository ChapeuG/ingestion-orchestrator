"""Partes que precisam de Spark local: criptografia, qualidade e DDL do catálogo.

Pulados automaticamente se o pyspark não estiver instalado (pip install -e .[dev]).
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ingestion.catalog import create_table_sql  # noqa: E402
from ingestion.config import QualityRule, parse_config  # noqa: E402
from ingestion.crypto import encrypt_columns  # noqa: E402
from ingestion.quality import QualityError, run_checks  # noqa: E402

KEY = base64.b64encode(b"0123456789abcdef").decode()  # AES-128 de teste
EXAMPLES = Path(__file__).resolve().parent.parent / "examples" / "ingestion-config"


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()


def test_encrypt_roundtrip_matches_transformation(spark):
    df = spark.sql("SELECT * FROM VALUES ('12345678901', 'Ana'), (CAST(NULL AS STRING), 'Bia') AS t(nu_cpf, nome)")
    out = encrypt_columns(df, ["NU_CPF"], KEY)
    assert out.columns == ["nu_cpf", "nome"]

    rows = out.orderBy("nome").collect()
    assert rows[0].nu_cpf != "12345678901" and rows[1].nu_cpf is None
    assert rows[0].nome == "Ana"

    # Mesma descriptografia usada na transformação (Scala): aes_decrypt(unbase64(col), unbase64(chave), 'ECB')
    plain = out.select(F.expr(f"cast(aes_decrypt(unbase64(nu_cpf), unbase64('{KEY}'), 'ECB') as string)").alias("v"))
    assert plain.collect()[0].v == "12345678901"


def test_encrypt_missing_column_fails(spark):
    df = spark.sql("SELECT 'a' AS nome")
    with pytest.raises(ValueError, match="nu_cpf"):
        encrypt_columns(df, ["nu_cpf"], KEY)


def test_quality_error_and_warning(spark):
    df = spark.sql("SELECT * FROM VALUES (1, CAST(NULL AS STRING)), (1, 'x') AS t(id, nome)")
    # warning não interrompe
    run_checks(df, (QualityRule("notNull", severity="warning", columns=("nome",)),), "t")
    with pytest.raises(QualityError, match="unique"):
        run_checks(df, (QualityRule("unique", columns=("ID",)),
                        QualityRule("rowCount", bounds={"mustBeGreaterThan": 0})), "t")


def test_create_table_sql(spark):
    cfg = parse_config((EXAMPLES / "sakila" / "actor.ingestion.yml").read_text(encoding="utf-8"), "prd")
    schema = spark.createDataFrame([], "actor_id int, first_name string, last_update timestamp, `@version` string").schema
    sql = create_table_sql(cfg, schema)
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS `raw_sakila`.`actor`" in sql
    assert "`actor_id` int" in sql and "`@version` string" in sql
    assert "PARTITIONED BY (`dt_ingestao` string)" in sql
    assert "LOCATION 's3://datalake-prd-raw/sakila/actor/'" in sql


@pytest.mark.skipif(sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
                    reason="escrita local do Spark no Windows exige winutils (HADOOP_HOME); roda no CI Linux")
def test_run_table_end_to_end(spark, tmp_path, monkeypatch):
    """Pipeline completo com a leitura JDBC e as secrets simuladas: qualidade → criptografia → JSON gzip → partição."""
    import yaml

    from ingestion import pipeline

    data = yaml.safe_load((EXAMPLES / "cartoes" / "cliente.ingestion.yml").read_text(encoding="utf-8"))
    data["target"]["path"] = (tmp_path / "raw").as_uri() + "/"
    cfg = parse_config(yaml.safe_dump(data), "prd")

    source = spark.sql(
        "SELECT * FROM VALUES (1, '12345678901', 'Ana', TIMESTAMP'2026-10-05 10:00:00', TIMESTAMP'2026-10-05 11:00:00') "
        "AS t(CD_CLIENTE, NU_CPF, NM_CLIENTE, DH_INCL_RGST, DH_ATLZ_RGST)")
    queries, tables = [], []

    def fake_read(spark_, config, jdbc, day, params):
        queries.append(day)
        assert jdbc["password"] == "p" and jdbc["driver"] == "oracle.jdbc.OracleDriver"
        return source.select(*[F.col(c).alias(c.lower()) for c in source.columns])

    monkeypatch.setattr(pipeline, "read_source", fake_read)
    monkeypatch.setattr(pipeline, "get_secret", lambda _id: {"username": "u", "password": "p"})
    monkeypatch.setattr(pipeline, "get_secret_field", lambda _arn, _field: KEY)
    monkeypatch.setattr(pipeline.catalog, "ensure_table", lambda s, c, schema, parts: tables.append((schema, parts)))

    params = pipeline.RunParams(env="prd", start_date="2026-10-04", end_date="2026-10-05")
    result = pipeline.run_table(spark, cfg, params)

    assert result.ok, result.error
    assert sorted(queries) == ["2026-10-04", "2026-10-05"]
    assert sorted(result.partitions) == ["20261005", "20261006"]  # reprocessamento: D grava em D+1
    files = list((tmp_path / "raw" / "dt_ingestao=20261006").glob("*.json.gz"))
    assert files, "JSON gzip não foi gravado"

    written = spark.read.json(str(tmp_path / "raw" / "dt_ingestao=20261006"))
    row = written.collect()[0]
    assert row.nu_cpf != "12345678901" and row.nm_cliente == "Ana"
    assert {"@timestamp", "@version"}.isdisjoint(written.columns)  # vêm da query JDBC real, não do fake
    assert tables and sorted(tables[0][1]) == ["20261005", "20261006"]
