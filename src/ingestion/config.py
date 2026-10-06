"""Leitura e validação do ingestion.yml (formato ingestion/v1).

O yml é gerado pelo framework dl-gen a partir do data contract (ODCS) e salvo no S3.
Aqui ele vira dataclasses imutáveis, já com {env} resolvido para o ambiente da execução.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import yaml

API_VERSION = "ingestion/v1"
PLACEHOLDER = "PREENCHER"
LOAD_MODES = ("incremental", "full")
QUALITY_CHECKS = ("notNull", "unique", "rowCount")
ROW_COUNT_BOUNDS = ("mustBeGreaterThan", "mustBeGreaterOrEqualTo", "mustBeLessThan", "mustBeLessOrEqualTo")


class ConfigError(ValueError):
    """ingestion.yml inválido: a execução não começa."""


@dataclass(frozen=True)
class Server:
    host: str
    port: str
    database: str
    secret_id: str


@dataclass(frozen=True)
class Column:
    name: str
    type: str = "string"
    physical_type: str = ""
    primary_key: bool = False
    required: bool = False
    encrypt: bool = False


@dataclass(frozen=True)
class Encryption:
    secret_arn: str
    secret_field: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class QualityRule:
    check: str
    severity: str = "error"
    columns: tuple[str, ...] = ()
    bounds: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Target:
    path: str
    database: str
    table: str
    partition_column: str
    compression: str = "gzip"


@dataclass(frozen=True)
class IngestionConfig:
    dataset: str
    table: str
    contract_id: str
    contract_version: str
    source_type: str
    source_table: str
    server: Server
    load_mode: str
    incremental_columns: tuple[str, ...]
    fetch_size: int
    parallel_days: int
    parallel_queries: int
    parallel_column: str
    columns: tuple[Column, ...]
    target: Target
    encryption: Encryption | None = None
    quality: tuple[QualityRule, ...] = ()
    env: str = ""
    description: str = ""

    @property
    def key(self) -> str:
        return f"{self.dataset}.{self.table}"


def _require(node: dict, key: str, where: str):
    value = node.get(key) if isinstance(node, dict) else None
    if value is None or value == "" or value == []:
        raise ConfigError(f"{where}.{key} é obrigatório")
    return value


def _env(value, env: str) -> str:
    return str(value).replace("{env}", env)


def _find_placeholders(node, path="") -> list[str]:
    if isinstance(node, dict):
        return [p for k, v in node.items() for p in _find_placeholders(v, f"{path}.{k}" if path else k)]
    if isinstance(node, list):
        return [p for i, v in enumerate(node) for p in _find_placeholders(v, f"{path}[{i}]")]
    return [path] if isinstance(node, str) and PLACEHOLDER in node else []


def parse_config(text: str, env: str, origin: str = "ingestion.yml") -> IngestionConfig:
    """Converte o yml em IngestionConfig, validando tudo antes de abrir qualquer conexão."""
    from ingestion.connectors import CONNECTORS  # evita import circular

    raw = yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ConfigError(f"{origin}: conteúdo não é um objeto YAML")
    if raw.get("apiVersion") != API_VERSION:
        raise ConfigError(f"{origin}: apiVersion precisa ser {API_VERSION} (recebido: {raw.get('apiVersion')})")

    source = _require(raw, "source", origin)
    load = raw.get("load") or {}
    target = _require(raw, "target", origin)
    servers = _require(source, "servers", f"{origin}.source")

    # Só o ambiente da execução precisa estar completo (dev pode existir sem prd e vice-versa)
    if env not in servers:
        raise ConfigError(f"{origin}: sem servidor para o ambiente '{env}' (disponíveis: {', '.join(servers)})")
    used = {k: v for k, v in raw.items() if k != "source"}
    used["source"] = {**source, "servers": {env: servers[env]}}
    pending = _find_placeholders(used)
    if pending:
        raise ConfigError(f"{origin}: campos ainda como {PLACEHOLDER} (complete o contrato): {', '.join(pending)}")

    source_type = str(_require(source, "type", f"{origin}.source")).lower()
    if source_type not in CONNECTORS:
        raise ConfigError(f"{origin}: conector '{source_type}' não existe. Opções: {', '.join(sorted(CONNECTORS))}")

    srv = servers[env]
    server = Server(
        host=str(srv.get("host", "")),
        port=str(srv.get("port", "")),
        database=str(srv.get("database", "")),
        secret_id=str(_require(srv, "secretId", f"{origin}.source.servers.{env}")),
    )

    mode = str(load.get("mode", "full")).lower()
    if mode not in LOAD_MODES:
        raise ConfigError(f"{origin}: load.mode '{mode}' inválido ({' | '.join(LOAD_MODES)})")
    incremental = tuple(load.get("incrementalColumns") or ())
    if mode == "incremental" and not incremental:
        raise ConfigError(f"{origin}: load.mode incremental exige load.incrementalColumns")

    columns = tuple(
        Column(
            name=str(_require(c, "name", f"{origin}.columns[{i}]")),
            type=str(c.get("type", "string")),
            physical_type=str(c.get("physicalType", "")),
            primary_key=bool(c.get("primaryKey", False)),
            required=bool(c.get("required", False)),
            encrypt=bool(c.get("encrypt", False)),
        )
        for i, c in enumerate(_require(raw, "columns", origin))
    )
    column_names = {c.name.lower() for c in columns}

    encryption = None
    encrypted_cols = tuple(c.name for c in columns if c.encrypt)
    if encrypted_cols or raw.get("encryption"):
        enc = raw.get("encryption") or {}
        enc_cols = tuple(enc.get("columns") or encrypted_cols)
        unknown = {c.lower() for c in enc_cols} - column_names
        if unknown:
            raise ConfigError(f"{origin}: encryption.columns fora de columns: {', '.join(sorted(unknown))}")
        encryption = Encryption(
            secret_arn=_env(_require(enc, "secretArn", f"{origin}.encryption"), env),
            secret_field=str(enc.get("secretField", "AES_KEY")),
            columns=enc_cols,
        )

    quality = []
    for i, q in enumerate(raw.get("quality") or []):
        check = q.get("check")
        if check not in QUALITY_CHECKS:
            raise ConfigError(f"{origin}: quality[{i}].check '{check}' inválido ({', '.join(QUALITY_CHECKS)})")
        cols = tuple(q.get("columns") or ())
        if check != "rowCount" and not cols:
            raise ConfigError(f"{origin}: quality[{i}] ({check}) exige columns")
        unknown = {c.lower() for c in cols} - column_names
        if unknown:
            raise ConfigError(f"{origin}: quality[{i}] usa colunas fora de columns: {', '.join(sorted(unknown))}")
        bounds = {k: q[k] for k in ROW_COUNT_BOUNDS if k in q}
        if check == "rowCount" and not bounds:
            raise ConfigError(f"{origin}: quality[{i}] (rowCount) exige um limite ({', '.join(ROW_COUNT_BOUNDS)})")
        severity = str(q.get("severity", "error")).lower()
        quality.append(QualityRule(check=check, severity="warning" if severity == "warning" else "error",
                                   columns=cols, bounds=bounds))

    # Coluna de partição da raw: obrigatória (vem de rawPartitionColumn no contrato)
    partition_column = str(_require(target, "partitionColumn", f"{origin}.target"))
    if not re.fullmatch(r"[a-z][a-z0-9_]*", partition_column):
        raise ConfigError(f"{origin}: target.partitionColumn '{partition_column}' inválida (minúsculo, letras, números e _)")
    if partition_column in column_names:
        raise ConfigError(f"{origin}: target.partitionColumn '{partition_column}' conflita com uma coluna da origem")

    parallel = load.get("parallel") or {}
    path = _env(_require(target, "path", f"{origin}.target"), env)
    if not path.endswith("/"):
        path += "/"

    return IngestionConfig(
        dataset=str(_require(raw, "dataset", origin)),
        table=str(_require(raw, "table", origin)),
        contract_id=str((raw.get("contract") or {}).get("id", "")),
        contract_version=str((raw.get("contract") or {}).get("version", "")),
        source_type=source_type,
        source_table=str(_require(source, "table", f"{origin}.source")),
        server=server,
        load_mode=mode,
        incremental_columns=incremental,
        fetch_size=int(load.get("fetchSize", 150000)),
        parallel_days=max(1, int(parallel.get("days", 1))),
        parallel_queries=max(1, int(parallel.get("queries", 1))),
        parallel_column=str(parallel.get("column") or (incremental[0] if incremental else "")),
        columns=columns,
        target=Target(
            path=path,
            database=_env(_require(target, "database", f"{origin}.target"), env),
            table=_env(_require(target, "table", f"{origin}.target"), env),
            partition_column=partition_column,
            compression=str(target.get("compression", "gzip")),
        ),
        encryption=encryption,
        quality=tuple(quality),
        env=env,
        description=str(raw.get("description", "")),
    )
