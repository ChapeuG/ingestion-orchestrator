"""Leitura dos ingestion.yml: s3://bucket/arquivo.yml, s3://bucket/prefixo/ ou caminho local (testes)."""

from __future__ import annotations

from pathlib import Path

SUFFIX = ".ingestion.yml"


def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket:
        raise ValueError(f"URI S3 sem bucket: {uri}")
    return bucket, key


def _s3():
    import boto3
    return boto3.client("s3")


def list_configs(uri: str) -> list[str]:
    """Um arquivo, ou todos os *.ingestion.yml abaixo do prefixo/pasta."""
    if uri.startswith("s3://"):
        if uri.endswith((".yml", ".yaml")):
            return [uri]
        bucket, prefix = _split(uri)
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        found: list[str] = []
        for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            found += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", []) if o["Key"].endswith(SUFFIX)]
        return sorted(found)

    path = Path(uri)
    if path.is_dir():
        return sorted(str(p) for p in path.rglob(f"*{SUFFIX}"))
    if not path.exists():
        raise FileNotFoundError(uri)
    return [str(path)]


def read_text(uri: str) -> str:
    if uri.startswith("s3://"):
        bucket, key = _split(uri)
        return _s3().get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return Path(uri).read_text(encoding="utf-8")
