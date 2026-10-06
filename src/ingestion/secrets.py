"""Secrets Manager (substitui o SecretManagerUtils da lib utils)."""

from __future__ import annotations

import json


def get_secret(secret_id: str) -> dict:
    """Secret em JSON (username, password, host, port, dbname, url...)."""
    import boto3

    value = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]
    try:
        data = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError(f"Secret {secret_id} não é um JSON") from e
    if not isinstance(data, dict):
        raise ValueError(f"Secret {secret_id} não é um objeto JSON")
    return data


def get_secret_field(secret_id: str, field: str) -> str:
    """Campo da secret (ex: AES_KEY, a chave AES em base64)."""
    data = get_secret(secret_id)
    if field not in data:
        raise ValueError(f"Campo '{field}' não encontrado na secret {secret_id}")
    return str(data[field])
