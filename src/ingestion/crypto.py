"""Criptografia de colunas (LGPD): AES/ECB + base64, igual ao CryptographyUtils dos projetos Scala.

O resultado é compatível com a descriptografia feita na transformação:
    aes_decrypt(unbase64(coluna), unbase64(chave), 'ECB')
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_KEY_COL = "__aes_key__"


def encrypt_columns(df: DataFrame, columns: list[str] | tuple[str, ...], secret_key: str) -> DataFrame:
    """Criptografa as colunas; nulos continuam nulos. Falha se faltar coluna, para nunca gravar em claro."""
    targets = {c.lower() for c in columns}
    missing = targets - {c.lower() for c in df.columns}
    if missing:
        raise ValueError(f"Colunas para criptografia não encontradas na origem: {', '.join(sorted(missing))}")

    # aes_encrypt via SQL: disponível desde o Spark 3.3 (a função Python só existe a partir do 3.5)
    with_key = df.withColumn(_KEY_COL, F.unbase64(F.lit(secret_key)))
    return with_key.select(*[
        F.when(F.col(f"`{c}`").isNotNull(),
               F.base64(F.expr(f"aes_encrypt(cast(`{c}` as string), {_KEY_COL}, 'ECB')"))).alias(c)
        if c.lower() in targets else F.col(f"`{c}`")
        for c in df.columns
    ])
