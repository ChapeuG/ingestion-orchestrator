"""Regras de qualidade do contrato, executadas antes de gravar na raw.

severity error   → interrompe a carga da tabela (nada é gravado)
severity warning → só registra no log
"""

from __future__ import annotations

import logging
import operator
from dataclasses import dataclass

from ingestion.config import QualityRule

log = logging.getLogger(__name__)

_BOUNDS = {
    "mustBeGreaterThan": (operator.gt, ">"),
    "mustBeGreaterOrEqualTo": (operator.ge, ">="),
    "mustBeLessThan": (operator.lt, "<"),
    "mustBeLessOrEqualTo": (operator.le, "<="),
}


class QualityError(RuntimeError):
    """Regra de qualidade com severity error falhou."""


@dataclass(frozen=True)
class QualityResult:
    rule: QualityRule
    passed: bool
    detail: str


def evaluate(rule: QualityRule, metrics: dict) -> QualityResult:
    """Avalia a regra a partir das métricas já calculadas (separado do Spark para testar sem cluster).

    metrics: {"rowCount": int, "nulls": {coluna: int}, "duplicates": {(colunas): int}}
    """
    if rule.check == "notNull":
        nulls = {c: metrics["nulls"][c.lower()] for c in rule.columns}
        bad = {c: n for c, n in nulls.items() if n}
        return QualityResult(rule, not bad, f"nulos: {bad}" if bad else "sem nulos")
    if rule.check == "unique":
        dup = metrics["duplicates"][tuple(c.lower() for c in rule.columns)]
        return QualityResult(rule, dup == 0, f"{dup} chaves duplicadas em {list(rule.columns)}")
    if rule.check == "rowCount":
        count = metrics["rowCount"]
        failed = [f"{sym} {v}" for k, v in rule.bounds.items() for op, sym in [_BOUNDS[k]] if not op(count, v)]
        return QualityResult(rule, not failed, f"{count} linhas" + (f" (esperado {', '.join(failed)})" if failed else ""))
    raise ValueError(f"check desconhecido: {rule.check}")


def collect_metrics(df, rules: tuple[QualityRule, ...]) -> dict:
    """Calcula numa passada o que as regras precisam (o DataFrame deve estar em cache)."""
    from pyspark.sql import functions as F

    null_cols = sorted({c.lower() for r in rules if r.check == "notNull" for c in r.columns})
    aggs = [F.count(F.lit(1)).alias("__rows__")] + [
        F.sum(F.when(F.col(f"`{c}`").isNull(), 1).otherwise(0)).alias(f"__null_{i}__") for i, c in enumerate(null_cols)
    ]
    row = df.agg(*aggs).collect()[0]
    metrics = {
        "rowCount": int(row["__rows__"]),
        "nulls": {c: int(row[f"__null_{i}__"] or 0) for i, c in enumerate(null_cols)},
        "duplicates": {},
    }
    for r in rules:
        if r.check == "unique":
            cols = tuple(c.lower() for c in r.columns)
            metrics["duplicates"][cols] = df.groupBy(*cols).count().filter(F.col("count") > 1).count()
    return metrics


def run_checks(df, rules: tuple[QualityRule, ...], label: str) -> list[QualityResult]:
    if not rules:
        return []
    metrics = collect_metrics(df, rules)
    results = [evaluate(r, metrics) for r in rules]
    errors = []
    for res in results:
        cols = f" {list(res.rule.columns)}" if res.rule.columns else ""
        msg = f"[{label}] qualidade {res.rule.check}{cols}: {res.detail}"
        if res.passed:
            log.info("✅ %s", msg)
        elif res.rule.severity == "warning":
            log.warning("⚠️ %s", msg)
        else:
            log.error("❌ %s", msg)
            errors.append(msg)
    if errors:
        raise QualityError("; ".join(errors))
    return results
