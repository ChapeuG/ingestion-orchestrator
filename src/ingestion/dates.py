"""Datas da execução.

- Sem start_date/end_date: processa D-1 ("em andamento") e grava na partição de hoje.
- Reprocessamento normal: grava o dia D na partição D+1 (o dado de D chega em D+1).
- Histórico: grava o dia D na partição D.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from ingestion.connectors.base import Window

DATE_FMT = "%Y-%m-%d"
PARTITION_FMT = "%Y%m%d"

NORMAL = "normal"
HISTORICAL = "historical"


def today_utc() -> date:
    # O EMR roda em UTC (o LocalDate.now() dos projetos Scala também)
    return datetime.now(timezone.utc).date()


def parse_date(value: str) -> date:
    return datetime.strptime(value, DATE_FMT).date()


def date_list(start_date: str = "", end_date: str = "", today: date | None = None) -> list[str]:
    """Dias a processar (mais recente primeiro, igual ao getDateList do Scala)."""
    yesterday = (today or today_utc()) - timedelta(days=1)
    start = parse_date(start_date) if start_date else yesterday
    end = parse_date(end_date) if end_date else (start if start_date else yesterday)
    if start > end:
        raise ValueError(f"start_date ({start}) depois de end_date ({end})")
    days = (end - start).days
    return [(end - timedelta(days=i)).strftime(DATE_FMT) for i in range(days + 1)]


def partition_value(day: str, ingestion_type: str, ongoing: bool, today: date | None = None) -> str:
    if ingestion_type == HISTORICAL:
        return parse_date(day).strftime(PARTITION_FMT)
    if ongoing:
        return (today or today_utc()).strftime(PARTITION_FMT)
    return (parse_date(day) + timedelta(days=1)).strftime(PARTITION_FMT)


def window(day: str) -> Window:
    return Window(start=f"{day} 00:00:00.000", end=f"{day} 23:59:59.999")


def parallel_bounds(day: str) -> tuple[str, str]:
    """lowerBound/upperBound da leitura paralela (mesma folga de 1h do GeneralUtils)."""
    upper = (parse_date(day) + timedelta(days=1)).strftime(DATE_FMT)
    return f"{day} 00:00:00.000", f"{upper} 01:00:00.000"


def chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), max(1, size))]
