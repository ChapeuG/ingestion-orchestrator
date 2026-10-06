"""Conector JDBC base: o que muda de um banco para outro fica nas subclasses."""

from __future__ import annotations

from dataclasses import dataclass

from ingestion.config import IngestionConfig, Server


@dataclass(frozen=True)
class Window:
    """Janela de data/hora do filtro incremental (fim inclusivo, mesmo padrão dos projetos Scala)."""
    start: str  # 'yyyy-MM-dd 00:00:00.000'
    end: str    # 'yyyy-MM-dd 23:59:59.999'


class Connector:
    name: str = ""
    driver: str = ""
    # Coordenada Maven do driver (spark-submit --packages); documentação e mensagem de erro
    maven_package: str = ""
    default_port: str = ""

    # ── Dialeto ──────────────────────────────────────────────────────────
    def quote_alias(self, alias: str) -> str:
        """Alias com caracteres especiais (@timestamp)."""
        return f'"{alias}"'

    def timestamp_literal(self, value: str) -> str:
        return f"'{value}'"

    def current_timestamp(self) -> str:
        return "CURRENT_TIMESTAMP"

    def build_url(self, host: str, port: str, database: str) -> str:
        raise NotImplementedError

    def extra_properties(self) -> dict[str, str]:
        """Propriedades JDBC extras do banco."""
        return {}

    # ── Query ────────────────────────────────────────────────────────────
    def build_query(self, config: IngestionConfig, window: Window | None, historical: bool = False) -> str:
        """Subquery para o dbtable do Spark JDBC.

        Seleciona só as colunas do contrato (o contrato é a fonte da verdade) e mantém
        @timestamp/@version dos projetos de ingestão atuais.
        """
        columns = ",\n          ".join(f"t.{c.name}" for c in config.columns)
        sql = (
            f"SELECT\n          {columns},\n"
            f"          {self.current_timestamp()} AS {self.quote_alias('@timestamp')},\n"
            f"          '1' AS {self.quote_alias('@version')}\n"
            f"        FROM {config.source_table} t"
        )
        if window is not None and config.load_mode == "incremental":
            # Histórico filtra só pela coluna principal (igual ao createQueryHistorical do Scala)
            cols = config.incremental_columns[:1] if historical else config.incremental_columns
            start, end = self.timestamp_literal(window.start), self.timestamp_literal(window.end)
            conditions = "\n          OR ".join(f"t.{c} BETWEEN {start} AND {end}" for c in cols)
            sql += f"\n        WHERE {conditions}"
        return f"({sql}\n    ) tab"

    # ── Conexão ──────────────────────────────────────────────────────────
    def jdbc_options(self, server: Server, secret: dict) -> dict[str, str]:
        """URL e credenciais. Prioridade: url da secret > host/port/dbname da secret > servidor do contrato."""
        url = secret.get("url") or ""
        if not url:
            host = str(secret.get("host") or server.host)
            port = str(secret.get("port") or server.port or self.default_port)
            database = str(secret.get("dbname") or secret.get("database") or server.database)
            if not host or not database:
                raise ValueError("Sem host/database: informe na secret (host, dbname ou url) ou no contrato (servers)")
            # host que já começa com jdbc: é usado como prefixo (mesmo comportamento do GeneralUtils)
            url = f"{host}:{port}/{database}" if host.startswith("jdbc:") else self.build_url(host, port, database)
        return {
            "url": url,
            "user": str(secret.get("username") or secret.get("user") or ""),
            "password": str(secret.get("password") or ""),
            "driver": self.driver,
            **self.extra_properties(),
        }
