"""Conectores JDBC por banco de origem (source.type do ingestion.yml).

Para um banco novo: crie a subclasse de Connector e registre em CONNECTORS.
"""

from __future__ import annotations

from ingestion.connectors.base import Connector, Window

ORACLE_SESSION_INIT = (
    "BEGIN "
    """execute immediate 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF"'; """
    """execute immediate 'ALTER SESSION SET NLS_DATE_FORMAT = "YYYY-MM-DD HH24:MI:SS"'; """
    "END;"
)


class PostgresConnector(Connector):
    name = "postgres"
    driver = "org.postgresql.Driver"
    maven_package = "org.postgresql:postgresql:42.7.4"
    default_port = "5432"

    def build_url(self, host, port, database):
        return f"jdbc:postgresql://{host}:{port}/{database}"


class OracleConnector(Connector):
    name = "oracle"
    driver = "oracle.jdbc.OracleDriver"
    maven_package = "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
    default_port = "1521"

    def build_url(self, host, port, database):
        return f"jdbc:oracle:thin:@//{host}:{port}/{database}"

    def timestamp_literal(self, value):
        return f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS.FF')"

    def extra_properties(self):
        # sessionInitStatement com os formatos NLS de data; DATE do Oracle (com hora) lido como timestamp
        return {"oracle.jdbc.mapDateToTimestamp": "true", "sessionInitStatement": ORACLE_SESSION_INIT}


class MySqlConnector(Connector):
    name = "mysql"
    driver = "com.mysql.cj.jdbc.Driver"
    maven_package = "com.mysql:mysql-connector-j:8.4.0"
    default_port = "3306"

    def build_url(self, host, port, database):
        return f"jdbc:mysql://{host}:{port}/{database}"

    def quote_alias(self, alias):
        # Sem ANSI_QUOTES o MySQL não aceita aspas duplas em identificador
        return f"`{alias}`"

    def extra_properties(self):
        # YEAR: por padrão o Connector/J devolve como data (2010-01-01); assim vem o ano como número
        return {"yearIsDateType": "false"}


class SqlServerConnector(Connector):
    name = "sqlserver"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    maven_package = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre8"
    default_port = "1433"

    def build_url(self, host, port, database):
        return f"jdbc:sqlserver://{host}:{port};databaseName={database}"

    def quote_alias(self, alias):
        return f"[{alias}]"

    def timestamp_literal(self, value):
        # DATETIME2: com DATETIME, '23:59:59.999' arredonda para o dia seguinte
        return f"CAST('{value}' AS DATETIME2)"


CONNECTORS: dict[str, type[Connector]] = {
    c.name: c for c in (PostgresConnector, OracleConnector, MySqlConnector, SqlServerConnector)
}


def get_connector(source_type: str) -> Connector:
    try:
        return CONNECTORS[source_type.lower()]()
    except KeyError:
        raise ValueError(f"Conector '{source_type}' não existe. Opções: {', '.join(sorted(CONNECTORS))}") from None


__all__ = ["CONNECTORS", "Connector", "Window", "get_connector"]
