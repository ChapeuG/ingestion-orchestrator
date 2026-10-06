# ingestion-orchestrator

Motor de ingestão **PySpark** guiado por **data contract**. Um projeto só ingere qualquer tabela de qualquer origem
JDBC (Oracle, Postgres, MySQL, SQL Server) para a raw do Data Lake: basta o contrato da tabela.

Ele é isolado do framework [`dl-gen`](https://github.com/ChapeuG/dl-gen) e não depende de lib utils externa: secrets, criptografia, datas e
criação da tabela no Athena foram reescritos aqui.

```
contracts/<dataset>/<tabela>.odcs.yaml      (pessoas escrevem — padrão ODCS v3.2.0)
        │  dl-gen generate --contract ... --publish-s3 s3://<bucket>/ingestion-config/
        ▼
s3://<bucket>/ingestion-config/<dataset>/<tabela>.ingestion.yml   (gerado — formato ingestion/v1)
        │  spark-submit jobs/main.py --config s3://<bucket>/ingestion-config/ --env prd
        ▼
JDBC (conector do source.type) → qualidade → criptografia AES → JSON gzip na raw → tabela Glue/Athena
```

O mesmo `dl-gen generate --contract` também gera o projeto de **transformação** (raw → staging Delta), como hoje.

## 1. Escrever o contrato

```bash
python scripts/novo_contrato.py sakila actor --domain exemplos
```

Isso copia [`contracts/template.odcs.yaml`](contracts/template.odcs.yaml) para `contracts/sakila/actor.odcs.yaml`
com id e datas preenchidos. O template explica cada campo e marca o que é obrigatório (`[OBRIGATÓRIO]`) e o que é
extensão própria (`[EXT]`, sempre em `customProperties`). Há dois exemplos completos:
[`sakila/actor`](contracts/sakila/actor.odcs.yaml) (Postgres) e
[`cartoes/cliente`](contracts/cartoes/cliente.odcs.yaml) (Oracle, CPF criptografado, leitura paralela).

### Editor visual

O contrato é ODCS padrão, então abre no [Data Contract Editor](https://editor.datacontract.com), que tem
modo visual, formulário guiado e YAML, com validação e preview. Como o contrato tem host e nome de banco,
prefira rodar o editor **localmente** (open source, MIT):

```bash
npx datacontract-editor contracts/sakila/actor.odcs.yaml
```

Alternativas: `docker run -p 4173:4173 datacontract/editor` ou `datacontract edit contracts/sakila/actor.odcs.yaml`.

### Validar

```bash
pip install datacontract-cli
datacontract lint contracts/sakila/actor.odcs.yaml
```

O CI roda o lint em todo PR que mexe em `contracts/`.

### Regras do contrato que viram comportamento

| No contrato | Efeito |
|---|---|
| `servers[]` (`environment`, `type`, `host`, `port`, `database`/`serviceName`) | Um servidor por ambiente; `type` escolhe o conector |
| `servers[].customProperties.secretId` | Secret com `username`/`password` (e opcional `host`, `port`, `dbname`, `url`) |
| `schema[].physicalName` | Tabela lida na origem |
| `properties[]` | Colunas selecionadas (só as do contrato: o contrato é a fonte da verdade) |
| `partitioned: true` | Filtro incremental do dia e coluna da leitura paralela |
| `customProperties.incrementalColumns` | Colunas extras no filtro (`OR`), ex: data de atualização |
| `customProperties.loadMode` | `incremental` ou `full` |
| `properties[].customProperties.encrypt: true` | AES/ECB + base64 antes de gravar (chave em `cryptographySecretArn`) |
| `quality` (`nullValues`, `duplicateValues`, `rowCount`) | Checagens antes de gravar: `error` bloqueia, `warning` registra |
| `customProperties.rawPartitionColumn` | **Obrigatório.** Coluna de partição da raw (`<coluna>=yyyyMMdd`) e do Athena |
| `customProperties.rawPath` / `rawDatabase` | Destino na raw e no Athena (`{env}` vira dev/hml/prd) |
| `primaryKey`, `stagingName`, `description` | Usados pelo framework na transformação (merge Delta, nomenclatura, comments) |

Senha e chave **nunca** entram no contrato: só o ARN da secret.

## 2. Gerar o ingestion.yml

No framework ([`dl-gen`](https://github.com/ChapeuG/dl-gen), outro repositório):

```bash
dl-gen generate --contract contracts/sakila/actor.odcs.yaml --publish-s3 s3://<bucket>/ingestion-config/
```

Sem `--publish-s3` o arquivo fica em `ingestion-config/<dataset>/<tabela>.ingestion.yml`, para enviar depois com
`aws s3 cp`. Exemplos gerados: [`examples/ingestion-config/`](examples/ingestion-config/).

O yml é gerado, **não edite à mão**: altere o contrato e gere de novo. Se algo obrigatório faltar no contrato, o
campo sai como `PREENCHER` e o orquestrador recusa a execução no ambiente que usa esse campo.

## 3. Executar

```bash
python scripts/package.py                                   # dist/ingestion.zip
aws s3 cp dist/ingestion.zip s3://<bucket-artefatos>/ingestion-orchestrator/
aws s3 cp jobs/main.py           s3://<bucket-artefatos>/ingestion-orchestrator/
```

Step do EMR:

```bash
spark-submit \
  --py-files s3://<bucket-artefatos>/ingestion-orchestrator/ingestion.zip \
  --packages org.postgresql:postgresql:42.7.4,com.oracle.database.jdbc:ojdbc8:21.9.0.0 \
  s3://<bucket-artefatos>/ingestion-orchestrator/main.py \
  --config s3://<bucket>/ingestion-config/sakila/actor.ingestion.yml \
  --env prd
```

| Parâmetro | Default | Função |
|---|---|---|
| `--config` | — | Um `ingestion.yml` ou um prefixo (roda todos os `*.ingestion.yml` abaixo dele) |
| `--env` | — | Escolhe o servidor do contrato e resolve `{env}` |
| `--start-date` / `--end-date` | D-1 | Reprocessamento (`yyyy-MM-dd`) |
| `--ingestion-type` | `normal` | `historical` filtra só pela coluna `partitioned` |
| `--tables` | todas | Filtra por `tabela` ou `dataset.tabela` |
| `--validate-only` | — | Só valida os yml, sem abrir o Spark |

Todos os yml são validados **antes** de qualquer carga. Uma tabela com erro não interrompe as outras, e o job sai
com código 1 se alguma falhar.

### Regras de data

| Execução | Dia lido | Partição gravada |
|---|---|---|
| Sem datas (diária) | D-1 | hoje |
| Reprocessamento `normal` | D | D+1 |
| `historical` | D | D |
| `loadMode: full` | tabela inteira | como a diária |

### Drivers JDBC (`--packages`)

| `source.type` | Maven |
|---|---|
| postgres | `org.postgresql:postgresql:42.7.4` |
| oracle | `com.oracle.database.jdbc:ojdbc8:21.9.0.0` |
| mysql | `com.mysql:mysql-connector-j:8.4.0` |
| sqlserver | `com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre8` |

Banco novo: crie a subclasse de `Connector` em
[`src/ingestion/connectors/__init__.py`](src/ingestion/connectors/__init__.py) (driver, URL, literal de data,
aspas do alias) e registre em `CONNECTORS`.

## Estrutura

```
contracts/                  template.odcs.yaml + contratos por dataset
examples/ingestion-config/  yml gerados pelo framework a partir dos exemplos
jobs/main.py                entrypoint do spark-submit
src/ingestion/
  config.py                 leitura e validação do ingestion.yml
  connectors/               um conector JDBC por banco
  pipeline.py               leitura → qualidade → criptografia → escrita → catálogo
  dates.py                  janelas e partições
  crypto.py                 AES/ECB + base64 (compatível com a transformação)
  quality.py                notNull, unique, rowCount
  catalog.py                tabela externa JSON no Glue/Athena
  secrets.py, storage.py    Secrets Manager e leitura do S3
scripts/                    novo_contrato.py, package.py
```

## Testes

```bash
pip install -e .[dev]
python -m pytest
```

Os testes de Spark (criptografia, qualidade, DDL do catálogo) usam Spark local e são pulados se o `pyspark` não
estiver instalado.
