"""Dados de teste: ingestion.yml no formato que o dl-gen gera (ingestion/v1)."""

ACTOR = """\n# Gerado pelo dl-gen a partir do data contract. Não edite à mão: altere o contrato e gere de novo.
# Lido pelo ingestion-orchestrator (valores com {env} são resolvidos por --env).
apiVersion: ingestion/v1
kind: Ingestion
contract:
  id: 7c1f2a52-5d0b-4f7e-9a51-3f6f0d2b8a11
  version: 1.0.0
  status: draft
dataset: sakila
table: actor
description: Cadastro de atores dos filmes da locadora.
source:
  type: postgres
  table: public.actor
  servers:
    dev:
      host: sakila-dev.example.internal
      port: 5432
      database: sakila
      secretId: arn:aws:secretsmanager:sa-east-1:111111111111:secret:dev/sakila/postgres
    prd:
      host: sakila-prd.example.internal
      port: 5432
      database: sakila
      secretId: arn:aws:secretsmanager:sa-east-1:222222222222:secret:prd/sakila/postgres
load:
  mode: incremental
  incrementalColumns:
  - last_update
  fetchSize: 150000
  parallel:
    days: 1
    queries: 1
    column: last_update
columns:
- name: actor_id
  type: integer
  physicalType: integer
  primaryKey: true
  required: true
- name: first_name
  type: string
  physicalType: varchar(45)
  required: true
- name: last_name
  type: string
  physicalType: varchar(45)
  required: true
- name: last_update
  type: timestamp
  physicalType: timestamp
  required: true
target:
  database: raw_sakila
  table: actor
  partitionColumn: dt_ingestao
  format: json
  compression: gzip
quality:
- check: unique
  columns:
  - actor_id
  severity: error
- check: rowCount
  mustBeGreaterThan: 0
  severity: warning
"""

CLIENTE = """\n# Gerado pelo dl-gen a partir do data contract. Não edite à mão: altere o contrato e gere de novo.
# Lido pelo ingestion-orchestrator (valores com {env} são resolvidos por --env).
apiVersion: ingestion/v1
kind: Ingestion
contract:
  id: 0f6c3e7d-8b9a-4c21-b3d4-5e6f7a8b9c0d
  version: 1.0.0
  status: draft
dataset: cartoes
table: cliente
description: Cadastro de clientes portadores de cartão.
source:
  type: oracle
  table: CRT.TB_CLIENTE
  servers:
    prd:
      host: cartoes-prd.example.internal
      port: 1521
      database: CARTOESPRD
      secretId: arn:aws:secretsmanager:sa-east-1:222222222222:secret:prd/cartoes/oracle
load:
  mode: incremental
  incrementalColumns:
  - DH_INCL_RGST
  - DH_ATLZ_RGST
  fetchSize: 150000
  parallel:
    days: 1
    queries: 4
    column: DH_INCL_RGST
columns:
- name: CD_CLIENTE
  type: number
  physicalType: NUMBER(18,0)
  primaryKey: true
  required: true
- name: NU_CPF
  type: string
  physicalType: VARCHAR2(11)
  encrypt: true
- name: NM_CLIENTE
  type: string
  physicalType: VARCHAR2(200)
- name: DH_INCL_RGST
  type: timestamp
  physicalType: DATE
  required: true
- name: DH_ATLZ_RGST
  type: timestamp
  physicalType: DATE
target:
  database: raw_cartoes
  table: cliente
  partitionColumn: dt_ingestao
  format: json
  compression: gzip
encryption:
  algorithm: AES/ECB
  secretArn: arn:aws:secretsmanager:sa-east-1:222222222222:secret:prd/cartoes/chave-criptografia
  secretField: AES_KEY
  columns:
  - NU_CPF
quality:
- check: notNull
  columns:
  - DH_ATLZ_RGST
  severity: warning
"""
