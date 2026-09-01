# Arquitetura do Pipeline de Dados e Decisão

## 1. Visão Geral do Fluxo

```
[ INEP (CSV) ] ----> Ingestão CSV (Chunked + Metadados) ----> data/bronze/enem/
                                                                     │
[ ANEEL (JSON API) ] -> Ingestão API (Retry + Metadados) ------> data/bronze/aneel/
                                                                     │
                                                    ┌────────────────┴────────────────┐
                                                    │  Processamento Bronze -> Silver │
                                                    │   - Tipagem e Contratos         │
                                                    │   - Limpeza e Quarentena        │
                                                    │   - Auditoria de JOIN (IBGE)    │
                                                    └────────────────┬────────────────┘
                                                                     ▼
                                                             data/silver/joined/
                                                                     │
                                                    ┌────────────────┴────────────────┐
                                                    │  Processamento Silver -> Gold   │
                                                    │   - Filtro Temporal (< t0)      │
                                                    │   - Engenharia de Features      │
                                                    │   - Criação do Label ML-Ready   │
                                                    └────────────────┬────────────────┘
                                                                     ▼
                                                             data/gold/ml_ready/
                                                                     │
                                                    ┌────────────────┴────────────────┐
                                                    │  Treinamento & Decisão (Model)  │
                                                    │   - Split Temporal              │
                                                    │   - Treinamento Anti-Leakage    │
                                                    │   - Matriz de Custo da Decisão  │
                                                    └─────────────────────────────────┘
```

---

## 2. Princípios de Engenharia Adotados

### 2.1. Idempotência e Rastreabilidade
- Todas as linhas ingeridas na Bronze recebem um hash SHA-256 (`_record_hash`) gerado a partir de seu conteúdo original.
- A ingestão elimina duplicatas antes da escrita no formato Parquet particionado.
- Metadados técnicos `_ingestion_time`, `_source` e `_load_id` garantem rastreabilidade completa.

### 2.2. Isolamento de Falhas (Quarentena)
- Linhas com erros de schema, valores corrompidos ou erros transitórios de API são descarregadas em `data/quarantine/`.
- O pipeline nunca quebra silenciosamente nem interrompe o processamento do lote por causa de registros individuais sujos.

### 2.3. Blindagem Anti-Vazamento (Anti-Leakage)
- O ponto de corte temporal é fixado estritamente em **$t_0 = 31/\text{Julho}$**.
- Todas as agregações e features preditivas de energia elétrica utilizam apenas dados do intervalo $[01/\text{Jan}, 31/\text{Jul}]$.
- O target (`target_abstencao_critica`) é apurado estritamente na data de realização do exame (Novembro, $> t_0$).
- O particionamento de treino e teste é temporal (ex: Anos anteriores para treino, ano recente para teste), simulando o cenário real de predição em produção.
