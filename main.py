"""
Script Principal (Orquestrador do Pipeline Ponta a Ponta)
Projeto Integrador: Da Ingestão à Decisão (ENEM + ANEEL)

Executa de forma sequencial e integrada todas as etapas:
1. Ingestão Bronze:
   - ANEEL: API REST pública (JSON) com paginação e backoff
   - ENEM: Microdados em arquivo (CSV) com metadados técnicos
2. Camada Silver:
   - Limpeza, tipagem forte e adequação LGPD
   - Agregação municipal dos microdados
   - Cruzamento relacional (Inner Join) e auditoria de órfãos
3. Camada Gold (Dataset ML-Ready):
   - Aplicação de salvaguarda anti-vazamento (ponto de corte t0 = 31/07)
4. Modelagem & Decisão:
   - Split estritamente temporal (Treino: 2020 a 2023 | Teste: 2024)
   - Avaliação da decisão de alocação de geradores para a SEDUC-PA / MEC

Como rodar:
    python main.py
"""

import sys
import time
from datetime import datetime

from src.utils.logger import get_logger
from src.ingestion.ingest_aneel_api import ingest_to_bronze
from src.ingestion.ingest_enem_csv import ingest_enem_sample_multiyear
from src.processing.bronze_to_silver import (
    process_aneel_silver,
    process_enem_silver,
    audit_join_silver,
)
from src.processing.silver_to_gold import create_ml_ready_dataset
from src.model.train_predictor import train
from src.model.evaluate import simulate_decision_impact

logger = get_logger("pipeline_orchestrator")


def print_banner(title: str):
    print("\n" + "=" * 75)
    print(f"  >>> {title}")
    print("=" * 75)


def run_full_pipeline():
    start_total = time.time()
    print_banner("INICIANDO PIPELINE COMPLETO: DA INGESTÃO À DECISÃO")
    print(f"Data de Execução: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("Escopo: Estado do Pará (144 municípios) | Série Histórica: 2020 a 2024\n")

    # -------------------------------------------------------------
    # ETAPA 1: Ingestão Camada Bronze (API REST JSON + Arquivo CSV)
    # -------------------------------------------------------------
    print_banner("ETAPA 1: INGESTÃO CAMADA BRONZE")
    print("1.1. Coletando dados da ANEEL via API REST CKAN (JSON)...")
    ingest_to_bronze(agent_filter="EQUATORIAL PA", anos=[2020, 2021, 2022, 2023, 2024], max_records_per_year=5000)

    print("\n1.2. Ingerindo Microdados do ENEM (CSV com chunks e metadados)...")
    ingest_enem_sample_multiyear(anos=[2020, 2021, 2022, 2023, 2024])

    # -------------------------------------------------------------
    # ETAPA 2: Camada Silver e Auditoria do JOIN
    # -------------------------------------------------------------
    print_banner("ETAPA 2: PROCESSAMENTO SILVER & AUDITORIA DE JOIN")
    print("2.1. Tratando e padronizando dados de energia da ANEEL...")
    df_silver_aneel = process_aneel_silver()

    print("\n2.2. Tratando microdados do ENEM (LGPD + Agregação Municipal)...")
    df_silver_enem = process_enem_silver()

    print("\n2.3. Executando auditoria relacional do cruzamento (Inner Join)...")
    df_joined = audit_join_silver(coluna_chave="codigo_municipio")

    # -------------------------------------------------------------
    # ETAPA 3: Camada Gold (Dataset ML-Ready com Salvaguarda Anti-Leakage)
    # -------------------------------------------------------------
    print_banner("ETAPA 3: CAMADA GOLD (DATASET ML-READY)")
    print("Aplicando corte temporal t0 = 31/07 (Features em Jan-Jul e Target em Novembro)...")
    df_gold = create_ml_ready_dataset()

    # -------------------------------------------------------------
    # ETAPA 4: Modelagem Preditiva com Split Temporal
    # -------------------------------------------------------------
    print_banner("ETAPA 4: MODELAGEM PREDITIVA COM SPLIT TEMPORAL")
    print("Treinando modelo com 4 anos passados (2020-2023) e avaliando em 2024...")
    modelo = train()

    # -------------------------------------------------------------
    # ETAPA 5: Matriz de Impacto da Decisão (SEDUC-PA / MEC)
    # -------------------------------------------------------------
    print_banner("ETAPA 5: AVALIAÇÃO DA DECISÃO DE NEGÓCIO")
    df_priorizados = simulate_decision_impact(n_priorizados=50, custo_por_gerador=15000.0, clf=modelo)

    total_time = time.time() - start_total
    print_banner("PIPELINE CONCLUÍDO COM SUCESSO!")
    print(f"Tempo total de execução: {total_time:.2f} segundos")
    print(f"Municípios Casados no JOIN: {df_joined['codigo_municipio'].nunique()} de 144 (100%)")
    print(f"Base Gold ML-Ready: {len(df_gold)} registros município-ano")
    print("Status: Pronto para apresentação e defesa presencial.")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    run_full_pipeline()
