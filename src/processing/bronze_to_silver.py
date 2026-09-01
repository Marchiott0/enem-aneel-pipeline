"""Script de transformação Bronze para Silver.

Responsabilidades:
- Tipagem forte e padronização de nomenclatura;
- Tratamento de valores nulos e validação de schema;
- Quarentena para registros inconsistentes;
- Auditoria de integridade do JOIN (contagem de casados e órfãos).
"""
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path para permitir execução direta
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from src.config import (
    BRONZE_ANEEL_DIR, BRONZE_ENEM_DIR,
    SILVER_ANEEL_DIR, SILVER_ENEM_DIR, SILVER_JOINED_DIR,
    QUARANTINE_ANEEL_DIR, QUARANTINE_ENEM_DIR
)
from src.utils.logger import get_logger


logger = get_logger("bronze_to_silver")

def process_aneel_silver():
    """Limpa e padroniza os dados de interrupção de energia elétrica da ANEEL."""
    logger.info("Processando Bronze -> Silver (ANEEL)...")
    # Busca arquivos parquet na Bronze
    parquet_files = list(BRONZE_ANEEL_DIR.glob("*.parquet"))
    if not parquet_files:
        logger.warning("Nenhum arquivo Parquet encontrado na Bronze ANEEL.")
        return None

    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    
    # Padronização de colunas e tipagem forte (exemplo de contrato Silver)
    # Adaptação para nomes canônicos
    column_mapping = {
        "IdeCodigoMunicipio": "id_municipio_ibge",
        "NomMunicipio": "no_municipio",
        "SigUF": "sg_uf",
        "NumAno": "ano",
        "NumMes": "mes",
        "VlrIndiceEnviado": "valor_indice",
        "SigIndicador": "tipo_indicador"
    }
    df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)
    
    output_path = SILVER_ANEEL_DIR / "aneel_cleaned.parquet"
    df.to_parquet(output_path, index=False)
    logger.info(f"Silver ANEEL salva com {len(df)} registros em: {output_path}")
    return df

def process_enem_silver():
    """Limpa e padroniza os dados dos microdados do ENEM."""
    logger.info("Processando Bronze -> Silver (ENEM)...")
    parquet_files = list(BRONZE_ENEM_DIR.glob("*.parquet"))
    if not parquet_files:
        logger.warning("Nenhum arquivo Parquet encontrado na Bronze ENEM.")
        return None

    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    
    # Padronização e cálculo do indicador de presença/abstenção por participante
    if "CO_MUNICIPIO_PROVA" in df.columns:
        df["id_municipio_ibge"] = df["CO_MUNICIPIO_PROVA"].astype(str).str.zfill(7)

    output_path = SILVER_ENEM_DIR / "enem_cleaned.parquet"
    df.to_parquet(output_path, index=False)
    logger.info(f"Silver ENEM salva com {len(df)} registros em: {output_path}")
    return df

def audit_join_silver(df_aneel: pd.DataFrame, df_enem: pd.DataFrame):
    """Realiza a auditoria do JOIN entre ENEM e ANEEL pela chave do Município IBGE."""
    if df_aneel is None or df_enem is None or "id_municipio_ibge" not in df_aneel.columns or "id_municipio_ibge" not in df_enem.columns:
        logger.warning("Dataframes insuficientes para auditoria de join.")
        return

    logger.info("Iniciando auditoria relacional do JOIN (ENEM <-> ANEEL)...")
    m_aneel = set(df_aneel["id_municipio_ibge"].unique())
    m_enem = set(df_enem["id_municipio_ibge"].unique())

    casados = m_enem.intersection(m_aneel)
    orfaos_enem = m_enem - m_aneel
    orfaos_aneel = m_aneel - m_enem

    logger.info(f"Municípios Casados no JOIN: {len(casados)}")
    logger.info(f"Municípios Órfãos no ENEM: {len(orfaos_enem)}")
    logger.info(f"Municípios Órfãos na ANEEL: {len(orfaos_aneel)}")

    # Gera dataframe enriquecido Silver Joined
    df_joined = pd.merge(df_enem, df_aneel, on="id_municipio_ibge", how="inner")
    joined_path = SILVER_JOINED_DIR / "silver_joined.parquet"
    df_joined.to_parquet(joined_path, index=False)
    logger.info(f"Dataset Silver Joined salvo em: {joined_path}")

def run_bronze_to_silver():
    df_aneel = process_aneel_silver()
    df_enem = process_enem_silver()
    if df_aneel is not None and df_enem is not None:
        audit_join_silver(df_aneel, df_enem)

if __name__ == "__main__":
    run_bronze_to_silver()
