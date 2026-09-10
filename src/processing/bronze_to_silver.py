"""
Processamento Bronze -> Silver para o ENEM

Adaptado para usar os caminhos e nomes de metadados definidos em src/config.py
(mesmos usados no ingest_enem_csv.py do projeto).

Funções:
- process_enem_silver: limpa/padroniza o ENEM, remove dado sensível (NU_INSCRICAO)
- audit_join_silver: relata quantos municípios casaram e quantos ficaram órfãos

Como rodar:
    python -m src.processing.bronze_to_silver
"""

from pathlib import Path

import pandas as pd

from src.config import (
    BRONZE_ENEM_DIR,
    SILVER_ENEM_DIR,
    SILVER_ANEEL_DIR,
    SILVER_JOINED_DIR,
)
from src.utils.logger import get_logger

logger = get_logger("bronze_to_silver")

RELATORIO_JOIN_PATH = SILVER_JOINED_DIR / "relatorio_join_enem_aneel.md"


# 1. Silver do ENEM
def padronizar_codigo_municipio(serie: pd.Series) -> pd.Series:
    """
    Garante que o código IBGE do município tenha 7 dígitos
    """
    return serie.astype(str).str.strip().str.zfill(7)


def process_enem_silver() -> pd.DataFrame:
    """
    Lê todos os parquets da Bronze do ENEM (todos os anos já ingeridos),
    aplica:
    - remoção de dado sensível
    - padronização do código IBGE do município
    - tipagem forte nas colunas numéricas
    - deduplicação pela chave técnica (_record_hash)
    """
    arquivos = list(BRONZE_ENEM_DIR.glob("**/*.parquet"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum parquet encontrado em {BRONZE_ENEM_DIR}. Rode a ingestão primeiro."
        )

    logger.info(f"Lendo {len(arquivos)} arquivo(s) parquet da Bronze do ENEM")
    df = pd.concat([pd.read_parquet(f) for f in arquivos], ignore_index=True)
    logger.info(f"Lidos {len(df)} registros da Bronze do ENEM (todos os anos)")

    # Remove dado individual/sensível
    if "NU_INSCRICAO" in df.columns:
        df = df.drop(columns=["NU_INSCRICAO"])

    # Padroniza código do município (nome de coluna usado no ingest_enem_csv.py)
    if "CO_MUNICIPIO_PROVA" in df.columns:
        df["CO_MUNICIPIO_PROVA"] = padronizar_codigo_municipio(df["CO_MUNICIPIO_PROVA"])
        df = df.rename(columns={"CO_MUNICIPIO_PROVA": "codigo_municipio"})

    # Tipagem forte das notas (numéricas)
    colunas_nota = [c for c in df.columns if c.startswith("NU_NOTA_")]
    for col in colunas_nota:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Deduplicação pela chave técnica (_record_hash, gerado no ingest_enem_csv.py)
    antes = len(df)
    df = df.drop_duplicates(subset=["_record_hash"])
    logger.info(f"Removidos {antes - len(df)} duplicados na deduplicação da Silver")

    SILVER_ENEM_DIR.mkdir(parents=True, exist_ok=True)
    caminho_saida = SILVER_ENEM_DIR / "enem_silver.parquet"
    df.to_parquet(caminho_saida, index=False)
    logger.info(f"Silver do ENEM salva em {caminho_saida} ({len(df)} registros)")

    return df


# --- 2. Auditoria do JOIN ENEM x ANEEL ---
def audit_join_silver(coluna_chave: str = "codigo_municipio") -> pd.DataFrame:
    """
    Cruza ENEM Silver com ANEEL Silver pelo código IBGE do município
    e relata:
    - quantos municípios distintos existem em cada base
    - quantos casaram no join (inner)
    - quantos ficaram órfãos de cada lado

    Grava um relatório em markdown e retorna o DataFrame do join (inner).
    """
    caminho_enem = SILVER_ENEM_DIR / "enem_silver.parquet"
    arquivos_aneel = list(SILVER_ANEEL_DIR.glob("*.parquet"))

    if not caminho_enem.exists():
        raise FileNotFoundError(f"{caminho_enem} não existe. Rode process_enem_silver() primeiro.")
    if not arquivos_aneel:
        logger.warning(
            f"Nenhum parquet encontrado em {SILVER_ANEEL_DIR}. "
            "Peça pro responsável pela ANEEL gerar a Silver antes de rodar a auditoria."
        )
        return pd.DataFrame()

    df_enem = pd.read_parquet(caminho_enem)
    df_aneel = pd.concat([pd.read_parquet(f) for f in arquivos_aneel], ignore_index=True)

    if coluna_chave not in df_aneel.columns:
        raise KeyError(
            f"A coluna '{coluna_chave}' não existe na Silver da ANEEL. "
            f"Colunas disponíveis: {list(df_aneel.columns)}. "
            "Confirme o nome real da coluna de município com quem fez a ingestão da ANEEL."
        )

    df_aneel[coluna_chave] = padronizar_codigo_municipio(df_aneel[coluna_chave])

    municipios_enem = set(df_enem[coluna_chave].unique())
    municipios_aneel = set(df_aneel[coluna_chave].unique())

    casaram = municipios_enem & municipios_aneel
    orfaos_enem = municipios_enem - municipios_aneel
    orfaos_aneel = municipios_aneel - municipios_enem

    merged = pd.merge(
        df_enem, df_aneel, on=coluna_chave, how="inner", suffixes=("_enem", "_aneel")
    )

    relatorio = f"""# Auditoria do JOIN — ENEM x ANEEL

Chave de cruzamento: `{coluna_chave}`

| Métrica | Valor |
|---|---|
| Municípios distintos no ENEM | {len(municipios_enem)} |
| Municípios distintos na ANEEL | {len(municipios_aneel)} |
| Municípios que casaram (em ambas) | {len(casaram)} |
| Órfãos só no ENEM (sem dado ANEEL) | {len(orfaos_enem)} |
| Órfãos só na ANEEL (sem dado ENEM) | {len(orfaos_aneel)} |
| Registros no join final | {len(merged)} |

## O que foi feito com os órfãos
Os municípios órfãos foram **excluídos** do
join principal e não entram na base ML-Ready. Ajustem este
texto se decidirem tratar diferente (ex: manter com valor nulo/flag).
"""

    SILVER_JOINED_DIR.mkdir(parents=True, exist_ok=True)
    RELATORIO_JOIN_PATH.write_text(relatorio, encoding="utf-8")

    logger.info(f"Relatório de auditoria salvo em {RELATORIO_JOIN_PATH}")
    print(relatorio)

    return merged


if __name__ == "__main__":
    process_enem_silver()
    audit_join_silver()