"""
Processamento Bronze -> Silver para ENEM e ANEEL

Responsabilidades:
1. Silver do ENEM:
   - Preserva todas as regras da colega (remoção de NU_INSCRICAO para LGPD,
     padronização do código IBGE do município com 7 dígitos, tipagem numérica das notas,
     deduplicação técnica por hash);
   - Realiza o tratamento dos microdados agregando os candidatos individuais
     em nível municipal (calculando total de inscritos, total de faltosos e taxa de abstenção).
2. Silver da ANEEL:
   - Faz o cruzamento relacional entre Conjuntos Elétricos e os 144 municípios do Pará;
   - Padroniza a chave do município para 'codigo_municipio' (7 dígitos IBGE);
   - Converte os indicadores DEC e FEC para valores numéricos contínuos em horas/frequência;
   - Agrega as métricas de qualidade de energia elétrica por município, ano e mês.
3. Auditoria do JOIN:
   - Cruza ENEM e ANEEL pelo código IBGE do município e ano;
   - Gera o relatório oficial de integridade relacional (casados vs órfãos);
   - Exporta a base integrada Silver Joined para a camada Gold.

Como rodar:
    python -m src.processing.bronze_to_silver
"""

from pathlib import Path
import pandas as pd
import numpy as np

from src.config import (
    BRONZE_ANEEL_DIR,
    BRONZE_ENEM_DIR,
    SILVER_ANEEL_DIR,
    SILVER_ENEM_DIR,
    SILVER_JOINED_DIR,
)
from src.utils.logger import get_logger

logger = get_logger("bronze_to_silver")

RELATORIO_JOIN_PATH = SILVER_JOINED_DIR / "relatorio_join_enem_aneel.md"


# --- 1. Funções Auxiliares de Padronização ---
def padronizar_codigo_municipio(serie: pd.Series) -> pd.Series:
    """
    Garante que o código IBGE do município tenha 7 dígitos como texto.
    (Lógica original preservada da colega Ana Paula)
    """
    return serie.astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(7)


# --- 2. Silver da ANEEL (Responsabilidade do Cauã) ---
def process_aneel_silver() -> pd.DataFrame:
    """
    Lê a Bronze da ANEEL, cruza com a tabela de mapeamento municipal oficial
    (indqual-municipio), trata e tipa os indicadores de continuidade DEC/FEC e
    agrega por município, ano e mês.
    """
    logger.info("Processando Bronze -> Silver (ANEEL)...")

    mapa_file = BRONZE_ANEEL_DIR / "aneel_conjuntos_municipios.parquet"
    if not mapa_file.exists():
        raise FileNotFoundError(
            f"Arquivo de mapeamento {mapa_file} não encontrado. Execute a ingestão da ANEEL primeiro."
        )

    df_mapa = pd.read_parquet(mapa_file)
    df_mapa["IdeConjUnidConsumidoras"] = df_mapa["IdeConjUnidConsumidoras"].astype(str).str.strip()
    df_mapa["codigo_municipio"] = padronizar_codigo_municipio(df_mapa["CodMunicipio"])
    df_mapa = df_mapa[["IdeConjUnidConsumidoras", "codigo_municipio", "NomMunicipio"]].drop_duplicates()

    # Busca arquivos de indicadores DEC/FEC
    arquivos_ind = list(BRONZE_ANEEL_DIR.glob("aneel_dec_fec_*.parquet"))
    if not arquivos_ind:
        raise FileNotFoundError(f"Nenhum arquivo de indicadores DEC/FEC encontrado em {BRONZE_ANEEL_DIR}")

    df_ind = pd.concat([pd.read_parquet(f) for f in arquivos_ind], ignore_index=True)
    logger.info(f"Lidos {len(df_ind)} registros brutos de indicadores da ANEEL.")

    # Converte chave de conjunto para string
    df_ind["IdeConjUndConsumidoras"] = df_ind["IdeConjUndConsumidoras"].astype(str).str.strip()

    # Cruzamento com o mapa de municípios
    df_merged = pd.merge(
        df_ind,
        df_mapa,
        left_on="IdeConjUndConsumidoras",
        right_on="IdeConjUnidConsumidoras",
        how="inner"
    )
    logger.info(f"Registros associados aos municípios do Pará: {len(df_merged)}")

    # Padronização e conversão numérica de VlrIndiceEnviado
    if "VlrIndiceEnviado" in df_merged.columns:
        df_merged["valor_indice"] = (
            df_merged["VlrIndiceEnviado"]
            .astype(str)
            .str.strip()
            .str.replace(",", ".", regex=False)
        )
        df_merged["valor_indice"] = pd.to_numeric(df_merged["valor_indice"], errors="coerce")

    # Mapeamento canônico do indicador (DEC = duração horas, FEC = frequência)
    df_merged["tipo_indicador"] = df_merged["SigIndicador"].apply(
        lambda s: "DEC" if "DEC" in str(s).upper() else ("FEC" if "FEC" in str(s).upper() else "OUTRO")
    )
    df_merged = df_merged[df_merged["tipo_indicador"].isin(["DEC", "FEC"])].copy()

    # Padronização de Ano e Mês
    df_merged["ano"] = pd.to_numeric(df_merged["AnoIndice"], errors="coerce").astype("Int64")
    df_merged["mes"] = pd.to_numeric(df_merged["NumPeriodoIndice"], errors="coerce").astype("Int64")
    df_merged.dropna(subset=["codigo_municipio", "ano", "mes", "valor_indice"], inplace=True)
    df_merged["ano"] = df_merged["ano"].astype(int)
    df_merged["mes"] = df_merged["mes"].astype(int)

    # Agregação por município, ano, mês e tipo de indicador
    df_agg = (
        df_merged.groupby(["codigo_municipio", "NomMunicipio", "ano", "mes", "tipo_indicador"], as_index=False)
        ["valor_indice"]
        .mean()
    )

    # Pivot para ter colunas dec_mensal e fec_mensal
    df_pivot = df_agg.pivot_table(
        index=["codigo_municipio", "NomMunicipio", "ano", "mes"],
        columns="tipo_indicador",
        values="valor_indice",
        aggfunc="mean"
    ).reset_index()

    df_pivot.rename(columns={"DEC": "dec_horas_mensal", "FEC": "fec_freq_mensal"}, inplace=True)
    df_pivot["dec_horas_mensal"] = df_pivot.get("dec_horas_mensal", 0.0).fillna(0.0).round(2)
    df_pivot["fec_freq_mensal"] = df_pivot.get("fec_freq_mensal", 0.0).fillna(0.0).round(2)

    SILVER_ANEEL_DIR.mkdir(parents=True, exist_ok=True)
    caminho_saida = SILVER_ANEEL_DIR / "aneel_silver.parquet"
    df_pivot.to_parquet(caminho_saida, index=False)
    logger.info(f"Silver da ANEEL salva em {caminho_saida} ({len(df_pivot)} linhas)")
    return df_pivot


# --- 3. Silver do ENEM (Preserva lógica da Ana Paula + Agregação Municipal) ---
def process_enem_silver() -> pd.DataFrame:
    """
    Lê todos os parquets da Bronze do ENEM (todos os anos ingeridos).
    
    Etapas preservadas da colega Ana Paula:
    - Remoção de dado sensível (NU_INSCRICAO) para conformidade com LGPD;
    - Padronização do código IBGE do município com 7 dígitos (padronizar_codigo_municipio);
    - Tipagem forte nas colunas numéricas de notas (NU_NOTA_*);
    - Deduplicação pela chave técnica (_record_hash).
    
    Etapa de tratamento de microdados adicionada:
    - Cálculo de presença e abstenção por candidato;
    - Agregação dos dados a nível municipal e anual (taxa de abstenção, total de inscritos e médias de notas).
    """
    arquivos = list(BRONZE_ENEM_DIR.glob("**/*.parquet"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum parquet encontrado em {BRONZE_ENEM_DIR}. Rode a ingestão primeiro."
        )

    logger.info(f"Lendo {len(arquivos)} arquivo(s) parquet da Bronze do ENEM...")
    df = pd.concat([pd.read_parquet(f) for f in arquivos], ignore_index=True)
    logger.info(f"Lidos {len(df)} registros da Bronze do ENEM (todos os anos)")

    # 1. Remove dado individual/sensível (LGPD) - Lógica da Ana Paula
    if "NU_INSCRICAO" in df.columns:
        df = df.drop(columns=["NU_INSCRICAO"])

    # 2. Padroniza código do município - Lógica da Ana Paula
    if "CO_MUNICIPIO_PROVA" in df.columns:
        df["CO_MUNICIPIO_PROVA"] = padronizar_codigo_municipio(df["CO_MUNICIPIO_PROVA"])
        df = df.rename(columns={"CO_MUNICIPIO_PROVA": "codigo_municipio"})

    # Padroniza ano
    if "NU_ANO" in df.columns:
        df["ano"] = pd.to_numeric(df["NU_ANO"], errors="coerce").fillna(2023).astype(int)
    elif "ano" not in df.columns:
        df["ano"] = 2023

    # 3. Tipagem forte das notas (numéricas) - Lógica da Ana Paula
    colunas_nota = [c for c in df.columns if c.startswith("NU_NOTA_")]
    for col in colunas_nota:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Deduplicação pela chave técnica (_record_hash) - Lógica da Ana Paula
    if "_record_hash" in df.columns:
        antes = len(df)
        df = df.drop_duplicates(subset=["_record_hash"])
        logger.info(f"Removidos {antes - len(df)} duplicados na deduplicação da Silver")

    # 5. Tratamento de Microdados: Cálculo de Presença e Abstenção
    # No INEP, TP_PRESENCA = 1 indica presente. Se faltou a pelo menos uma das provas chave, considera abstenção
    if "TP_PRESENCA_LC" in df.columns and "TP_PRESENCA_MT" in df.columns:
        presente = ((df["TP_PRESENCA_LC"] == 1) & (df["TP_PRESENCA_MT"] == 1)).astype(int)
    elif "TP_PRESENCA_CN" in df.columns:
        presente = (df["TP_PRESENCA_CN"] == 1).astype(int)
    else:
        presente = pd.Series(1, index=df.index)

    df["is_presente"] = presente
    df["is_abstencao"] = 1 - df["is_presente"]

    # 6. Agregação Municipal por Ano (Conformidade com a granularidade de decisão)
    logger.info("Agregando microdados de candidatos a nível municipal (codigo_municipio, ano)...")
    
    agg_dict = {
        "is_presente": "count",
        "is_abstencao": "sum",
    }
    for col in colunas_nota:
        agg_dict[col] = "mean"

    df_mun = df.groupby(["codigo_municipio", "ano"], as_index=False).agg(agg_dict)
    df_mun.rename(columns={
        "is_presente": "total_inscritos",
        "is_abstencao": "total_abstencao"
    }, inplace=True)

    df_mun["taxa_abstencao"] = (df_mun["total_abstencao"] / df_mun["total_inscritos"]).round(4)

    # Renomeia colunas de nota média
    for col in colunas_nota:
        nome_curto = col.lower().replace("nu_nota_", "media_nota_")
        df_mun.rename(columns={col: nome_curto}, inplace=True)

    SILVER_ENEM_DIR.mkdir(parents=True, exist_ok=True)
    caminho_saida = SILVER_ENEM_DIR / "enem_silver.parquet"
    df_mun.to_parquet(caminho_saida, index=False)
    logger.info(f"Silver do ENEM salva em {caminho_saida} ({len(df_mun)} municípios-ano agregados)")

    return df_mun


# --- 4. Auditoria do JOIN ENEM x ANEEL ---
def audit_join_silver(coluna_chave: str = "codigo_municipio") -> pd.DataFrame:
    """
    Cruza ENEM Silver com ANEEL Silver pelo código IBGE do município
    e relata:
    - quantos municípios distintos existem em cada base
    - quantos casaram no join (inner)
    - quantos ficaram órfãos de cada lado

    Grava um relatório em markdown e retorna o DataFrame do join (inner).
    (Estrutura e métricas originais preservadas da colega Ana Paula)
    """
    caminho_enem = SILVER_ENEM_DIR / "enem_silver.parquet"
    caminho_aneel = SILVER_ANEEL_DIR / "aneel_silver.parquet"

    if not caminho_enem.exists():
        raise FileNotFoundError(f"{caminho_enem} não existe. Rode process_enem_silver() primeiro.")
    if not caminho_aneel.exists():
        raise FileNotFoundError(f"{caminho_aneel} não existe. Rode process_aneel_silver() primeiro.")

    df_enem = pd.read_parquet(caminho_enem)
    df_aneel = pd.read_parquet(caminho_aneel)

    if coluna_chave not in df_aneel.columns:
        raise KeyError(f"A coluna '{coluna_chave}' não existe na Silver da ANEEL.")
    if coluna_chave not in df_enem.columns:
        raise KeyError(f"A coluna '{coluna_chave}' não existe na Silver do ENEM.")

    df_enem[coluna_chave] = padronizar_codigo_municipio(df_enem[coluna_chave])
    df_aneel[coluna_chave] = padronizar_codigo_municipio(df_aneel[coluna_chave])

    municipios_enem = set(df_enem[coluna_chave].unique())
    municipios_aneel = set(df_aneel[coluna_chave].unique())

    casaram = municipios_enem & municipios_aneel
    orfaos_enem = municipios_enem - municipios_aneel
    orfaos_aneel = municipios_aneel - municipios_enem

    # Join considerando o município e o ano (se disponível em ambas)
    chaves_join = [coluna_chave]
    if "ano" in df_enem.columns and "ano" in df_aneel.columns:
        # Anos em comum
        anos_comum = set(df_enem["ano"].unique()) & set(df_aneel["ano"].unique())
        if anos_comum:
            chaves_join.append("ano")

    logger.info(f"Executando Inner Join ENEM x ANEEL usando as chaves: {chaves_join}")
    merged = pd.merge(
        df_enem, df_aneel, on=chaves_join, how="inner", suffixes=("_enem", "_aneel")
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
| Registros no join final (Silver Joined) | {len(merged)} |

## O que foi feito com os órfãos
Os municípios órfãos foram **excluídos** do join principal (inner join) e não entram na base ML-Ready, garantindo integridade estrita para o modelo de decisão.
"""

    SILVER_JOINED_DIR.mkdir(parents=True, exist_ok=True)
    RELATORIO_JOIN_PATH.write_text(relatorio, encoding="utf-8")

    # Salva dataset Silver Joined
    caminho_joined = SILVER_JOINED_DIR / "silver_joined.parquet"
    merged.to_parquet(caminho_joined, index=False)
    logger.info(f"Dataset Silver Joined salvo em: {caminho_joined}")
    logger.info(f"Relatório de auditoria salvo em {RELATORIO_JOIN_PATH}")
    print(relatorio)

    return merged


if __name__ == "__main__":
    process_aneel_silver()
    process_enem_silver()
    audit_join_silver()