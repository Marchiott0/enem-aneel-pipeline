"""Script de transformação Silver para Gold (Dataset ML-Ready).

Responsabilidades:
- Aplicação rigorosa do ponto de corte t0 (31 de Julho);
- Agregação de métricas de energia elétrica na Janela de Observação (Jan a Jul);
- Construção do target de abstenção na Janela de Predição (Novembro);
- Garantia de blindagem anti-leakage (sem contaminação futura);
- Exportação da base final orientada à decisão para data/gold/ml_ready/.
"""
import pandas as pd
import numpy as np
from src.config import SILVER_JOINED_DIR, GOLD_ANALYTICS_DIR, GOLD_ML_READY_DIR, T0_DATE
from src.utils.logger import get_logger

logger = get_logger("silver_to_gold")

def create_ml_ready_dataset():
    """Gera o dataset ML-Ready a partir da camada Silver com validação anti-vazamento."""
    logger.info(f"Gerando dataset Gold ML-Ready com corte temporal t0={T0_DATE} (Jan a Jul)...")
    
    joined_file = SILVER_JOINED_DIR / "silver_joined.parquet"
    if not joined_file.exists():
        raise FileNotFoundError(f"Arquivo {joined_file} não encontrado. Execute bronze_to_silver.py antes.")

    df_joined = pd.read_parquet(joined_file)
    logger.info(f"Carregados {len(df_joined)} registros de Silver Joined.")

    # 1. Filtro rigoroso anti-leakage: Janela de Observação estritamente anterior a t0 (meses 1 a 7)
    df_obs = df_joined[df_joined["mes"] <= 7].copy()
    logger.info(f"Registros na janela de observação (Jan-Jul, meses 1-7): {len(df_obs)}")

    # Features adicionais: reta final antes da prova (maio a julho)
    df_reta_final = df_obs[df_obs["mes"] >= 5].copy()  # maio, junho, julho

    df_features_reta_final = df_reta_final.groupby(
        ["codigo_municipio", "ano"], as_index=False
    ).agg(
        fec_medio_reta_final=("fec_freq_mensal", "mean"),
        dec_horas_total_reta_final=("dec_horas_mensal", "sum"),
        dec_horas_pior_mes=("dec_horas_mensal", "max"),
    )

    df_mes1 = df_obs[df_obs["mes"] == 1][["codigo_municipio", "ano", "dec_horas_mensal"]] \
        .rename(columns={"dec_horas_mensal": "dec_horas_mes1"})

    # 2. Agregação das features de qualidade de energia elétrica por município e ano
    df_features = df_obs.groupby(["codigo_municipio", "NomMunicipio", "ano"], as_index=False).agg(
        fec_medio_jan_jul=("fec_freq_mensal", "mean"),
        dec_horas_total_jan_jul=("dec_horas_mensal", "sum"),
        dec_horas_medio_jan_jul=("dec_horas_mensal", "mean"),
        meses_observados=("mes", "nunique")
    )
    df_features["fec_medio_jan_jul"] = df_features["fec_medio_jan_jul"].round(2)
    df_features["dec_horas_total_jan_jul"] = df_features["dec_horas_total_jan_jul"].round(2)
    df_features["dec_horas_medio_jan_jul"] = df_features["dec_horas_medio_jan_jul"].round(2)
    # Junta as features novas ao dataframe principal de features
    df_features = df_features.merge(df_features_reta_final, on=["codigo_municipio", "ano"], how="left")
    df_features = df_features.merge(df_mes1, on=["codigo_municipio", "ano"], how="left")
    df_features["dec_variacao_ano"] = (
        df_features["dec_horas_pior_mes"] - df_features["dec_horas_mes1"]
    ).round(2)

    # 3. Informações de desfecho do ENEM (Target observado no final do ano)
    df_target = (
        df_joined[["codigo_municipio", "ano", "total_inscritos", "total_abstencao", "taxa_abstencao"]]
        .drop_duplicates(subset=["codigo_municipio", "ano"])
    )

    # Cruzamento de features com target por município e ano
    df_ml = pd.merge(df_features, df_target, on=["codigo_municipio", "ano"], how="inner")

    # 4. Definição formal do Target de Decisão (Requisito da disciplina)
    # Abstenção crítica = 1 se abstenção municipal > 35%, 0 caso contrário
    df_ml["target_abstencao_critica"] = (df_ml["taxa_abstencao"] > 0.35).astype(int)

    GOLD_ML_READY_DIR.mkdir(parents=True, exist_ok=True)
    ml_output_path = GOLD_ML_READY_DIR / "enem_aneel_ml_ready.parquet"
    df_ml.to_parquet(ml_output_path, index=False)
    logger.info(
        f"Dataset Gold ML-Ready gerado com sucesso ({len(df_ml)} linhas município-ano): {ml_output_path}"
    )

    # Exibe resumo pedagógico e estruturado da base Gold ML-Ready
    taxa_positivos = df_ml['target_abstencao_critica'].mean()
    contagem_anos = df_ml["ano"].value_counts().sort_index().to_dict()

    print("\n" + "-" * 70)
    print("  [CAMADA GOLD - DATASET ML-READY]")
    print("-" * 70)
    print(f"  * Total de Registros: {len(df_ml)} linhas (1 linha por municipio por ano)")
    print(f"  * Granularidade: 144 municipios do Para ao longo de 5 anos")
    print(f"  * Distribuicao Anual: {contagem_anos}")
    print(f"  * O que significa: Cada registro junta a energia (Jan-Jul) com o ENEM (Nov).")
    print(f"  * Para que serve: Base congelada em t0 (31/07) sem risco de vazamento temporal.")
    print(f"  * Taxa de Crise (Alvo > 35% de falta): {taxa_positivos:.1%}")
    print(f"  * O que significa: Em {taxa_positivos:.1%} dos casos municipais, houve abstenção crítica.")
    print(f"  * Para que serve: Define a meta (label) que o modelo precisa prever.")
    print("-" * 70 + "\n")

    return df_ml

if __name__ == "__main__":
    create_ml_ready_dataset()

