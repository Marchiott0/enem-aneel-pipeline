import pandas as pd
from src.config import SILVER_ENEM_DIR, SILVER_ANEEL_DIR

def check_enem():
    df = pd.read_parquet(SILVER_ENEM_DIR / "enem_silver.parquet")
    print("--- ENEM Silver ---")
    print(f"Linhas: {len(df)} | Duplicatas (município+ano): {df.duplicated(['codigo_municipio','ano']).sum()}")
    print(f"taxa_abstencao fora de [0,1]: {((df['taxa_abstencao'] < 0) | (df['taxa_abstencao'] > 1)).sum()}")
    for col in [c for c in df.columns if c.startswith("media_nota_")]:
        fora = ((df[col] < 0) | (df[col] > 1000)).sum()
        print(f"{col}: fora de [0,1000] = {fora} | nulos = {df[col].isna().sum()}")
    print(f"total_inscritos <= 0: {(df['total_inscritos'] <= 0).sum()}")

def check_aneel():
    df = pd.read_parquet(SILVER_ANEEL_DIR / "aneel_silver.parquet")
    print("\n--- ANEEL Silver ---")
    print(f"Linhas: {len(df)} | Duplicatas (município+ano+mês): {df.duplicated(['codigo_municipio','ano','mes']).sum()}")
    print(f"dec_horas_mensal negativo: {(df['dec_horas_mensal'] < 0).sum()}")
    print(f"dec_horas_mensal > 744h (impossível num mês): {(df['dec_horas_mensal'] > 744).sum()}")
    print(f"fec_freq_mensal negativo: {(df['fec_freq_mensal'] < 0).sum()}")
    print(f"mês fora de 1-12: {(~df['mes'].between(1,12)).sum()}")

if __name__ == "__main__":
    check_enem()
    check_aneel()