"""
Script de avaliação de impacto da decisão de negócio e custos de erro.

Responsabilidades:
- Ranqueamento dos 50 municípios mais vulneráveis à instabilidade elétrica;
- Cálculo do ganho social (população estudantil protegida e acertos críticos);
- Análise de custos de falsos positivos (locação preventiva) e falsos negativos (evasão);
- Exibição didática e estruturada para a banca examinadora.
"""
import pandas as pd
import numpy as np
from src.config import GOLD_ML_READY_DIR
from src.model.train_predictor import train
from src.utils.logger import get_logger

logger = get_logger("evaluate_decision")


def simulate_decision_impact(n_priorizados: int = 50, custo_por_gerador: float = 15000.0, clf=None):
    """
    Simula a matriz de ganhos e custos de decisão para apoiar a Secretaria
    de Educação do Estado do Pará (SEDUC-PA) e o MEC na priorização dos 50 municípios.
    """
    logger.info("Avaliando impacto de decisão para a SEDUC-PA / MEC...")

    ml_file = GOLD_ML_READY_DIR / "enem_aneel_ml_ready.parquet"
    if not ml_file.exists():
        logger.error(f"Arquivo {ml_file} não encontrado. Execute silver_to_gold.py antes.")
        return None

    df = pd.read_parquet(ml_file)
    ano_teste = int(df["ano"].max())
    df_teste = df[df["ano"] == ano_teste].copy()

    # Se o classificador não foi passado, treina
    if clf is None:
        clf = train()
    if clf is None:
        return None

    features = ["fec_medio_jan_jul", "dec_horas_total_jan_jul", "dec_horas_medio_jan_jul",
            "fec_medio_reta_final", "dec_horas_total_reta_final", "dec_horas_pior_mes", "dec_variacao_ano"]
    df_teste["prob_risco_eletrico"] = clf.predict_proba(df_teste[features])[:, 1]

    # Ordena pelos municípios de maior risco predito
    df_priorizados = df_teste.sort_values(by="prob_risco_eletrico", ascending=False).head(n_priorizados)

    total_alunos_impactados = int(df_priorizados["total_inscritos"].sum())
    acertos_criticos = int((df_priorizados["target_abstencao_critica"] == 1).sum())
    falsos_positivos = int((df_priorizados["target_abstencao_critica"] == 0).sum())
    custo_total_alocacao = n_priorizados * custo_por_gerador

    print("\n" + "=" * 70)
    print(f"  [RELATORIO DE DECISAO - RECOMENDACAO PARA SEDUC-PA E MEC]")
    print("=" * 70)
    print("  1. A FRASE DE DECISAO (O ESCOPO DO PROJETO):")
    print("     'Recomendamos que a SEDUC-PA e o MEC facam a alocacao preventiva")
    print("      de geradores moveis nos 50 municipios do Para de maior probabilidade")
    print("      de instabilidade eletrica nos 3 meses anteriores ao ENEM.'")
    print("\n  2. METRICAS DA INTERVENCAO (O QUE CADA DADO SIGNIFICA):")
    print(f"     * Municipios Priorizados: {n_priorizados} cidades (de {len(df_teste)} polos no Para)")
    print(f"       -> O QUE E: O ranking das cidades que mais sofrem com falta de luz.")
    print(f"       -> PRA QUE SERVE: Foca o orcamento publico onde o risco e mais grave.")
    print(f"     * Cidades Criticas Protegidas (Verdadeiros Positivos): {acertos_criticos} municipios")
    print(f"       -> O QUE E: Cidades onde haveria crise de falta e o gerador garantiu a prova.")
    print(f"       -> PRA QUE SERVE: Mede o ganho real de equidade e acesso ao ensino superior.")
    print(f"     * Alocacoes de Precaucao (Falsos Positivos): {falsos_positivos} municipios")
    print(f"       -> O QUE E: Cidades que receberam gerador, mas a evasao nao passou de 35%.")
    print(f"       -> PRA QUE SERVE: Funciona como seguro preventivo (melhor ter do que faltar).")
    print(f"     * Total de Estudantes Protegidos: {total_alunos_impactados:,} candidatos")
    print(f"       -> O QUE E: Quantidade de jovens que puderam fazer o ENEM com energia estavel.")
    print("\n  3. MATRIZ FINANCEIRA E CUSTOS DE ERRAR (EXIGENCIA DA BANCA):")
    print(f"     * Orcamento Publico da Acao: R$ {custo_total_alocacao:,.2f}")
    print(f"       -> Calculo: {n_priorizados} polos x R$ 15.000,00 (locacao por 3 meses + combustivel).")
    print("     * Custo do Falso Positivo (Gastar de forma preventiva):")
    print("       -> R$ 15.000,00 de custo logistico por polo alocado desnecessariamente.")
    print("     * Custo do Falso Negativo (Nao enviar gerador onde faltou energia):")
    print("       -> Interrupcao do exame, anulacao de provas e evasao definitiva de estudantes.")
    print("=" * 70 + "\n")

    return df_priorizados


if __name__ == "__main__":
    simulate_decision_impact()
