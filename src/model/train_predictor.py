"""
Script de treinamento do modelo preditivo com salvaguardas anti-leakage.

Responsabilidades:
- Split estritamente temporal (Treino: 2020 a 2023 | Teste: 2024);
- Cálculo do Baseline trivial para comparação (DummyClassifier);
- Treinamento do classificador Random Forest com semente fixa;
- Exibição estruturada e explicada de cada métrica para a banca.
"""
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, f1_score
from src.config import GOLD_ML_READY_DIR, RANDOM_SEED
from src.utils.logger import get_logger

logger = get_logger("train_predictor")


def train():
    logger.info("Carregando base Gold ML-Ready...")
    ml_file = GOLD_ML_READY_DIR / "enem_aneel_ml_ready.parquet"
    if not ml_file.exists():
        logger.error(f"Base {ml_file} não encontrada. Execute silver_to_gold.py antes.")
        return None

    df = pd.read_parquet(ml_file)
    features = ["fec_medio_jan_jul", "dec_horas_total_jan_jul", "dec_horas_medio_jan_jul"]
    target = "target_abstencao_critica"

    # Split Estritamente Temporal (Salvaguarda Anti-Leakage Obrigatória)
    ano_teste = df["ano"].max()
    train_mask = df["ano"] < ano_teste
    test_mask = df["ano"] == ano_teste

    X_train = df.loc[train_mask, features]
    y_train = df.loc[train_mask, target]
    X_test = df.loc[test_mask, features]
    y_test = df.loc[test_mask, target]

    anos_treino = sorted(df.loc[train_mask, "ano"].unique())
    logger.info(f"Split Temporal: Treino={anos_treino} ({len(X_train)} amostras) | Teste={ano_teste} ({len(X_test)} amostras)")

    # 1. Baseline Trivial (DummyClassifier - Regra Cega de Classe Majoritária)
    baseline = DummyClassifier(strategy="most_frequent")
    baseline.fit(X_train, y_train)
    y_base_pred = baseline.predict(X_test)
    f1_base = f1_score(y_test, y_base_pred, zero_division=0)

    # 2. Treinamento do Modelo Preditivo (Random Forest)
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=RANDOM_SEED)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    y_prob = clf.predict_proba(X_test)[:, 1]

    roc = roc_auc_score(y_test, y_prob)
    f1_mod = f1_score(y_test, y_pred, zero_division=0)
    acc = (y_pred == y_test).mean()

    # Importância das Features
    importances = dict(zip(features, clf.feature_importances_))

    print("\n" + "=" * 70)
    print("  [MODELAGEM PREDITIVA - RESULTADOS E EXPLICACAO DOS DADOS]")
    print("=" * 70)
    print("  1. DIVISAO TEMPORAL DOS DADOS (REGRA ANTI-VAZAMENTO):")
    print(f"     * Dados de Treino: Anos {[int(a) for a in anos_treino]} ({len(X_train)} municipios-ano)")
    print(f"       -> O QUE E: O passado historico usado para o algoritmo aprender.")
    print(f"       -> PRA QUE SERVE: Garante que o modelo so enxergue o passado.")
    print(f"     * Dados de Teste: Ano {ano_teste} ({len(X_test)} municipios do Para)")
    print(f"       -> O QUE E: O ano futuro usado para testar se ele prevê direito.")
    print(f"       -> PRA QUE SERVE: Simula a vida real (prever a prova antes de acontecer).")
    print("\n  2. COMPARACAO COM O BASELINE TRIVIAL (EXIGENCIA DO PROFESSOR):")
    print(f"     * F1-Score do Baseline (Regra Cega / Chute mais comum): {f1_base:.4f}")
    print(f"       -> O QUE E: A pontuacao de quem chuta que tudo vai ficar calmo.")
    print(f"     * F1-Score do Modelo Inteligente: {f1_mod:.4f}")
    print(f"       -> PRA QUE SERVE: Prova cientifica que olhar a energia melhora a previsao.")
    print("\n  3. METRICAS DE PREVISAO NO ANO FUTURO (2024):")
    print(f"     * Acuracia Geral: {acc:.1%} de acertos gerais")
    print(f"     * ROC-AUC Score: {roc:.4f}")
    print(f"       -> O QUE E: Capacidade do modelo de separar cidades criticas de cidades calmas.")
    print("\n  4. O QUE MAIS EXPLICA A FALTA DOS ALUNOS (PESO DE CADA DADO):")
    nomes_amigaveis = {
        "dec_horas_total_jan_jul": "Horas Totais no Escuro (DEC Jan-Jul)",
        "fec_medio_jan_jul": "Frequencia de Quedas de Energia (FEC Jan-Jul)",
        "dec_horas_medio_jan_jul": "Duracao Media de Cada Falha (DEC Medio Jan-Jul)"
    }
    for feat, imp in importances.items():
        rotulo = nomes_amigaveis.get(feat, feat)
        print(f"     * {rotulo}: {imp:.1%}")
    print("=" * 70 + "\n")

    return clf


if __name__ == "__main__":
    train()
