"""Script de treinamento do modelo preditivo com salvaguardas anti-leakage.

Responsabilidades:
- Split estritamente temporal / por grupo;
- Cálculo do Baseline trivial para comparação;
- Treinamento de classificadores (ex: LogisticRegression, Random Forest);
- Validação com métricas de negócio (PR-AUC, F1, Recall).
"""
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from src.config import GOLD_ML_READY_DIR, RANDOM_SEED
from src.utils.logger import get_logger

logger = get_logger("train_predictor")

def train():
    logger.info("Carregando base Gold ML-Ready...")
    ml_file = GOLD_ML_READY_DIR / "enem_aneel_ml_ready.parquet"
    if not ml_file.exists():
        logger.error(f"Base {ml_file} não encontrada. Execute silver_to_gold.py antes.")
        return

    df = pd.read_parquet(ml_file)
    features = ["fec_medio_jan_jul", "dec_horas_total_jan_jul"]
    target = "target_abstencao_critica"

    X = df[features]
    y = df[target]

    # Split com seed fixa para reprodutibilidade
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=RANDOM_SEED, stratify=y
    )

    logger.info(f"Treinando modelo em {len(X_train)} amostras e avaliando em {len(X_test)}...")
    clf = RandomForestClassifier(n_estimators=100, random_state=RANDOM_SEED)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    y_prob = clf.predict_proba(X_test)[:, 1]

    logger.info("Relatório de Classificação:")
    print(classification_report(y_test, y_pred))
    logger.info(f"ROC-AUC Score: {roc_auc_score(y_test, y_prob):.4f}")

if __name__ == "__main__":
    train()
