"""
Script de treinamento do modelo preditivo com salvaguardas anti-leakage.

Responsabilidades:
- Split estritamente temporal (Treino: 2020 a 2023 | Teste: 2024);
- Cálculo do Baseline trivial para comparação (DummyClassifier);
- Treinamento do classificador Random Forest com semente fixa;
- Exibição estruturada e explicada de cada métrica para a banca.
- NOVO: coorte (só municípios com os 7 meses de energia), checagens anti-vazamento,
  PR-AUC e top-K (métricas para classe desbalanceada) e baseline de persistência.
"""
import numpy as np  # NOVO: usado no top-K
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, f1_score, average_precision_score
from src.config import GOLD_ML_READY_DIR, RANDOM_SEED
from src.utils.logger import get_logger

logger = get_logger("train_predictor")

K = 50  # NOVO: capacidade da decisão (quantos municípios recebem gerador), igual ao evaluate.py


def precisao_recall_topk(y, score, k=K):
    """NOVO: entre os k municípios de maior score, quantos eram críticos (precisão) e quantos dos críticos reais entraram (recall)."""
    topo = y[np.argsort(-score, kind="stable")[:k]]
    return topo.mean(), topo.sum() / y.sum()


def train():
    logger.info("Carregando base Gold ML-Ready...")
    ml_file = GOLD_ML_READY_DIR / "enem_aneel_ml_ready.parquet"
    if not ml_file.exists():
        logger.error(f"Base {ml_file} não encontrada. Execute silver_to_gold.py antes.")
        return None

    df = pd.read_parquet(ml_file)

    # NOVO: coorte. Só entra município-ano com os 7 meses (jan-jul) observados;
    # com menos meses, as somas de horas de energia ficam subestimadas e enganam o modelo.
    n_antes = len(df)
    df = df[df["meses_observados"] == 7].copy()
    logger.info(f"Coorte: {len(df)} de {n_antes} municipio-ano ({n_antes - len(df)} excluidos por janela incompleta).")
    logger.info("Positivos por ano:\n" + df.groupby("ano")["target_abstencao_critica"].agg(n="size", taxa_positivos="mean").round(3).to_string())

    features = ["fec_medio_jan_jul", "dec_horas_total_jan_jul", "dec_horas_medio_jan_jul",
            "fec_medio_reta_final", "dec_horas_total_reta_final", "dec_horas_pior_mes", "dec_variacao_ano"]
    target = "target_abstencao_critica"

    # NOVO: salvaguardas. Colunas que só existem depois da prova nunca podem ser feature,
    # e a chave (município, ano) não pode ter duplicata.
    colunas_do_futuro = {"taxa_abstencao", "total_abstencao", "total_inscritos", target}
    assert not colunas_do_futuro & set(features), "Feature vinda do futuro (vazamento)."
    assert not df.duplicated(["codigo_municipio", "ano"]).any(), "Chave (municipio, ano) duplicada."

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
    # NOVO: class_weight compensa a classe crítica ser minoria. Obs.: o RandomForest do sklearn>=1.4
    # aceita NaN nas features (ex.: dec_variacao_ano sem mês 1), então não precisa imputar.
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, class_weight="balanced_subsample", random_state=RANDOM_SEED)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    y_prob = clf.predict_proba(X_test)[:, 1]

    # CORRIGIDO: roc_auc_score quebra se o ano de teste tiver uma só classe.
    if y_test.nunique() < 2:
        logger.warning("Ano de teste com uma so classe: ROC-AUC/PR-AUC nao definidos.")
        return clf

    roc = roc_auc_score(y_test, y_prob)
    pr_auc = average_precision_score(y_test, y_prob)  # NOVO: métrica principal para classe desbalanceada
    f1_mod = f1_score(y_test, y_pred, zero_division=0)
    acc = (y_pred == y_test).mean()

    # NOVO: baseline de persistência = abstenção do ano anterior do mesmo município
    # (novembro do ano anterior é anterior ao t0, então não é vazamento).
    anterior = df[["codigo_municipio", "ano", "taxa_abstencao"]].assign(ano=df["ano"] + 1)
    anterior = anterior.rename(columns={"taxa_abstencao": "taxa_ano_anterior"})
    teste = df.loc[test_mask, ["codigo_municipio", "ano"]].merge(anterior, on=["codigo_municipio", "ano"], how="left")
    score_persist = teste["taxa_ano_anterior"].fillna(teste["taxa_ano_anterior"].median()).to_numpy()
    y_arr = y_test.to_numpy()
    pr_persist = average_precision_score(y_arr, score_persist)
    prec_k, rec_k = precisao_recall_topk(y_arr, y_prob)
    prec_k_persist, _ = precisao_recall_topk(y_arr, score_persist)
    pr_base = y_arr.mean()  # PR-AUC do baseline "moda" = proporção de positivos no teste

    # Importância das Features
    importances = dict(zip(features, clf.feature_importances_))

    print("\n" + "=" * 70)
    print("  [MODELAGEM PREDITIVA - RESULTADOS E EXPLICACAO DOS DADOS]")
    print("=" * 70)
    print("  1. DIVISAO TEMPORAL DOS DADOS (REGRA ANTI-VAZAMENTO):")
    print(f"     * Dados de Treino: Anos {[int(a) for a in anos_treino]} ({len(X_train)} municipios-ano)")
    print(f"       -> O QUE E: O passado historico usado para o algoritmo aprender.")
    print(f"       -> PRA QUE SERVE: Garante que o modelo so enxergue o passado.")
    print(f"     * Dados de Teste: Ano {ano_teste} ({len(X_test)} municipios)")
    print(f"       -> O QUE E: O ano futuro usado para testar se ele prevê direito.")
    print(f"       -> PRA QUE SERVE: Simula a vida real (prever a prova antes de acontecer).")
    print("\n  2. COMPARACAO COM O BASELINE TRIVIAL (EXIGENCIA DO PROFESSOR):")
    print(f"     * F1-Score do Baseline (Regra Cega / Chute mais comum): {f1_base:.4f}")
    print(f"       -> O QUE E: A pontuacao de quem chuta que tudo vai ficar calmo.")
    print(f"     * F1-Score do Modelo Inteligente: {f1_mod:.4f}")
    print(f"       -> PRA QUE SERVE: Mostra se olhar a energia melhora a previsao (CORRIGIDO: nao e "prova"; compare tambem com a persistencia).")
    print(f"     * PR-AUC do Baseline (moda): {pr_base:.4f} | do Modelo: {pr_auc:.4f}  (NOVO)")
    print(f"       -> O QUE E: area sob a curva precisao x recall; a referencia do acaso e a proporcao de positivos.")
    print(f"     * PR-AUC do Baseline de Persistencia (abstencao do ano anterior): {pr_persist:.4f}  (NOVO)")
    print(f"       -> PRA QUE SERVE: se a persistencia ganhar do modelo, a energia nao agrega alem do historico.")
    # CORRIGIDO: o ano estava fixo em "2024" no texto.
    print(f"\n  3. METRICAS DE PREVISAO NO ANO FUTURO ({ano_teste}):")
    print(f"     * Acuracia Geral: {acc:.1%} de acertos gerais (enganosa com classes desbalanceadas; use PR-AUC)")
    print(f"     * ROC-AUC Score: {roc:.4f}")
    print(f"       -> O QUE E: Capacidade do modelo de separar cidades criticas de cidades calmas.")
    print(f"     * Precisao no top-{K}: {prec_k:.1%} (persistencia: {prec_k_persist:.1%}) | Recall no top-{K}: {rec_k:.1%}  (NOVO)")
    print(f"       -> O QUE E: dos {K} municipios que receberiam gerador, quantos eram criticos de fato.")
    print(f"       -> PRA QUE SERVE: liga a metrica ao custo: R$ {(1 - prec_k) * K * 15000:,.0f} em geradores alocados sem necessidade.")
    print("\n  4. O QUE MAIS EXPLICA A FALTA DOS ALUNOS (PESO DE CADA DADO):")
    nomes_amigaveis = {
        "dec_horas_total_jan_jul": "Horas Totais no Escuro (DEC Jan-Jul)",
        "fec_medio_jan_jul": "Frequencia de Quedas de Energia (FEC Jan-Jul)",
        "dec_horas_medio_jan_jul": "Duracao Media de Cada Falha (DEC Medio Jan-Jul)",
        # CORRIGIDO: as outras 4 features apareciam com o nome técnico
        "fec_medio_reta_final": "Frequencia de Quedas na Reta Final (FEC Mai-Jul)",
        "dec_horas_total_reta_final": "Horas no Escuro na Reta Final (DEC Mai-Jul)",
        "dec_horas_pior_mes": "Pior Mes de Horas no Escuro (Mai-Jul)",
        "dec_variacao_ano": "Variacao do DEC (pior mes de Mai-Jul menos Janeiro)",
    }
    for feat, imp in importances.items():
        rotulo = nomes_amigaveis.get(feat, feat)
        print(f"     * {rotulo}: {imp:.1%}")
    print("=" * 70 + "\n")

    return clf


if __name__ == "__main__":
    train()
