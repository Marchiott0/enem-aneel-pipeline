# Modelagem ML-Ready e Anti-Vazamento (Requisitos 5 e 6)

Código: `src/model/train_predictor.py` (treino e métricas) e `src/model/evaluate.py` (decisão).

## 1. Definição do problema (documentada antes de treinar)

| Elemento | Definição |
| :--- | :--- |
| Label | Positivo = município com abstenção no ENEM > 35%. Só se sabe após a aplicação da prova. |
| Regra de rotulagem | `target_abstencao_critica = (taxa_abstencao > 0.35)`, em `silver_to_gold.py`. |
| Coorte | Município-ano com os 7 meses (jan-jul) de energia observados (`meses_observados == 7`). Janela incompleta subestimaria as somas de horas. A quantidade excluída aparece no log. |
| Ponto de corte (t0) | 31 de julho do ano do exame. |
| Janela de observação | Janeiro a julho (features de DEC/FEC), sempre anterior ao t0. |
| Janela de predição | Abstenção observada na aplicação do ENEM do mesmo ano, posterior ao t0. |
| Split | Temporal: treino = todos os anos anteriores ao último; teste = o último ano da Gold. |
| Baselines | (1) moda (`DummyClassifier`); (2) persistência: abstenção do ano anterior do mesmo município. |
| Métrica | PR-AUC (principal, adequada a classe positiva minoritária), mais precisão e recall no top-50 (a decisão escolhe 50 municípios). ROC-AUC, F1 e acurácia são complementares; a acurácia engana com classes desbalanceadas. |

## 2. Checklist anti-vazamento

1. **Toda feature existia antes do t0?** Sim. As 7 features vêm só de DEC/FEC de jan-jul. As colunas `taxa_abstencao`, `total_abstencao`, `total_inscritos` e o target são barradas por `assert` no treino.
2. **Agregações só com dados anteriores ao t0?** Sim. `silver_to_gold.py` filtra `mes <= 7` antes de agregar. A persistência usa a abstenção de novembro do ano anterior, que é anterior ao t0.
3. **O split respeita tempo e grupo?** Respeita o tempo (treino sempre anterior ao teste). Por grupo, não: os mesmos municípios aparecem em treino e teste, em anos diferentes. É intencional, porque a decisão se repete todo ano sobre os mesmos municípios. Separar por município mediria generalização para municípios nunca vistos, que é outra pergunta.
4. **Scalers, encoders e imputadores ajustados só no treino?** Não há scaler nem encoder (features numéricas, Random Forest). Não há imputador: o Random Forest do scikit-learn >= 1.4 aceita NaN. Hiperparâmetros fixos, sem ajuste olhando o teste.

Se o ROC-AUC for muito alto (> 0,95), investigue vazamento antes de comemorar.

## 3. Decisão (Requisito 6)

- **Decisor:** SEDUC-PA e MEC. **Ação:** alocar geradores móveis e planos de estudo offline em 50 municípios entre agosto e outubro.
- **Limiar:** top-50 do ranking de risco, definido pela capacidade de orçamento (50 geradores), não por uma probabilidade arbitrária como 0,5.
- **Custo do falso positivo:** R$ 15.000 por gerador em município que não precisava. O treino imprime `(1 - precisão_top50) × 50 × 15.000`.
- **Custo do falso negativo:** município crítico sem gerador. Ainda não está quantificado em R$ pelo grupo; a decisão deve citá-lo de forma qualitativa ou o grupo deve definir um valor.
- **Métrica ligada à consequência:** precisão@50 = fração dos geradores bem gastos; recall@50 = fração dos municípios críticos cobertos.

## 4. Limitações

- Associação, não causalidade: renda, distância, transporte e clima não estão nos dados.
- DEC/FEC são médias municipais de conjuntos elétricos; não captam falta de energia no local de prova.
- Cerca de 144 linhas por ano no Pará: o teste de um único ano é ruidoso.
- O limiar de 35% é fixo para todos os anos. Em anos atípicos (2020 e 2021, pandemia; o ENEM 2020 foi aplicado em janeiro/fevereiro de 2021) a taxa de positivos muda muito. Confira a tabela "Positivos por ano" no log.
- Se a persistência vencer o modelo de energia, a energia não agrega informação além do histórico, e o relatório deve dizer isso.
