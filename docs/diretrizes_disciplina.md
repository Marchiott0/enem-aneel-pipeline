# DIRETRIZES DA DISCIPLINA & ROTEIRO DE EXECUÇÃO

---

# PARTE 1: DIRETRIZES DA DISCIPLINA (DOCUMENTO DO PROFESSOR)

## 1. Objetivo
Construir, do zero, um pipeline de dados completo que parte de bases públicas reais, atravessa as camadas **Bronze**, **Silver** e **Gold**, e termina em uma recomendação de decisão sustentada por um modelo preditivo honesto. 

O projeto integra os três blocos vistos em sala:
1. **Ingestão de dados**;
2. **Boas práticas de pipeline**;
3. **Fundamentos ML-Ready**.

> **Nota de Avaliação:** O grupo não recebe o problema pronto. Vocês escolhem as bases, descobrem o que o cruzamento delas permite responder e decidem qual decisão o projeto vai apoiar. Essa escolha é parte da avaliação: um pipeline tecnicamente impecável que não serve para decidir nada vale menos do que um pipeline simples que muda a ação de alguém.

---

## 2. A Pergunta que o Projeto Precisa Responder
Ao final, o grupo deve conseguir completar esta frase com números do próprio pipeline:

> *"Cruzando as bases **X**, **Y** e **Z**, identificamos que **____**. Recomendamos que **____** faça **____** nos próximos **____**, priorizando **____**. Se agir, o ganho esperado é **____**; se errarmos, o custo é **____**."*

**Exigências da frase de decisão:**
- Um responsável concreto pela decisão;
- Uma ação possível e viável;
- Um prazo determinado;
- O custo explícito de errar (falsos positivos/negativos).

*Um número que não muda a ação de ninguém não é resultado — é curiosidade.*

---

## 3. Requisito 1 — Escolha e Cruzamento das Bases
- **Mínimo de 2 ou mais bases públicas reais**, de pelo menos duas instituições diferentes. *(Dados sintéticos, de exemplo ou de tutorial não são aceitos).*
- **Pelo menos 2 formatos distintos entre as fontes** (CSV, JSON, XLSX, Parquet, banco relacional, API REST).
- **Pelo menos uma fonte acessada por API e uma por arquivo.**
- **Chave de cruzamento explícita e documentada:** Código IBGE de município, UF, ano/mês, código INEP de escola, CNPJ, CID-10 ou equivalente.
- **Relatório de integridade do JOIN:** O grupo deve relatar quantos registros casaram e quantos ficaram órfãos no *join* — e o que fez com os órfãos.
- **Fontes abertas:** Fontes com licença de uso aberto, com URL e data de coleta registradas no dicionário de dados.
- *Recomendação:* Baixe uma amostra real de cada base antes de fechar o tema.

### 3.1. Catálogo de Fontes Sugeridas *(Conferidas em 26/08/2026)*

| Fonte | Endereço | O que oferece | Acesso | Chave típica |
| :--- | :--- | :--- | :--- | :--- |
| **IBGE** | [apisidra.ibge.gov.br](https://apisidra.ibge.gov.br) | População, PIB municipal, produção agrícola e tabelas SIDRA | API REST (JSON) | Código IBGE do município, ano |
| **Base dos Dados** | [basedosdados.org](https://basedosdados.org) | Centenas de bases brasileiras padronizadas (RAIS, Censo) | CSV, SQL BigQuery, Python/R | `id_municipio`, ano |
| **INEP** | [gov.br/inep](https://www.gov.br/inep) | Microdados: Censo Escolar, ENEM, Educação Superior | ZIP com CSV (separador `\|` ou `;`) | Código INEP da escola, município |
| **OpenDataSUS** | [opendatasus.saude.gov.br](https://opendatasus.saude.gov.br) | Mortalidade (SIM), notificações (SINAN), vacinação | CSV e API (CKAN) | Município, CID, competência |
| **INPE** | [data.inpe.br/queimadas](https://data.inpe.br/queimadas) | Focos de calor, séries anuais até tempo real | CSV e KML | Município, data |
| **INMET** | [bdmep.inmet.gov.br](https://bdmep.inmet.gov.br) | Séries históricas de chuva, temperatura, umidade, vento | CSV (ZIP anual ou estação) | Código da estação, data |
| **Portal da Transparência** | [portaldatransparencia.gov.br](https://portaldatransparencia.gov.br) | Contratos, licitações, despesas, benefícios, servidores | CSV e API REST (limite/min) | CNPJ, município, ano |
| **Banco Central** | [dadosabertos.bcb.gov.br](https://dadosabertos.bcb.gov.br) | Séries econômicas: câmbio, Selic, crédito, inadimplência | API REST JSON (máx. 10 anos) | Data |
| **PDET** | [gov.br/trabalho-e-emprego](https://www.gov.br/trabalho-e-emprego) | Microdados RAIS e Novo CAGED (admissões/desligamentos) | TXT (separador `;`) via FTP | Município, competência, CNAE |
| **ANP** | [gov.br/anp](https://www.gov.br/anp) | Série semanal de preços de combustíveis/GLP | CSV | Município, semana, CNPJ |
| **ANEEL** | [dadosabertos.aneel.gov.br](https://dadosabertos.aneel.gov.br) | Geração, tarifas, interrupções (Qualidade de energia) | CSV e JSON (CKAN API) | Município, distribuidora, mês |
| **Portal Brasileiro** | [dados.gov.br](https://dados.gov.br) | Catálogo geral do governo federal | Variado | Variado |

---

## 4. Requisito 2 — Ingestão de Dados
O pipeline deve implementar, no mínimo, três formas distintas de ingestão:
1. **Arquivo (CSV, JSON aninhado ou Excel):** com tratamento de encoding, separador e tipagem.
2. **API REST:** com paginação, tratamento de erro e retry com backoff exponencial.
3. **Banco relacional / Carga incremental:** full load ou incremental por watermark / micro-batch com checkpoint para fonte que se atualize.

### Obrigatoriedades da Ingestão:
- **Metadados Técnicos (Camada Bronze):** `_ingestion_time`, `_source`, `_load_id` e `_record_hash`.
- **Idempotência:** Rodar a ingestão duas vezes seguidas não pode duplicar dados nem gerar inconsistências.
- **Quarentena:** Registros problemáticos vão para uma área separada (`data/quarantine/`). O job de ingestão não pode quebrar por dado sujo.

---

## 5. Requisito 3 — Arquitetura Medalhão

| Camada | O que precisa estar lá | O que NÃO pode estar |
| :--- | :--- | :--- |
| **Bronze** | Dado bruto como veio da fonte, imutável, metadados técnicos de auditoria, particionamento por data. | Regras de negócio, deduplicação semântica, agregação, descarte de registros. |
| **Silver** | Tipagem forte, padronização de nomenclatura, chave explícita tratada, validações com quarentena, integridade relacional do join. | Dado sem contrato/schema definido; correções silenciosas sem rastreabilidade. |
| **Gold** | Tabelas orientadas à decisão: agregações analíticas, indicadores de negócio e a base **ML-Ready**. | Qualquer limpeza pesada ou parsing estrutural (isso deve ocorrer na Silver). |

---

## 6. Requisito 4 — Repositório e Reprodutibilidade
- Repositório Git estruturado: código-fonte (`src/`), notebooks, scripts e documentação.
- Arquivo `requirements.txt` com versões fixadas e `README.md` detalhado e claro.
- **Dados não vão para o Git:** Configurar rigorosamente o `.gitignore` para a pasta `data/`.
- **Seeds fixas:** Garantir reprodutibilidade total em todas as operações aleatórias e splits.
- **Dicionário de Dados:** Documentando colunas, tipos, domínios, origens e significado de cada variável.
- Histórico de commits distribuído e semântico.

---

## 7. Requisito 5 — Base ML-Ready e Anti-Vazamento

| Elemento | O que documentar |
| :--- | :--- |
| **Label** | O que é o positivo e em qual momento exato o evento ocorre. |
| **Regra de Rotulagem** | A condição formal do evento, reproduzível em código sem ambiguidade. |
| **Coorte** | Quem é elegível no $t_0$ e quais casos específicos foram excluídos. |
| **Ponto de Corte ($t_0$)** | O instante no tempo em que o conhecimento do modelo é rigorosamente congelado. |
| **Janela de Observação** | Período de onde vêm as features (estritamente anterior a $t_0$). |
| **Janela de Predição** | Período onde o label é observado e medido (posterior a $t_0$). |
| **Split** | Temporal, por grupo ou ambos (com justificativa formal). |
| **Baseline** | O modelo trivial contra o qual o modelo treinado será comparado. |
| **Métrica** | A métrica escolhida (ex: F1-Score, PR-AUC, Recall) e sua adequação ao impacto no negócio. |

### Checklist Anti-Vazamento (Obrigatório no Relatório):
- [ ] Toda e qualquer feature existia comprovadamente antes do $t_0$?
- [ ] As agregações temporais usaram apenas dados estritamente anteriores ao $t_0$?
- [ ] O split respeita o corte temporal e a separação de grupos/municípios?
- [ ] Scalers, encoders e imputers foram ajustados (*fit*) **exclusivamente** no conjunto de treino?

---

## 8. Requisito 6 & 9 — Tomada de Decisão, IA e Integridade
- Declarar o decisor real, a ação concreta a ser tomada, os custos detalhados de falsos positivos e falsos negativos, o limiar (*threshold*) de decisão e as limitações do pipeline.
- Ferramentas de IA generativa são permitidas e devem ser declaradas no README. Todo integrante do grupo deve saber explicar qualquer trecho de código.
- Dados sensíveis ou pessoais (PII) do ENEM devem ser anonimizados e agregados logo na camada Silver.

---
---

# PARTE 2: ROTEIRO DE EXECUÇÃO DO GRUPO (ENEM + ANEEL)

## 1. A Frase de Decisão (O Escopo de Negócio)
> *"Cruzando as bases do **ENEM (INEP)** e **Qualidade de Energia (ANEEL)**, identificamos que municípios com alta instabilidade elétrica nos meses de preparação sofrem impacto drástico na abstenção e nas notas. Recomendamos que a **Secretaria de Educação do Estado do Pará (SEDUC-PA)** e o **MEC** façam a **alocação de geradores móveis e distribuição de planos de estudo offline** nos próximos **3 meses (agosto a outubro)**, priorizando os **50 municípios** com maior probabilidade predita de falha estrutural na infraestrutura elétrica. Se agir, o ganho esperado é **garantir acesso equitativo e evitar a evasão de até milhares de candidatos**; se errarmos, o custo é **referente à logística e locação de geradores para polos de prova**."*

---

## 2. Especificação da Base ML-Ready (Requisito 5)
- **Ponto de Corte ($t_0$):** 31 de Julho do ano de aplicação do ENEM.
- **Janela de Observação:** Janeiro a Julho do mesmo ano (Features de energia: FEC, DEC médios das distribuidoras municipais).
- **Janela de Predição:** Novembro (Mês oficial de aplicação das provas do ENEM).
- **Label (`abstencao_critica`):** `1` se o município teve abstenção $> 35\%$ no ENEM; `0` caso contrário.
- **Coorte:** Municípios do estado do Pará (`UF = 'PA'`) que possuam polos de prova cadastrados no INEP.
- **Split:** Temporal. Treinar com o histórico de anos anteriores e testar no ano mais recente (impede vazamento temporal).
- **Chave de Cruzamento:** Código IBGE do Município (7 dígitos numéricos).

---

## 3. Módulos Implementados no Código-Fonte

O código oficial e executável do pipeline encontra-se organizado na pasta `src/`:

1. **Ingestão ANEEL (API REST JSON):**
   - Arquivo: [`src/ingestion/ingest_aneel_api.py`](file:///c:/Users/SuporteACC/Desktop/py/C/isaacprofessor/src/ingestion/ingest_aneel_api.py)
   - Atende aos requisitos de paginação (`limit`/`offset`), retry com backoff exponencial via `tenacity`, 4 metadados técnicos obrigatórios, idempotência com hash SHA-256 e quarentena de falhas.

2. **Ingestão ENEM (Arquivo CSV):**
   - Arquivo: [`src/ingestion/ingest_enem_csv.py`](file:///c:/Users/SuporteACC/Desktop/py/C/isaacprofessor/src/ingestion/ingest_enem_csv.py)
   - Atende aos requisitos de leitura em lotes (`chunksize`), tratamento de delimitador/encoding e anexação de metadados técnicos.

3. **Processamento Silver & Auditoria de JOIN:**
   - Arquivo: [`src/processing/bronze_to_silver.py`](file:///c:/Users/SuporteACC/Desktop/py/C/isaacprofessor/src/processing/bronze_to_silver.py)
   - Anonimização LGPD, tipagem forte e contagem de municípios casados e órfãos.

4. **Camada Gold ML-Ready & Modelagem:**
   - Arquivos: [`src/processing/silver_to_gold.py`](file:///c:/Users/SuporteACC/Desktop/py/C/isaacprofessor/src/processing/silver_to_gold.py), [`src/model/train_predictor.py`](file:///c:/Users/SuporteACC/Desktop/py/C/isaacprofessor/src/model/train_predictor.py) e [`src/model/evaluate.py`](file:///c:/Users/SuporteACC/Desktop/py/C/isaacprofessor/src/model/evaluate.py).
