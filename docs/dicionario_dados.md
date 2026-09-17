# Dicionário de Dados — Projeto Integrador (ENEM + ANEEL)
*Versão: 2.2.0 | Atualizado conforme diretrizes da disciplina*

Este documento descreve detalhadamente as bases utilizadas, suas origens oficiais, formatos de ingestão, contratos de dados, colunas, tipos de dados, domínios e significado de cada variável ao longo das camadas **Bronze**, **Silver** e **Gold**.

---

## 1. Base 1: Qualidade dos Serviços de Distribuição de Energia (ANEEL)

- **Órgão Responsável:** Agência Nacional de Energia Elétrica (ANEEL) / Ministério de Minas e Energia (MME)
- **Origem / URL Oficial:** [dadosabertos.aneel.gov.br](https://dadosabertos.aneel.gov.br/dataset/qualidade-do-servico-distribuicao)
- **Licença de Uso:** Política de Dados Abertos do Poder Executivo Federal (Decreto nº 8.777/2016)
- **Data da Coleta / Competência:** Série histórica de 2020 a 2024
- **Formato na Ingestão:** JSON via API REST CKAN Datastore
- **Recurso Principal (DEC/FEC):** `4493985c-baea-429c-9df5-3030422c71d7`
- **Recurso de Mapeamento (indqual-municipio):** `3f841488-80a8-42f2-a6ca-e0c593b228de`
- **Chave de Cruzamento Primária:** `IdeConjUndConsumidoras` (Bronze) $\rightarrow$ `codigo_municipio` (Silver/Gold, Código IBGE de 7 dígitos)

### 1.1. Dicionário da Tabela Silver ANEEL (`data/silver/aneel/aneel_silver.parquet`)

| Coluna | Tipo | Descrição | Domínio / Faixa Válida |
| :--- | :--- | :--- | :--- |
| `codigo_municipio` | `VARCHAR(7)` | Código numérico IBGE do município (chave relacional) | Ex: `1501402` (Belém/PA) |
| `NomMunicipio` | `VARCHAR` | Nome oficial do município em maiúsculas | Ex: `BELÉM`, `SANTARÉM` |
| `ano` | `INT` | Ano da competência do indicador | `2020` a `2024` |
| `mes` | `INT` | Mês da competência do indicador | `1` a `12` |
| `dec_horas_mensal` | `FLOAT` | **Duração Equivalente de Interrupção**: média municipal de horas sem energia | $\ge 0.0$ e $\le 744.0\text{ h}$ |
| `dec_horas_max` | `FLOAT` | **Pior Caso do Município**: maior duração observada no conjunto elétrico mais crítico | $\ge 0.0$ e $\le 744.0\text{ h}$ |
| `fec_freq_mensal` | `FLOAT` | **Frequência Equivalente de Interrupção**: média municipal de vezes que a luz caiu | $\ge 0.0$ interrupções |
| `fec_freq_max` | `FLOAT` | **Pior Caso de Quedas**: maior frequência de quedas registrada entre os conjuntos | $\ge 0.0$ interrupções |
| `qtd_conjuntos` | `INT` | Quantidade de conjuntos elétricos (subestações) atendendo o município | $\ge 1$ subestações |
| `_silver_processed_at` | `TIMESTAMP` | Data/hora UTC em que a regra de transformação foi aplicada na Silver | Formato ISO-8601 UTC |
| `_silver_rules_version`| `VARCHAR` | Versão das regras e contratos de dados aplicados | Ex: `2.2.0` |
| `_validation_status` | `VARCHAR` | Confirmação de conformidade com contratos de dados | `PASSED_CONTRACTS` |

---

## 2. Base 2: Microdados do Exame Nacional do Ensino Médio — ENEM (INEP)

- **Órgão Responsável:** Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP / MEC)
- **Origem / URL Oficial:** [gov.br/inep/microdados](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem)
- **Licença de Uso:** Dados Abertos Governamentais / Domínio Público Federal
- **Data da Coleta / Competência:** Provas anuais de 2020 a 2024
- **Formato na Ingestão:** Arquivo CSV com separador `;`, processado em lotes (*chunks*)
- **Tratamento de Privacidade (LGPD):** O identificador individual do candidato (`NU_INSCRICAO`) é expurgado na transição Bronze $\rightarrow$ Silver. Nenhuma informação de identificação pessoal avança no pipeline.
- **Chave de Cruzamento Primária:** `CO_MUNICIPIO_PROVA` $\rightarrow$ `codigo_municipio` (Código IBGE de 7 dígitos)

### 2.1. Dicionário da Tabela Silver ENEM (`data/silver/enem/enem_silver.parquet`)

| Coluna | Tipo | Descrição | Domínio / Faixa Válida |
| :--- | :--- | :--- | :--- |
| `codigo_municipio` | `VARCHAR(7)` | Código IBGE do município polo de aplicação da prova | Ex: `1501402` (Belém) |
| `ano` | `INT` | Ano de realização do exame nacional | `2020` a `2024` |
| `total_inscritos` | `INT` | Total de estudantes inscritos no município naquele ano | $\ge 1$ candidatos |
| `total_presentes` | `INT` | Total de estudantes que compareceram aos dois dias de prova | $\ge 0$ candidatos |
| `total_abstencao` | `INT` | Total de candidatos que faltaram a pelo menos um dia de aplicação | $\ge 0$ candidatos |
| `taxa_abstencao` | `FLOAT` | Proporção de abstenção (`total_abstencao / total_inscritos`) | $0.0000$ a $1.0000$ ($0\%$ a $100\%$) |
| `taxa_presenca` | `FLOAT` | Proporção de presença (`total_presentes / total_inscritos`) | $0.0000$ a $1.0000$ ($0\%$ a $100\%$) |
| `media_nota_cn` | `FLOAT` | Média da nota municipal em Ciências da Natureza | $0.0$ a $1000.0$ |
| `media_nota_ch` | `FLOAT` | Média da nota municipal em Ciências Humanas | $0.0$ a $1000.0$ |
| `media_nota_lc` | `FLOAT` | Média da nota municipal em Linguagens e Códigos | $0.0$ a $1000.0$ |
| `media_nota_mt` | `FLOAT` | Média da nota municipal em Matemática | $0.0$ a $1000.0$ |
| `media_nota_redacao`| `FLOAT` | Média da nota municipal na prova de Redação | $0.0$ a $1000.0$ |
| `media_nota_geral` | `FLOAT` | Média aritmética consolidada das 5 áreas avaliadas no ENEM | $0.0$ a $1000.0$ |
| `_silver_processed_at` | `TIMESTAMP` | Data/hora UTC do processamento e agregação na Silver | Formato ISO-8601 UTC |
| `_silver_rules_version`| `VARCHAR` | Versão das regras e contratos de dados aplicados | Ex: `2.2.0` |
| `_validation_status` | `VARCHAR` | Confirmação de conformidade com contratos de dados | `PASSED_CONTRACTS` |

---

## 3. Tabela Silver Integrada — Silver Joined (`data/silver/joined/silver_joined.parquet`)

Resultado do Inner Join relacional entre as duas bases auditadas pela chave composta `(codigo_municipio, ano)`. Contém a série temporal mensal completa de energia atrelada aos indicadores anuais do ENEM para os 144 municípios do Pará (totalizando 8.542 registros mensais).

- **Chave Composta:** `codigo_municipio` + `ano` + `mes`
- **Granularidade:** 1 linha por Município / Ano / Mês
- **Cobertura Relacional:** 144 municípios casados (100% de integridade referencial, 0 órfãos).

---

## 4. Base ML-Ready Orientada à Decisão (Camada Gold)

- **Arquivo:** `data/gold/ml_ready/enem_aneel_ml_ready.parquet`
- **Nível de Granularidade:** 1 linha por Município / Ano de aplicação do ENEM (720 linhas = 144 municípios $\times$ 5 anos).
- **Ponto de Corte ($t_0$):** 31 de Julho do ano do exame.
- **Janela de Observação (Features):** Janeiro a Julho ($< t_0$).
- **Janela de Predição (Target):** Novembro ($> t_0$).

| Feature / Target | Tipo | Origem | Descrição e Papel no Modelo |
| :--- | :--- | :--- | :--- |
| `codigo_municipio` | `VARCHAR(7)` | Silver Joined | Chave primária identificadora do município no Pará |
| `NomMunicipio` | `VARCHAR` | Silver Joined | Nome oficial da cidade para exibição em relatórios e mapas |
| `ano` | `INT` | Silver Joined | Ano de referência para o Split Estritamente Temporal |
| `fec_medio_jan_jul` | `FLOAT` | ANEEL (Jan-Jul) | **Feature:** Média mensal de quedas de energia no período preparatório |
| `dec_horas_total_jan_jul` | `FLOAT` | ANEEL (Jan-Jul) | **Feature:** Total de horas no escuro acumuladas no período preparatório |
| `dec_horas_medio_jan_jul` | `FLOAT` | ANEEL (Jan-Jul) | **Feature:** Média mensal de horas sem energia |
| `meses_observados` | `INT` | ANEEL (Jan-Jul) | **Auditoria:** Número de meses observados no período (esperado: 7) |
| `total_inscritos` | `INT` | ENEM | Histórico de contingente estudantil do município polo |
| `total_abstencao` | `INT` | ENEM | Total de faltosos no exame de Novembro |
| `taxa_abstencao` | `FLOAT` | ENEM | Taxa real de abstenção observada em Novembro |
| **`target_abstencao_critica`**| `INT` | ENEM (Novembro) | **Target Binário:** `1` se abstenção municipal $> 35\%$; `0` caso contrário |

---

## 5. Dicionário da Tabela de Quarentena (`data/quarantine/`)

Registros que violam contratos de schema, limites físicos ou integridade relacional são isolados nos diretórios `data/quarantine/aneel/` e `data/quarantine/enem/` com os seguintes metadados de auditoria:

| Metadado de Quarentena | Tipo | Significado | Exemplo |
| :--- | :--- | :--- | :--- |
| `_quarantine_timestamp` | `TIMESTAMP` | Data e hora UTC exata do evento de rejeição | `2026-09-17T18:48:23.102Z` |
| `_quarantine_layer` | `VARCHAR` | Camada em que a violação foi interceptada | `SILVER` |
| `_quarantine_source` | `VARCHAR` | Fonte dos dados problemáticos | `ANEEL` ou `ENEM` |
| `_rejection_reason` | `VARCHAR` | Código canônico do motivo do descarte | `DEC_MENSAL_EXCEDE_HORAS_TOTAIS_DO_MES` |
| `_rejection_value` | `VARCHAR` | Valor específico da coluna que causou a violação | `999.0` ou `-15.2` |
