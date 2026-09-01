# Dicionário de Dados — Projeto Integrador

Este documento descreve detalhadamente as bases utilizadas, suas origens, formatos, colunas, tipos de dados e domínios de valores.

---

## 1. Base 1: Qualidade de Energia (ANEEL)
- **Órgão Responsável:** Agência Nacional de Energia Elétrica (ANEEL)
- **Formato:** JSON (API REST - CKAN Datastore)
- **URL Base:** `https://dadosabertos.aneel.gov.br/api/3/action/datastore_search`
- **Resource ID:** `74100dc8-a832-4752-95f3-cdd09d4eb4af`
- **Frequência de Atualização:** Mensal
- **Chave de Cruzamento:** `IdeCodigoMunicipio` (Código IBGE de 7 dígitos)

### Colunas Principais (Camada Silver / Gold)

| Coluna | Tipo | Descrição | Domínio / Exemplo |
| :--- | :--- | :--- | :--- |
| `id_municipio_ibge` | `VARCHAR(7)` | Código IBGE do município | Ex: `1501402` (Belém/PA) |
| `ano` | `INT` | Ano da competência | Ex: `2023`, `2024` |
| `mes` | `INT` | Mês da competência | `1` a `12` |
| `sig_distribuidora` | `VARCHAR` | Sigla da distribuidora de energia | Ex: `EQUATORIAL PA` |
| `num_fec` | `FLOAT` | Frequência Equivalente de Interrupção | $\ge 0.0$ |
| `num_dec` | `FLOAT` | Duração Equivalente de Interrupção (horas) | $\ge 0.0$ |
| `_ingestion_time` | `TIMESTAMP` | Data/hora UTC da ingestão na Bronze | ISO-8601 |
| `_source` | `VARCHAR` | Identificador da fonte de dados | `API_ANEEL_CKAN` |
| `_load_id` | `VARCHAR` | UUID único da execução da carga | UUID v4 |
| `_record_hash` | `VARCHAR(64)` | Hash SHA-256 para idempotência | Hexadecimal 64 chars |

---

## 2. Base 2: Microdados do ENEM (INEP)
- **Órgão Responsável:** Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)
- **Formato:** CSV (Arquivo com delimitador `;` ou `|`)
- **Encoding:** `ISO-8859-1` ou `UTF-8`
- **URL Base:** `https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem`
- **Chave de Cruzamento:** `CO_MUNICIPIO_PROVA` / `CO_MUNICIPIO_ESC` (Código IBGE de 7 dígitos)

### Colunas Principais (Camada Silver / Gold)

| Coluna | Tipo | Descrição | Domínio / Exemplo |
| :--- | :--- | :--- | :--- |
| `NU_INSCRICAO` | `VARCHAR` | Identificador único do participante (anonimizado) | Numérico |
| `CO_MUNICIPIO_PROVA` | `VARCHAR(7)` | Código IBGE do município de realização da prova | Ex: `1501402` |
| `NO_MUNICIPIO_PROVA` | `VARCHAR` | Nome do município de realização da prova | Ex: `Belém` |
| `SG_UF_PROVA` | `VARCHAR(2)` | Sigla do Estado | Ex: `PA` |
| `TP_PRESENCA_CN` | `INT` | Presença em Ciências da Natureza | `0` = Faltou, `1` = Presente, `2` = Eliminado |
| `TP_PRESENCA_CH` | `INT` | Presença em Ciências Humanas | `0` = Faltou, `1` = Presente, `2` = Eliminado |
| `TP_PRESENCA_LC` | `INT` | Presença em Linguagens e Códigos | `0` = Faltou, `1` = Presente, `2` = Eliminado |
| `TP_PRESENCA_MT` | `INT` | Presença em Matemática | `0` = Faltou, `1` = Presente, `2` = Eliminado |
| `NU_NOTA_CN` | `FLOAT` | Nota em Ciências da Natureza | `0.0` a `1000.0` |
| `NU_NOTA_CH` | `FLOAT` | Nota em Ciências Humanas | `0.0` a `1000.0` |
| `NU_NOTA_LC` | `FLOAT` | Nota em Linguagens e Códigos | `0.0` a `1000.0` |
| `NU_NOTA_MT` | `FLOAT` | Nota em Matemática | `0.0` a `1000.0` |
| `NU_NOTA_REDACAO` | `FLOAT` | Nota da Redação | `0.0` a `1000.0` |

---

## 3. Base ML-Ready (Camada Gold)
- **Nível de Granularidade:** 1 linha por Município / Ano de aplicação do ENEM.
- **Ponto de Corte ($t_0$):** 31 de Julho do ano do exame.
- **Janela de Observação (Features):** Janeiro a Julho ($< t_0$).
- **Janela de Predição (Target):** Novembro ($> t_0$).

| Feature / Target | Tipo | Origem | Descrição |
| :--- | :--- | :--- | :--- |
| `id_municipio_ibge` | `VARCHAR(7)` | ENEM & ANEEL | Código identificador do município |
| `ano_enem` | `INT` | ENEM | Ano da prova do ENEM |
| `fec_medio_jan_jul` | `FLOAT` | ANEEL (Jan-Jul) | Média mensal de interrupções no período preparatório |
| `fec_max_jan_jul` | `FLOAT` | ANEEL (Jan-Jul) | Pico mensal de interrupções no período preparatório |
| `dec_horas_total_jan_jul` | `FLOAT` | ANEEL (Jan-Jul) | Horas acumuladas sem energia (Jan-Jul) |
| `dec_horas_medio_jan_jul` | `FLOAT` | ANEEL (Jan-Jul) | Média mensal de duração de quedas |
| `taxa_inscritos_ano_anterior` | `FLOAT` | ENEM | Histórico de inscritos por habitante |
| **`target_abstencao_critica`** | `INT` | ENEM (Novembro) | **Target Binário:** `1` se abstenção $> 30\%$, `0` caso contrário |
