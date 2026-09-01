# Projeto Integrador: Da Ingestão à Decisão (ENEM + ANEEL)

Projeto desenvolvido para a disciplina de **Engenharia de Dados e Machine Learning**, estruturando um pipeline ponta a ponta que conecta a ingestão de bases públicas reais à recomendação orientada à tomada de decisão.

---

## 1. A Pergunta de Negócio e Decisão

> *"Cruzando as bases do **ENEM (INEP)** e **Qualidade de Energia (ANEEL)**, identificamos que municípios com alta instabilidade elétrica nos meses de preparação sofrem impacto drástico na abstenção e nas notas. Recomendamos que a **Secretaria de Educação do Estado do Pará (SEDUC-PA)** e o **MEC** façam a **alocação de geradores móveis e distribuição de planos de estudo offline** nos próximos **3 meses (agosto a outubro)**, priorizando os **50 municípios** com maior probabilidade predita de falha estrutural na infraestrutura elétrica. Se agir, o ganho esperado é **garantir acesso equitativo e evitar a evasão de até milhares de candidatos**; se errarmos, o custo é referente à **logística e locação de geradores móveis para polos de prova**."*

---

## 2. Arquitetura do Repositório

```plaintext
isaacprofessor/
├── .gitignore                     # Proteção rigorosa (não comita data/)
├── .env.example                   # Variáveis de ambiente configuráveis
├── README.md                      # Documentação central do projeto
├── requirements.txt               # Dependências com versões fixadas
├── docs/                          # Especificações detalhadas
│   ├── dicionario_dados.md        # Dicionário completo de features e tipos
│   └── arquitetura_pipeline.md    # Diagramas e princípios arquiteturais
├── notebooks/                     # Notebooks de exploração e prototipagem
│   └── 01_exploracao_bronze.ipynb
├── data/                          # Armazenamento local de dados (Ignorado no Git)
│   ├── bronze/                    # Dados brutos + metadados técnicos
│   │   ├── aneel/
│   │   └── enem/
│   ├── silver/                    # Dados tipados, limpos e join auditado
│   │   ├── aneel/
│   │   ├── enem/
│   │   └── joined/
│   ├── gold/                      # Agregações e dataset ML-Ready final
│   │   ├── analytics/
│   │   └── ml_ready/
│   └── quarantine/                # Registros rejeitados ou com erro
│       ├── aneel/
│       └── enem/
└── src/                           # Código-fonte modular
    ├── __init__.py
    ├── config.py                  # Caminhos, constantes e sementes
    ├── ingestion/                 # Módulos de extração e ingestão
    │   ├── __init__.py
    │   ├── ingest_aneel_api.py    # Ingestão JSON via REST API (ANEEL CKAN)
    │   └── ingest_enem_csv.py     # Ingestão CSV em chunks (INEP ENEM)
    ├── processing/                # Pipeline de transformação Medalhão
    │   ├── __init__.py
    │   ├── bronze_to_silver.py    # Tratamento, contratos e integridade de join
    │   └── silver_to_gold.py      # Feature engineering anti-leakage e corte t0
    ├── model/                     # Modelagem preditiva e tomada de decisão
    │   ├── __init__.py
    │   ├── train_predictor.py     # Treinamento com split temporal
    │   └── evaluate.py            # Avaliação de métricas e matriz de custos
    └── utils/
        ├── __init__.py
        └── logger.py              # Log padronizado com rastreabilidade
```

---

## 3. Como Configurar e Executar

### 3.1. Pré-requisitos
- Python 3.10+
- Git

### 3.2. Instalação do Ambiente

```bash
# 1. Criar e ativar o ambiente virtual
python -m venv venv

# Windows:
.\venv\Scripts\activate
# Linux/Mac:
# source venv/bin/activate

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar variáveis de ambiente
cp .env.example .env
```

### 3.3. Execução Sequencial do Pipeline

1. **Ingestão da Camada Bronze:**
   ```bash
   python src/ingestion/ingest_aneel_api.py
   python src/ingestion/ingest_enem_csv.py
   ```

2. **Processamento Bronze $\rightarrow$ Silver:**
   ```bash
   python src/processing/bronze_to_silver.py
   ```

3. **Processamento Silver $\rightarrow$ Gold (ML-Ready):**
   ```bash
   python src/processing/silver_to_gold.py
   ```

4. **Treinamento e Avaliação da Decisão:**
   ```bash
   python src/model/train_predictor.py
   python src/model/evaluate.py
   ```

---

## 4. Salvaguardas Anti-Leakage (ML-Ready)
- **Ponto de Corte ($t_0$):** 31 de Julho.
- **Janela de Features:** Apenas eventos ocorridos entre Janeiro e Julho ($< t_0$).
- **Janela de Target:** Abstenção observada em Novembro ($> t_0$).
- **Split Temporal:** Validação em anos futuros para evitar vazamento temporal e overfitting de tendência.

---

## 5. Uso de IA, Integridade e Privacidade de Dados (Requisito 9)

### 5.1. Declaração de Uso de IA Generativa
Em estrita conformidade com a **Cláusula 9 das Diretrizes da Disciplina**, declaramos os pontos em que ferramentas de IA generativa foram utilizadas como suporte ao desenvolvimento:

| Componente / Módulo | Finalidade do Uso de IA | Descrição do Auxílio Prestado |
| :--- | :--- | :--- |
| **Arquitetura & Configuração** | Estruturação de Projeto | Apoio na modelagem de pastas do pipeline Medalhão, arquivos `.gitignore`, `requirements.txt` e `.env.example`. |
| **Documentação Técnica** | Padronização e Especificação | Estruturação e formatação em Markdown dos arquivos `README.md`, `dicionario_dados.md`, `arquitetura_pipeline.md` e `diretrizes_disciplina.md`. |
| **`src/ingestion/ingest_aneel_api.py`** | Ingestão e Resiliência (API ANEEL) | Apoio na estruturação da função de requisição com *retry* e *backoff exponencial* via `tenacity`, paginação controlada, geração de hash SHA-256 para idempotência, injeção de metadados técnicos de auditoria e tratamento de quarentena. |
| **`src/config.py` & Scripts Modulares** | Configuração e Execução | Apoio na definição centralizada de caminhos e parâmetros, além da configuração do bootstrap de importação (`sys.path`) para permitir execução direta via terminal. |

> **Declaração de Domínio Técnico:** Todo código, pipeline, arquitetura e cálculos implementados com auxílio de ferramentas de IA foram integralmente revisados, testados e validados pelo grupo. Todos os integrantes dominam tecnicamente os componentes do projeto e estão plenamente aptos a justificar qualquer linha de código durante a arguição oral.

### 5.2. Privacidade e Conformidade (LGPD)
- **Tratamento de Dados Pessoais:** O pipeline consome apenas dados públicos e abertos.
- **Anonimização e Agregação:** Qualquer identificador individual presente no arquivo bruto do ENEM (`NU_INSCRICAO`) é expurgado logo na transição da camada Bronze para a **Silver**, onde os dados são obrigatoriamente agregados em nível municipal (`id_municipio_ibge`). Nenhuma informação identificável de participantes chega à camada Gold ou aos modelos preditivos.

### 5.3. Catálogo de Fontes, Licenças e Coleta

| Base de Dados | Órgão / Instituição | Formato | Licença | URL de Acesso | Data de Coleta |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Microdados do ENEM** | INEP / MEC | CSV (Arquivo) | Aberta / Domínio Público Federal | [gov.br/inep](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem) | 26/08/2026 |
| **Indicadores DEC/FEC** | ANEEL | JSON (API REST CKAN) | Política de Dados Abertos do Poder Executivo Federal | [dadosabertos.aneel.gov.br](https://dadosabertos.aneel.gov.br/api/3/action/datastore_search) | 26/08/2026 |



