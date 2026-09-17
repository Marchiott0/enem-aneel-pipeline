# Arquitetura do Pipeline de Dados e Decisão
*Projeto Integrador: Da Ingestão à Decisão (ENEM + ANEEL) | Versão: 2.2.0*

---

## 1. Visão Geral do Fluxo (Medallion Architecture)

```
[ INEP (CSV em Chunks) ] ───► Ingestão Bronze (Hash + Metadados) ───► data/bronze/enem/
                                                                           │
[ ANEEL (API REST CKAN) ] ──► Ingestão Bronze (Retry + Hash)    ───► data/bronze/aneel/
                                                                           │
                                                     ┌─────────────────────┴─────────────────────┐
                                                     │         CAMADA SILVER (821 LINHAS)        │
                                                     │  1. Contratos de Dados (Data Contracts)   │
                                                     │  2. Sistema Ativo de Quarentena           │
                                                     │  3. Modelagem Relacional N:M ANEEL (IBGE) │
                                                     │  4. Anonimização LGPD & Agregação ENEM    │
                                                     │  5. Auditoria de JOIN (144 Municípios)    │
                                                     │  6. Relatório de Data Quality Profiling   │
                                                     └─────────────────────┬─────────────────────┘
                                                                           ▼
                                                                  data/silver/joined/
                                                                           │
                                                     ┌─────────────────────┴─────────────────────┐
                                                     │        CAMADA GOLD (DATASET ML-READY)     │
                                                     │  - Congelamento no Corte Temporal t0      │
                                                     │  - Janela de Observação (Jan a Jul < t0)  │
                                                     │  - Janela de Predição (Nov > t0)          │
                                                     │  - Definição do Target de Decisão         │
                                                     └─────────────────────┬─────────────────────┘
                                                                           ▼
                                                                  data/gold/ml_ready/
                                                                           │
                                                     ┌─────────────────────┴─────────────────────┐
                                                     │      MODELO PREDITIVO & MATRIZ DE CUSTO   │
                                                     │  - Split Estritamente Temporal            │
                                                     │  - Random Forest vs. Dummy Baseline       │
                                                     │  - Matriz Financeira (SEDUC-PA / MEC)     │
                                                     └───────────────────────────────────────────┘
```

---

## 2. Aprofundamento: A Camada Silver (O Coração da Engenharia de Dados)

A Camada Silver do projeto foi concebida para atender estritamente ao **Requisito 3** das diretrizes da disciplina (*"Tipagem forte, padronização de nomenclatura, chave explícita tratada, validações com quarentena, integridade relacional do join"*).

### 2.1. Contratos de Dados (Data Contracts) & Limites Físicos
- **Validação ANEEL:**
  - Código do ano entre `2018` e `2030`;
  - Mês de competência entre `1` e `12`;
  - Conversão de string com vírgula para ponto (`float`);
  - Limite físico: $0.0 \le \text{DEC Mensal} \le 744.0\text{ h}$ (total de horas de um mês de 31 dias). Valores negativos ou $> 744\text{ h}$ são anomalias rejeitadas.
- **Validação ENEM:**
  - Código IBGE do município com exatamente 7 dígitos numéricos;
  - Códigos de presença restritos a $\{0, 1, 2\}$ (conforme dicionário oficial do INEP);
  - Notas das 5 disciplinas estritamente dentro da escala TRI $[0.0, 1000.0]$.

### 2.2. Sistema de Quarentena e Rastreabilidade
- Registros que violam os contratos não interrompem o processamento do lote e não são corrigidos silenciosamente.
- São isolados em arquivos Parquet com:
  - `_quarantine_timestamp` (carimbo temporal UTC);
  - `_quarantine_layer` (`SILVER`);
  - `_rejection_reason` (motivo formal do descarte);
  - `_rejection_value` (o valor original defeituoso).

### 2.3. Modelagem Relacional N:M da ANEEL (Conjuntos Elétricos $\leftrightarrow$ Municípios)
- Na ANEEL, os dados de qualidade são medidos por **Conjunto de Unidades Consumidoras** (subestações), e não na divisão político-administrativa dos municípios.
- Um município polo (ex: Belém, Santarém) possui múltiplos conjuntos elétricos.
- A Camada Silver resolve essa relação N:M cruzando a tabela `indqual-municipio` com os indicadores, consolidando:
  - `dec_horas_mensal`: média municipal de horas no escuro;
  - `dec_horas_max`: o pior conjunto elétrico do município (estresse máximo da rede);
  - `fec_freq_mensal`: média de quedas de energia;
  - `fec_freq_max`: maior pico de interrupções registrado;
  - `qtd_conjuntos`: quantidade de subestações atendendo a cidade.

### 2.4. Conformidade com a LGPD e Agregação do ENEM
- O identificador individual `NU_INSCRICAO` é eliminado logo na entrada da Silver.
- Os dados individuais dos candidatos são agregados em nível municipal por ano, gerando:
  - `total_inscritos`, `total_presentes`, `total_abstencao`;
  - `taxa_abstencao` ($= \text{total\_abstencao} / \text{total\_inscritos}$);
  - Médias municipais de desempenho nas 5 áreas avaliadas.

### 2.5. Auditoria do JOIN Relacional
- Cruzamento estrito via `codigo_municipio` (7 dígitos) e `ano`.
- **Taxa de Cobertura Obtida:** **100.00%** (144 de 144 municípios casados; 0 órfãos em ambas as bases).
- Emissão automática de relatórios de auditoria em Markdown (`relatorio_join_enem_aneel.md`) e JSON estruturado (`metricas_join.json`).

---

## 3. Salvaguarda Anti-Vazamento (Anti-Leakage na Gold)
- Ponto de corte temporal fixado estritamente em **$t_0 = 31/\text{Julho}$**.
- As features de energia elétrica utilizam exclusivamente medições do intervalo $[01/\text{Janeiro}, 31/\text{Julho}]$ ($< t_0$).
- O target de abstenção crítica ($> 35\%$) é apurado na aplicação da prova em Novembro ($> t_0$).
- Split Estritamente Temporal: Treinamento com anos de 2020 a 2023 e teste no ano futuro de 2024.
