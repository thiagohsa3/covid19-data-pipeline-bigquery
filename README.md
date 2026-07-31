# Projeto: Pipeline de Dados COVID-19 com Google BigQuery & Python

## 📌 Visão Geral do Projeto

- **Problema abordado**: A grande quantidade de microdados brutos de saúde em nuvem exige processamento otimizado, transformação consistente e testes rigorosos de sanidade antes de alimentar dashboards executivos e relatórios estratégicos.

- **Objetivo**: Construir um pipeline autônomo e auditável de ponta a ponta (ETL), extraindo dados da COVID-19 diretamente do Google BigQuery, aplicando regras de Data Quality (PASS/FAIL) e gerando entregáveis consolidados em múltiplos formatos.

- **Metodologia**: Conexão autônoma via API com Google Cloud Platform, consultas otimizadas em BigQuery SQL, testes automatizados de qualidade de dados (Data Quality Checks), exportação multiformato (CSV, Parquet, PNG) e rastreamento de saúde da aplicação via módulo de `logging`.

---

## 🎯 Etapas do Projeto

### 1. Fontes de Dados
| Fonte | Tipo | Método de Coleta | Link |
|-------|------|------------------|------|
| Google BigQuery Public Datasets (COVID-19) | Banco de Dados em Nuvem (SQL) | API Oficial (`google-cloud-bigquery`) | [Google Cloud Console](https://console.cloud.google.com/bigquery) |

- **Registros Extraídos**: Top 20 países agregados e 350 registros diários da série temporal do Brasil.
- **Variáveis Trabalhadas**: `country_region`, `total_confirmed`, `total_deaths`, `date`, `confirmed`.

### 2. Pipeline de Engenharia & Qualidade (ETL)
- **Script Python**:
  - [`pipeline_covid_bigquery.py`](scripts/pipeline_covid_bigquery.py) - Script principal responsável pela conexão, execução das consultas SQL, testes de sanidade, geração de gráficos e salvamento dos artefatos.
      * *Gerenciamento de Observabilidade:* Este script conta com uma rotina automatizada de logging profissional. A cada execução, todos os passos, metadados das tabelas e resultados dos testes de qualidade são registrados e acumulados em tempo real no arquivo [`pipeline_execution.log`](output/logs/pipeline_execution.log).

**Bibliotecas utilizadas**:
- Google Cloud BigQuery (Extração e integração com a nuvem GCP)
- Pandas, PyArrow (ETL, estruturação de dados e persistência colunar Parquet)
- Matplotlib (Geração automatizada de gráficos analíticos)
- Logging (Observabilidade, governança de dados e relatórios de auditoria)

- **Testes de Qualidade de Dados (Data Quality Checks)**:
  - 📊 **Ranking Top 20 Países**:
    - ✅ Volume exato de linhas (`PASS` - Garante o corte perfeito nos 20 maiores).
    - ✅ Integridade de casos (`PASS` - Zero contagens negativas de confirmados).
    - ✅ Integridade de óbitos (`PASS` - Zero contagens negativas de mortes).
    - ✅ Unicidade (`PASS` - Ausência de países duplicados no ranking).
  - 📈 **Série Temporal (Brasil)**:
    - ✅ Validação de sanidade (`PASS` - Sem contagens diárias negativas).
    - ✅ Ordenação cronológica (`PASS` - Datas dispostas do início ao fim sem inversões).
    - ✅ Unicidade temporal (`PASS` - Ausência de datas duplicadas no registro).

### 3. Principais Entregáveis & Resultados
- **Gráfico Temporal**: Visualização em formato PNG salva automaticamente em `output/images/grafico_covid_brasil.png`.
- **Bases Tratadas**:
  - `output/csv/covid_agg_top20.csv` - Tabela limpa e pronta para uso imediato em ferramentas de BI.
  - `output/parquet/covid_timeseries_brazil.parquet` - Arquivo colunar otimizado para alta performance e baixo consumo de memória.
- **Logs de Execução Auditados**:
  - [`pipeline_execution.log`](output/logs/pipeline_execution.log) - Registro completo de auditoria do sistema.

---

## 🛠️ Tecnologias Utilizadas

| Ferramenta | Finalidade |
|------------|------------|
| Google BigQuery / SQL | Engine de Data Warehouse na nuvem e consultas agregadas |
| Python 3.13 | Linguagem base para automação do Data Pipeline |
| Pandas & PyArrow | Manipulação de dados e exportação em CSV e Parquet |
| Matplotlib | Visualização gráfica automatizada da série temporal |
| Logging Module | Auditoria técnica, monitoramento e rastreabilidade (PASS) |

---

## 📂 Estrutura do Repositório

```text
.
├── output/
│   ├── csv/
│   │   └── covid_agg_top20.csv
│   ├── images/
│   │   └── grafico_covid_brasil.png
│   ├── logs/
│   │   └── pipeline_execution.log
│   └── parquet/
│       └── covid_timeseries_brazil.parquet
│
├── scripts/
│   └── pipeline_covid_bigquery.py
│
└── README.md
