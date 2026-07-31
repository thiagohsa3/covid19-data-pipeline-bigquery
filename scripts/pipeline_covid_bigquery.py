"""
===============================================================================
PIPELINE DE EXTRAÇÃO, TRATAMENTO E VALIDAÇÃO DE DADOS COVID-19 (BIGQUERY)
===============================================================================

Descrição:
    Script automatizado para consulta, tratamento, validação de qualidade (DQ)
    e exportação de dados da COVID-19 via Google BigQuery API.

-------------------------------------------------------------------------------
1. CONFIGURAÇÃO DE CREDENCIAIS E PERMISSÕES DO GOOGLE CLOUD PLATFORM (GCP):
-------------------------------------------------------------------------------
    - Permissões Exigidas no IAM do GCP:
        * BigQuery Data Viewer (roles/bigquery.dataViewer)
        * BigQuery Job User (roles/bigquery.jobUser)
    
    - Autenticação de Ambiente Local (Terminal):
        Execute o comando abaixo no terminal antes de rodar o script:
        $ gcloud auth application-default login

-------------------------------------------------------------------------------
2. INSTALAÇÃO DAS DEPENDÊNCIAS:
-------------------------------------------------------------------------------
    Execute no terminal/PowerShell:
    $ pip install google-cloud-bigquery pandas db-dtypes pyarrow matplotlib
===============================================================================
"""

import os
import sys
import logging
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery

# =============================================================================
# SEÇÃO DE CONFIGURAÇÃO (PARÂMETROS DO PIPELINE)
# =============================================================================
PROJECT_ID = "project-95f87894-d8fa-4e49-828"
DATASET_TABLE = "project-95f87894-d8fa-4e49-828.bd_covid19.covid_19_geographic_distribution_worldwide"

# Parâmetros de Análise
COUNTRY_TARGET = "Brazil"
TOP_LIMIT = 20

# Nomes dos Arquivos de Saída
FILE_CSV_NAME = "covid_agg_top20.csv"
FILE_PARQUET_NAME = "covid_timeseries_brazil.parquet"
FILE_CHART_NAME = "grafico_covid_brasil.png"
FILE_LOG_NAME = "pipeline_execution.log"

# Mapeamento e Criação dos Diretórios de Saída
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_CSV = os.path.join(BASE_DIR, "output", "csv")
DIR_PARQUET = os.path.join(BASE_DIR, "output", "parquet")
DIR_IMAGES = os.path.join(BASE_DIR, "output", "images")
DIR_LOGS = os.path.join(BASE_DIR, "output", "logs")

for directory in [DIR_CSV, DIR_PARQUET, DIR_IMAGES, DIR_LOGS]:
    os.makedirs(directory, exist_ok=True)

# =============================================================================
# CONFIGURAÇÃO DE LOGGING (TERMINAL + ARQUIVO .LOG)
# =============================================================================
log_filepath = os.path.join(DIR_LOGS, FILE_LOG_NAME)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | [%(levelname)s] | %(message)s",
    handlers=[
        logging.FileHandler(log_filepath, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info("=" * 60)
logging.info("INICIANDO EXECUTOR DO PIPELINE COVID-19 (BIGQUERY)")
logging.info("=" * 60)

# =============================================================================
# 1. INICIALIZAÇÃO DO CLIENTE BIGQUERY
# =============================================================================
try:
    logging.info(f"Conectando ao Google BigQuery (Project ID: {PROJECT_ID})...")
    client = bigquery.Client(project=PROJECT_ID)
    logging.info("✅ Conexão estabelecida com sucesso!")
except Exception as e:
    logging.error(f"❌ Falha ao conectar ao BigQuery: {e}")
    raise SystemExit(e)

# =============================================================================
# 2. QUERY 2 — TOP 20 PAÍSES POR CASOS CONFIRMADOS
# =============================================================================
logging.info(f"Executando Query: Top {TOP_LIMIT} Países por Casos Confirmados...")
sql_agg = f"""
SELECT
    countries_and_territories AS country_region,
    SUM(CAST(daily_confirmed_cases AS INT64)) AS total_confirmed,
    SUM(CAST(daily_deaths AS INT64)) AS total_deaths
FROM `{DATASET_TABLE}`
GROUP BY country_region
ORDER BY total_confirmed DESC
LIMIT {TOP_LIMIT}
"""

df_agg = client.query(sql_agg).to_dataframe()

# Tratamento de Tipos e Limpeza
df_agg['total_confirmed'] = pd.to_numeric(df_agg['total_confirmed'], errors='coerce').fillna(0).astype('int64')
df_agg['total_deaths'] = pd.to_numeric(df_agg['total_deaths'], errors='coerce').fillna(0).astype('int64')

# Registrando Informações da Base no Log
logging.info("=== METADADOS DA BASE AGG (TOP 20 PAÍSES) ===")
logging.info(f"Total de Linhas Extradas: {len(df_agg)}")
logging.info(f"Tipos de Colunas:\n{df_agg.dtypes.to_string()}")
logging.info(f"Quantidade de Nulos por Coluna:\n{df_agg.isna().sum().to_string()}")

# -----------------------------------------------------------------------------
# TESTES DE QUALIDADE DE DADOS (DATA QUALITY CHECKS) - AGG
# -----------------------------------------------------------------------------
logging.info("--- Executando Testes de Qualidade (Ranking Top 20) ---")

# Teste 1: Quantidade de países
assert len(df_agg) == TOP_LIMIT, f"Erro: Esperado {TOP_LIMIT} registros, retornado {len(df_agg)}"
logging.info(f"✅ [PASS] O ranking contém exatamente {TOP_LIMIT} países.")

# Teste 2: Valores negativos em casos
assert (df_agg['total_confirmed'] >= 0).all(), "Erro: Existem valores negativos em total_confirmed!"
logging.info("✅ [PASS] Não existem casos confirmados negativos.")

# Teste 3: Valores negativos em óbitos
assert (df_agg['total_deaths'] >= 0).all(), "Erro: Existem valores negativos em total_deaths!"
logging.info("✅ [PASS] Não existem óbitos negativos.")

# Teste 4: Duplicatas
assert not df_agg['country_region'].duplicated().any(), "Erro: Países duplicados encontrados!"
logging.info("✅ [PASS] Não há países duplicados no ranking.")

# =============================================================================
# 3. QUERY 3 — SÉRIE TEMPORAL DO PAÍS ALVO (BRASIL)
# =============================================================================
logging.info(f"Executando Query: Série Temporal para {COUNTRY_TARGET}...")
sql_ts = f"""
SELECT
    date,
    countries_and_territories AS country_region,
    SUM(CAST(daily_confirmed_cases AS INT64)) AS confirmed
FROM `{DATASET_TABLE}`
WHERE countries_and_territories = '{COUNTRY_TARGET}'
GROUP BY date, country_region
ORDER BY date ASC
"""

df_ts = client.query(sql_ts).to_dataframe()

# Tratamento de Tipos
df_ts['date'] = pd.to_datetime(df_ts['date'])
df_ts['confirmed'] = pd.to_numeric(df_ts['confirmed'], errors='coerce').fillna(0).astype('int64')

# Registrando Informações da Base no Log
logging.info(f"=== METADADOS DA BASE TEMPORAL ({COUNTRY_TARGET}) ===")
logging.info(f"Total de Registros Diários: {len(df_ts)}")
logging.info(f"Tipos de Colunas:\n{df_ts.dtypes.to_string()}")
logging.info(f"Quantidade de Nulos por Coluna:\n{df_ts.isna().sum().to_string()}")

# -----------------------------------------------------------------------------
# TESTES DE QUALIDADE DE DADOS (DATA QUALITY CHECKS) - TS
# -----------------------------------------------------------------------------
logging.info(f"--- Executando Testes de Qualidade (Série Temporal {COUNTRY_TARGET}) ---")

# Teste 1: Casos negativos
assert (df_ts['confirmed'] >= 0).all(), "Erro: Casos diários negativos encontrados!"
logging.info("✅ [PASS] Não existem casos diários negativos.")

# Teste 2: Ordenação de Datas
assert df_ts['date'].is_monotonic_increasing, "Erro: As datas da série temporal não estão em ordem cronológica!"
logging.info("✅ [PASS] As datas estão perfeitamente ordenadas do início ao fim do período.")

# Teste 3: Registro duplicado na mesma data
assert not df_ts['date'].duplicated().any(), "Erro: Registros duplicados para a mesma data!"
logging.info("✅ [PASS] Não existem datas duplicadas na série temporal.")

# =============================================================================
# 4. GERAÇÃO E SALVAMENTO DO GRÁFICO
# =============================================================================
logging.info("Gerando gráfico de linha da série temporal...")
plt.figure(figsize=(12, 6))
plt.plot(df_ts['date'], df_ts['confirmed'], color='#1f77b4', linewidth=1.8, label='Casos Diários')
plt.title(f'Evolução Diária de Casos Confirmados de COVID-19 — {COUNTRY_TARGET}', fontsize=14, fontweight='bold')
plt.xlabel('Data', fontsize=12)
plt.ylabel('Novos Casos Confirmados', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend(loc='upper right')
plt.tight_layout()

chart_path = os.path.join(DIR_IMAGES, FILE_CHART_NAME)
plt.savefig(chart_path, dpi=300)
plt.close()
logging.info(f"✅ Gráfico salvo com sucesso em: {chart_path}")

# =============================================================================
# 5. EXPORTAÇÃO DOS ARQUIVOS FINAIS
# =============================================================================
path_csv = os.path.join(DIR_CSV, FILE_CSV_NAME)
path_parquet = os.path.join(DIR_PARQUET, FILE_PARQUET_NAME)

df_agg.to_csv(path_csv, index=False, encoding='utf-8')
df_ts.to_parquet(path_parquet, index=False)

logging.info(f"✅ CSV exportado em: {path_csv}")
logging.info(f"✅ Parquet exportado em: {path_parquet}")
logging.info(f"✅ Logs completos gravados em: {log_filepath}")

logging.info("=" * 60)
logging.info("🚀 PIPELINE EXECUTADO E AUDITADO COM SUCESSO COMPLETO!")
logging.info("=" * 60)