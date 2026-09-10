"""Configurações globais, caminhos de diretórios e parâmetros do projeto."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

# Diretórios Raiz e de Dados
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

BRONZE_ANEEL_DIR = DATA_DIR / "bronze" / "aneel"
BRONZE_ENEM_DIR = DATA_DIR / "bronze" / "enem"

SILVER_ANEEL_DIR = DATA_DIR / "silver" / "aneel"
SILVER_ENEM_DIR = DATA_DIR / "silver" / "enem"
SILVER_JOINED_DIR = DATA_DIR / "silver" / "joined"

GOLD_ANALYTICS_DIR = DATA_DIR / "gold" / "analytics"
GOLD_ML_READY_DIR = DATA_DIR / "gold" / "ml_ready"

QUARANTINE_ANEEL_DIR = DATA_DIR / "quarantine" / "aneel"
QUARANTINE_ENEM_DIR = DATA_DIR / "quarantine" / "enem"

RAW_ENEM_DIR = DATA_DIR / "raw" / "enem"

# Garantia de existência de diretórios em tempo de execução
for d in [
    RAW_ENEM_DIR,
    BRONZE_ANEEL_DIR, BRONZE_ENEM_DIR,
    SILVER_ANEEL_DIR, SILVER_ENEM_DIR, SILVER_JOINED_DIR,
    GOLD_ANALYTICS_DIR, GOLD_ML_READY_DIR,
    QUARANTINE_ANEEL_DIR, QUARANTINE_ENEM_DIR
]:
    d.mkdir(parents=True, exist_ok=True)

# Parâmetros de Ingestão ANEEL
# Resource DEC/FEC 2020-2029: 4493985c-baea-429c-9df5-3030422c71d7
ANEEL_RESOURCE_ID = os.getenv("ANEEL_RESOURCE_ID", "4493985c-baea-429c-9df5-3030422c71d7")
# Resource de-para Conjunto -> Município IBGE: 3f841488-80a8-42f2-a6ca-e0c593b228de
ANEEL_RESOURCE_MUNICIPIOS = os.getenv("ANEEL_RESOURCE_MUNICIPIOS", "3f841488-80a8-42f2-a6ca-e0c593b228de")
ANEEL_API_BASE_URL = os.getenv("ANEEL_API_BASE_URL", "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search")

# Parâmetros de Filtro e Escopo (Série Histórica de 5 Anos)
ANOS_ESTUDO = [int(a.strip()) for a in os.getenv("ANOS_ESTUDO", "2019,2020,2021,2022,2023").split(",") if a.strip()]
UF_TARGET = [uf.strip() for uf in os.getenv("UF_TARGET", "PA,AM,RO,AP,RO,RR,AC,TO").split(",") if uf.strip()]
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))
T0_DATE = os.getenv("T0_DATE", "2024-07-31")

