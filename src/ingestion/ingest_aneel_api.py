"""Script de ingestão da API REST da ANEEL para a camada Bronze.

Implementa:
- Paginação controlada por limit/offset;
- Backoff exponencial com Tenacity;
- Metadados técnicos de auditoria (_ingestion_time, _source, _load_id, _record_hash);
- Idempotência por deduplicação de hash;
- Quarentena para falhas de requisição.
"""
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path para permitir execução direta
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import requests
import pandas as pd
import hashlib
import uuid
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import BRONZE_ANEEL_DIR, QUARANTINE_ANEEL_DIR, ANEEL_API_BASE_URL, ANEEL_RESOURCE_ID
from src.utils.logger import get_logger


logger = get_logger("ingest_aneel_api")

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_aneel_data(limit: int = 1000, offset: int = 0) -> dict:
    """Busca registros na API CKAN da ANEEL com retry automático."""
    url = f"{ANEEL_API_BASE_URL}?resource_id={ANEEL_RESOURCE_ID}&limit={limit}&offset={offset}"
    logger.info(f"Requisitando API ANEEL (limit={limit}, offset={offset})...")
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return response.json()

def generate_row_hash(row: pd.Series) -> str:
    """Gera hash SHA-256 a partir dos valores da linha para deduplicação idempotente."""
    row_str = "".join(str(val) for val in row.values)
    return hashlib.sha256(row_str.encode("utf-8")).hexdigest()

def ingest_to_bronze(max_records_debug: int = 10000):
    """Executa o ciclo completo de ingestão e gravação na Bronze."""
    load_id = str(uuid.uuid4())
    ingestion_time = datetime.now().isoformat()
    logger.info(f"Iniciando carga Bronze ANEEL - Load ID: {load_id}")

    all_records = []
    limit = 1000
    offset = 0
    has_more = True

    while has_more:
        try:
            data = fetch_aneel_data(limit=limit, offset=offset)
            records = data.get("result", {}).get("records", [])

            if not records:
                has_more = False
                break

            all_records.extend(records)
            offset += limit
            logger.info(f"Progresso: {len(all_records)} registros baixados...")

            if max_records_debug and offset >= max_records_debug:
                logger.info(f"Limite preventivo de debug ({max_records_debug}) atingido.")
                break

        except Exception as e:
            error_msg = f"Falha na extração no offset {offset}. Erro: {e}"
            logger.error(error_msg)
            quarantine_path = QUARANTINE_ANEEL_DIR / f"erro_extracao_{load_id}.txt"
            with open(quarantine_path, "w", encoding="utf-8") as f:
                f.write(error_msg)
            break

    if all_records:
        df = pd.DataFrame(all_records)

        # Adição de Metadados Técnicos Obrigatórios
        df["_ingestion_time"] = ingestion_time
        df["_source"] = "API_ANEEL_CKAN"
        df["_load_id"] = load_id
        df["_record_hash"] = df.apply(generate_row_hash, axis=1)

        # Garantia de Idempotência
        initial_count = len(df)
        df.drop_duplicates(subset=["_record_hash"], inplace=True)
        dedup_count = len(df)
        logger.info(f"Idempotência aplicada: {initial_count - dedup_count} duplicatas removidas.")

        # Particionamento por data de ingestão (YYYYMMDD)
        date_partition = datetime.now().strftime("%Y%m%d")
        output_file = BRONZE_ANEEL_DIR / f"aneel_dec_fec_{date_partition}.parquet"
        df.to_parquet(output_file, index=False)
        logger.info(f"Sucesso! {len(df)} registros gravados na Bronze em: {output_file}")

if __name__ == "__main__":
    ingest_to_bronze()
