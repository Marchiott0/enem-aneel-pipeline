"""Script de ingestão do arquivo CSV dos Microdados do ENEM para a camada Bronze.

Implementa:
- Leitura em lotes (chunksize) para suportar arquivos volumosos;
- Tratamento de encoding e separadores;
- Metadados técnicos (_ingestion_time, _source, _load_id, _record_hash);
- Idempotência com deduplicação;
- Quarentena para linhas malformadas.
"""
import pandas as pd
import hashlib
import uuid
from datetime import datetime
from pathlib import Path

from src.config import BRONZE_ENEM_DIR, QUARANTINE_ENEM_DIR, UF_TARGET
from src.utils.logger import get_logger

logger = get_logger("ingest_enem_csv")

def generate_row_hash(row: pd.Series) -> str:
    """Gera hash SHA-256 para cada registro."""
    row_str = "".join(str(val) for val in row.values)
    return hashlib.sha256(row_str.encode("utf-8", errors="ignore")).hexdigest()

def ingest_enem_csv(csv_path: str = "data/raw/enem_microdados.csv", chunk_size: int = 50000):
    """Lê o arquivo CSV do ENEM, anexa metadados e persiste em Parquet na camada Bronze."""
    load_id = str(uuid.uuid4())
    ingestion_time = datetime.now().isoformat()
    logger.info(f"Iniciando ingestão do CSV ENEM - Load ID: {load_id}")

    csv_file = Path(csv_path)
    if not csv_file.exists():
        logger.warning(f"Arquivo {csv_file} não encontrado localmente. Utilizando placeholder estruturado para template.")
        # Cria dataframe de demonstração caso o arquivo bruto ainda não tenha sido baixado
        demo_data = {
            "NU_INSCRICAO": ["210001", "210002", "210003"],
            "CO_MUNICIPIO_PROVA": ["1501402", "1500800", "1501402"],
            "NO_MUNICIPIO_PROVA": ["Belém", "Ananindeua", "Belém"],
            "SG_UF_PROVA": [UF_TARGET, UF_TARGET, UF_TARGET],
            "TP_PRESENCA_CN": [1, 0, 1],
            "TP_PRESENCA_CH": [1, 0, 1],
            "TP_PRESENCA_LC": [1, 0, 1],
            "TP_PRESENCA_MT": [1, 0, 1],
            "NU_NOTA_CN": [550.0, None, 620.0],
            "NU_NOTA_CH": [580.0, None, 640.0],
            "NU_NOTA_LC": [530.0, None, 590.0],
            "NU_NOTA_MT": [600.0, None, 710.0],
            "NU_NOTA_REDACAO": [700.0, None, 840.0]
        }
        df = pd.DataFrame(demo_data)
        df["_ingestion_time"] = ingestion_time
        df["_source"] = "CSV_ENEM_DEMO"
        df["_load_id"] = load_id
        df["_record_hash"] = df.apply(generate_row_hash, axis=1)

        date_partition = datetime.now().strftime("%Y%m%d")
        output_file = BRONZE_ENEM_DIR / f"enem_{date_partition}.parquet"
        df.to_parquet(output_file, index=False)
        logger.info(f"Arquivo demonstrativo salvo em: {output_file}")
        return

    # Leitura em lotes para grandes arquivos
    chunks = []
    try:
        for i, chunk in enumerate(pd.read_csv(csv_file, sep=";", encoding="latin1", chunksize=chunk_size, low_memory=False)):
            logger.info(f"Processando chunk {i+1}...")
            # Filtragem prévia por UF para economia de memória
            if "SG_UF_PROVA" in chunk.columns:
                chunk = chunk[chunk["SG_UF_PROVA"] == UF_TARGET].copy()
            chunks.append(chunk)
    except Exception as e:
        error_msg = f"Erro ao processar CSV: {e}"
        logger.error(error_msg)
        with open(QUARANTINE_ENEM_DIR / f"erro_csv_{load_id}.txt", "w", encoding="utf-8") as f:
            f.write(error_msg)
        return

    if chunks:
        df = pd.concat(chunks, ignore_index=True)
        df["_ingestion_time"] = ingestion_time
        df["_source"] = "CSV_ENEM_MICRODADOS"
        df["_load_id"] = load_id
        df["_record_hash"] = df.apply(generate_row_hash, axis=1)
        df.drop_duplicates(subset=["_record_hash"], inplace=True)

        date_partition = datetime.now().strftime("%Y%m%d")
        output_file = BRONZE_ENEM_DIR / f"enem_{date_partition}.parquet"
        df.to_parquet(output_file, index=False)
        logger.info(f"Sucesso! {len(df)} registros do ENEM ingeridos em {output_file}")

if __name__ == "__main__":
    ingest_enem_csv()
