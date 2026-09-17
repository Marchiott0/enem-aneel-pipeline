"""Script de ingestão do arquivo CSV dos Microdados do ENEM para a camada Bronze.

Implementa:
- Leitura em lotes (chunksize) para suportar arquivos volumosos;
- Tratamento de encoding e separadores;
- Metadados técnicos (_ingestion_time, _source, _load_id, _record_hash);
- Idempotência com deduplicação;
- Quarentena para linhas malformadas.

CORREÇÃO (03/09/2026): o arquivo de saída agora é nomeado pelo ANO DO ENEM
(extraído da coluna NU_ANO dos dados), e não mais pela data de ingestão.
Antes, ingerir dois anos diferentes no mesmo dia sobrescrevia o Parquet
anterior, porque os dois geravam o mesmo nome de arquivo (enem_<data_de_hoje>.parquet).
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
    logger.info(f"Filtrando pelas UFs: {UF_TARGET}")
 
    csv_file = Path(csv_path)
    if not csv_file.exists():
        logger.error(f"Arquivo {csv_file} não encontrado. Abortando ingestão (nenhum dado de demonstração será criado).")
        return
 
    # Leitura em lotes para grandes arquivos
    chunks = []
    try:
        for i, chunk in enumerate(pd.read_csv(csv_file, sep=";", encoding="latin1", chunksize=chunk_size, low_memory=False)):
            logger.info(f"Processando chunk {i+1}...")
            # Filtragem prévia por lista de UFs (Região Norte) para economia de memória
            if "SG_UF_PROVA" in chunk.columns:
                chunk = chunk[chunk["SG_UF_PROVA"].isin(UF_TARGET)].copy()
            chunks.append(chunk)
    except Exception as e:
        error_msg = f"Erro ao processar CSV: {e}"
        logger.error(error_msg)
        with open(QUARANTINE_ENEM_DIR / f"erro_csv_{load_id}.txt", "w", encoding="utf-8") as f:
            f.write(error_msg)
        return
 
    if not chunks:
        logger.warning("Nenhum registro restou após o filtro de UF. Nada foi salvo.")
        return
 
    df = pd.concat(chunks, ignore_index=True)
 
    if df.empty:
        logger.warning("Nenhum registro das UFs selecionadas foi encontrado neste arquivo. Nada foi salvo.")
        return
 
    df["_ingestion_time"] = ingestion_time
    df["_source"] = "CSV_ENEM_MICRODADOS"
    df["_load_id"] = load_id
    df["_record_hash"] = df.apply(generate_row_hash, axis=1)
    df.drop_duplicates(subset=["_record_hash"], inplace=True)
 
    # --- Detecta o ano do ENEM a partir dos próprios dados ---
    ano_enem = None
    if "NU_ANO" in df.columns:
        anos_encontrados = df["NU_ANO"].dropna().unique()
        if len(anos_encontrados) == 1:
            ano_enem = str(int(anos_encontrados[0]))
        elif len(anos_encontrados) > 1:
            logger.warning(
                f"Mais de um ano encontrado no arquivo ({anos_encontrados}). "
                "Usando o nome do arquivo de entrada para nomear a partição."
            )
 
    if ano_enem is None:
        import re
        match = re.search(r"(20\d{2})", csv_file.stem)
        ano_enem = match.group(1) if match else "desconhecido"
        if ano_enem == "desconhecido":
            logger.warning(
                "Não foi possível determinar o ano do ENEM. Usando 'desconhecido' — "
                "RENOMEIE o Parquet gerado manualmente."
            )
 
    output_file = BRONZE_ENEM_DIR / f"enem_{ano_enem}.parquet"
 
    if output_file.exists():
        logger.warning(
            f"{output_file} já existe e será SOBRESCRITO. Isso é esperado se você está "
            "reingerindo o mesmo ano com o novo filtro de UFs (ex: trocando de só-PA para "
            "Região Norte inteira); NÃO é esperado se eram anos diferentes."
        )
 
    df.to_parquet(output_file, index=False)
    logger.info(f"Sucesso! {len(df)} registros do ENEM ({ano_enem}) ingeridos em {output_file}")


def ingest_enem_bronze(anos=None):
    """
    Ingere ou valida a presença dos microdados do ENEM na Camada Bronze.
    1. Verifica se há arquivos CSV brutos em data/raw/ ou data/raw/enem/ para processar em lotes;
    2. Garante a idempotência: caso os dados já estejam salvos na Bronze, valida e reaproveita.
    """
    import os
    if anos is None:
        anos = [2020, 2021, 2022, 2023, 2024]

    # 1. Verifica se há novos arquivos CSV brutos para ingerir
    raw_files = list(Path("data/raw").glob("**/*.csv"))
    env_csv = os.getenv("ENEM_SOURCE_PATH")
    if env_csv and Path(env_csv).exists():
        raw_files.append(Path(env_csv))

    for csv_f in raw_files:
        logger.info(f"Arquivo CSV bruto encontrado: {csv_f}. Executando ingestão...")
        ingest_enem_csv(csv_path=str(csv_f))

    # 2. Validação de Idempotência da Camada Bronze
    arquivos_bronze = list(BRONZE_ENEM_DIR.glob("enem_*.parquet"))
    anos_presentes = []
    for f in arquivos_bronze:
        import re
        match = re.search(r"enem_(\d{4})", f.name)
        if match:
            anos_presentes.append(int(match.group(1)))

    faltando = [ano for ano in anos if ano not in anos_presentes]

    if not faltando:
        logger.info(
            f"Camada Bronze do ENEM já consolidada com dados reais (Anos: {sorted(anos_presentes)}). "
            f"Idempotência confirmada. Mantendo integridade dos dados brutos."
        )
        return

    logger.warning(f"Anos ausentes na Bronze ENEM: {faltando}. Verifique os arquivos CSV na pasta data/raw/.")


def ingest_enem_sample_multiyear(anos=None):
    """Alias para ingest_enem_bronze para compatibilidade."""
    return ingest_enem_bronze(anos=anos)


if __name__ == "__main__":
    ingest_enem_bronze()

 