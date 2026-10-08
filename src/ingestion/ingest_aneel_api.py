"""Script de ingestão da API REST da ANEEL para a camada Bronze.

Implementa:
- Paginação controlada por limit/offset;
- Backoff exponencial com Tenacity;
- Metadados técnicos de auditoria (_ingestion_time, _source, _source_object,
  _load_id, _record_hash);
- Idempotência por deduplicação de hash;
- Quarentena para falhas de requisição.
"""
import json
import requests
import pandas as pd
import hashlib
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import (
    BRONZE_ANEEL_DIR,
    QUARANTINE_ANEEL_DIR,
    ANEEL_API_BASE_URL,
    ANEEL_RESOURCE_ID,
    ANEEL_RESOURCE_MUNICIPIOS,
    UF_TARGET
)
from src.utils.logger import get_logger

logger = get_logger("ingest_aneel_api")

# Os cinco metadados exigidos pela rubrica: horário, sistema, objeto de
# origem, identificador da carga e hash estável do registro.
TECHNICAL_METADATA_COLUMNS = {
    "_ingestion_time",
    "_source",
    "_source_object",
    "_load_id",
    "_record_hash",
}
ANEEL_CHECKPOINT_FILE = BRONZE_ANEEL_DIR / "_aneel_ingestion_checkpoint.json"

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_aneel_data(
    resource_id: str,
    limit: int = 1000,
    offset: int = 0,
    filters: Optional[Dict[str, Any]] = None
) -> dict:
    """Busca registros na API CKAN da ANEEL com retry automático e suporte a filtros."""
    url = f"{ANEEL_API_BASE_URL}?resource_id={resource_id}&limit={limit}&offset={offset}"
    if filters:
        url += f"&filters={json.dumps(filters)}"

    logger.info(f"Requisitando API ANEEL (resource={resource_id[:8]}..., limit={limit}, offset={offset})...")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def generate_row_hash(row: pd.Series) -> str:
    """Gera hash estável do payload, sem metadados variáveis entre cargas."""
    payload = {
        str(column): row[column]
        for column in row.index
        if column not in TECHNICAL_METADATA_COLUMNS
    }
    row_str = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(row_str.encode("utf-8")).hexdigest()


def _with_technical_metadata(
    df: pd.DataFrame,
    *,
    source: str,
    source_object: str,
    load_id: str,
    ingestion_time: str,
) -> pd.DataFrame:
    """Adiciona os metadados técnicos obrigatórios sem contaminar o hash."""
    result = df.copy()
    result["_ingestion_time"] = ingestion_time
    result["_source"] = source
    result["_source_object"] = source_object
    result["_load_id"] = load_id
    result["_record_hash"] = result.apply(generate_row_hash, axis=1)
    return result


def _load_checkpoint() -> Optional[dict]:
    """Lê o checkpoint da carga incremental, se houver um checkpoint válido."""
    if not ANEEL_CHECKPOINT_FILE.exists():
        return None
    try:
        return json.loads(ANEEL_CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"Checkpoint ANEEL inválido; uma nova carga será iniciada: {exc}")
        return None


def _save_checkpoint(checkpoint: dict) -> None:
    """Grava o checkpoint de forma atômica para permitir retomada após falhas."""
    temporary_file = ANEEL_CHECKPOINT_FILE.with_suffix(".tmp")
    temporary_file.write_text(
        json.dumps(checkpoint, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_file.replace(ANEEL_CHECKPOINT_FILE)

def ingest_aneel_municipios_mapping(uf: str = "PA") -> Optional[pd.DataFrame]:
    """
    Ingere a tabela de relacionamento entre Conjuntos Elétricos e Municípios IBGE
    (Recurso indqual-municipio da ANEEL).
    """
    load_id = str(uuid.uuid4())
    ingestion_time = datetime.now().isoformat()
    logger.info(f"Iniciando ingestão do mapa Conjunto x Município da ANEEL (UF={uf}) - Load ID: {load_id}")

    all_records = []
    limit = 1000
    offset = 0
    has_more = True

    while has_more:
        try:
            data = fetch_aneel_data(
                resource_id=ANEEL_RESOURCE_MUNICIPIOS,
                limit=limit,
                offset=offset,
                filters={"SigUF": uf}
            )
            records = data.get("result", {}).get("records", [])
            if not records:
                break
            all_records.extend(records)
            offset += limit
            if len(records) < limit:
                break
        except Exception as e:
            error_msg = f"Falha na ingestão do mapa de municípios no offset {offset}. Erro: {e}"
            logger.error(error_msg)
            with open(QUARANTINE_ANEEL_DIR / f"erro_municipios_{load_id}.txt", "w", encoding="utf-8") as f:
                f.write(error_msg)
            break

    if not all_records:
        logger.warning("Nenhum registro de mapeamento retornado pela API ANEEL.")
        return None

    df = _with_technical_metadata(
        pd.DataFrame(all_records),
        source="API_ANEEL_INDQUAL_MUNICIPIO",
        source_object=f"datastore:{ANEEL_RESOURCE_MUNICIPIOS}",
        load_id=load_id,
        ingestion_time=ingestion_time,
    )
    df.drop_duplicates(subset=["_record_hash"], inplace=True)

    output_file = BRONZE_ANEEL_DIR / "aneel_conjuntos_municipios.parquet"
    df.to_parquet(output_file, index=False)
    logger.info(f"Sucesso! {len(df)} associações Conjunto-Município gravadas em: {output_file}")
    return df

def ingest_to_bronze(
    agent_filter: str = "EQUATORIAL PA",
    anos: Optional[list] = None,
    max_records_per_year: int = 5000,
    force_refresh: bool = False
):
    """
    Executa a ingestão dos indicadores DEC/FEC da ANEEL em micro-batches.

    Cada página da API é persistida antes de o checkpoint avançar. Assim, uma
    interrupção retoma do último offset confirmado, sem baixar novamente toda a
    fonte. O hash é calculado apenas sobre o payload original, tornando a
    deduplicação estável entre cargas.
    """
    if anos is None:
        anos = [2020, 2021, 2022, 2023, 2024]

    anos = sorted({int(ano) for ano in anos})

    mapa_file = BRONZE_ANEEL_DIR / "aneel_conjuntos_municipios.parquet"
    ind_files = list(BRONZE_ANEEL_DIR.glob("aneel_dec_fec_*.parquet"))

    checkpoint = _load_checkpoint()
    checkpoint_config = {
        "agent_filter": agent_filter,
        "anos": anos,
        "max_records_per_year": max_records_per_year,
        "resource_id": ANEEL_RESOURCE_ID,
    }

    if (
        not force_refresh
        and mapa_file.exists()
        and ind_files
        and checkpoint is not None
        and checkpoint.get("status") == "completed"
        and checkpoint.get("config") == checkpoint_config
    ):
        logger.info(
            f"Camada Bronze da ANEEL já consolidada com dados reais ({len(ind_files)} arquivo(s) de indicadores). "
            "Idempotência confirmada. Mantendo integridade dos dados brutos."
        )
        return

    if (
        force_refresh
        or checkpoint is None
        or checkpoint.get("config") != checkpoint_config
        or checkpoint.get("status") == "completed"
    ):
        checkpoint = {
            "version": 1,
            "status": "running",
            "config": checkpoint_config,
            "load_id": str(uuid.uuid4()),
            "ingestion_time": datetime.now().isoformat(),
            "years": {},
        }

    load_id = checkpoint["load_id"]
    ingestion_time = checkpoint["ingestion_time"]
    _save_checkpoint(checkpoint)

    # 1. Ingestão da tabela de correlação municipal (indqual-municipio)
    if not mapa_file.exists() or force_refresh:
        ingest_aneel_municipios_mapping(uf="PA")

    # 2. Ingestão dos Indicadores de Continuidade DEC/FEC por Ano
    logger.info(f"Iniciando carga Bronze ANEEL DEC/FEC (Agente={agent_filter}, Anos={anos}) - Load ID: {load_id}")

    # Ordena decrescente para coletar primeiro os anos recentes com dados abertos na API
    anos_ordenados = sorted(anos, reverse=True)

    for ano in anos_ordenados:
        year_state = checkpoint["years"].setdefault(
            str(ano),
            {"status": "pending", "next_offset": 0, "records_written": 0},
        )
        if year_state.get("status") == "completed":
            continue

        limit = 2000
        offset = int(year_state.get("next_offset", 0))
        records_written = int(year_state.get("records_written", 0))
        filters = {"SigAgente": agent_filter, "AnoIndice": ano} if agent_filter else {"AnoIndice": ano}

        logger.info(f"Coletando dados ANEEL para o ano {ano}...")
        while True:
            try:
                data = fetch_aneel_data(
                    resource_id=ANEEL_RESOURCE_ID,
                    limit=limit,
                    offset=offset,
                    filters=filters
                )
                records = data.get("result", {}).get("records", [])
                if not records:
                    year_state.update({"status": "completed", "next_offset": offset})
                    _save_checkpoint(checkpoint)
                    break

                if max_records_per_year:
                    remaining = max_records_per_year - records_written
                    if remaining <= 0:
                        year_state.update({"status": "completed", "next_offset": offset})
                        _save_checkpoint(checkpoint)
                        break
                    records = records[:remaining]

                batch_df = _with_technical_metadata(
                    pd.DataFrame(records),
                    source="API_ANEEL_CKAN",
                    source_object=f"datastore:{ANEEL_RESOURCE_ID}",
                    load_id=load_id,
                    ingestion_time=ingestion_time,
                )
                batch_df.drop_duplicates(subset=["_record_hash"], inplace=True)

                batch_file = BRONZE_ANEEL_DIR / f"aneel_dec_fec_{ano}_{offset}_{load_id}.parquet"
                batch_df.to_parquet(batch_file, index=False)
                records_written += len(batch_df)
                next_offset = offset + limit
                reached_limit = bool(max_records_per_year and records_written >= max_records_per_year)
                year_state.update({
                    "status": "completed" if reached_limit or len(data.get("result", {}).get("records", [])) < limit else "running",
                    "next_offset": next_offset,
                    "records_written": records_written,
                    "last_batch": str(batch_file.name),
                })
                _save_checkpoint(checkpoint)
                logger.info(f"Ano {ano}: {records_written} registros persistidos em micro-batches...")

                if reached_limit or len(data.get("result", {}).get("records", [])) < limit:
                    logger.info(f"Ano {ano}: checkpoint concluído.")
                    break
                offset = next_offset
            except Exception as e:
                error_msg = f"Falha na extração para ano {ano} offset {offset}. Erro: {e}"
                logger.error(error_msg)
                with open(QUARANTINE_ANEEL_DIR / f"erro_{ano}_{load_id}.txt", "w", encoding="utf-8") as f:
                    f.write(error_msg)
                year_state.update({"status": "failed", "last_error": str(e), "next_offset": offset})
                _save_checkpoint(checkpoint)
                break

    checkpoint["status"] = (
        "completed"
        if all(checkpoint["years"].get(str(ano), {}).get("status") == "completed" for ano in anos)
        else "partial"
    )
    _save_checkpoint(checkpoint)
    logger.info(f"Checkpoint ANEEL finalizado com status: {checkpoint['status']}")

if __name__ == "__main__":
    ingest_to_bronze(agent_filter="EQUATORIAL PA", anos=[2020, 2021, 2022, 2023, 2024], max_records_per_year=5000)

