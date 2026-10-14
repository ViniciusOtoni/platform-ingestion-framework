"""Bronze: fiel à origem, append-only, sem aplicar o schema do payload (ADR-0001).

Cada registro Avro do Capture vira uma linha com o CloudEvent bruto (``body``), os campos do envelope
extraídos para facilitar dedupe/roteamento e a posição no log do Event Hubs (replay/auditoria).
Eventos fora do contrato ENTRAM na Bronze e só são barrados na Silver: nada se perde.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from edp_ingestion.config import IngestionSpec

log = logging.getLogger(__name__)

# Schema do Avro gravado pelo Event Hubs Capture (fixo, definido pela Azure).
CAPTURE_AVRO_SCHEMA = (
    "SequenceNumber BIGINT, Offset STRING, EnqueuedTimeUtc STRING, "
    "SystemProperties MAP<STRING, STRING>, Properties MAP<STRING, STRING>, Body BINARY"
)

BRONZE_COLUMNS = [
    "body", "event_id", "event_type", "event_source", "event_time", "subject", "aggregate_id", "aggregate_version",
    "eh_partition", "eh_offset", "eh_sequence_number", "eh_enqueued_time", "_source_file", "_ingested_at",
]


def _envelope(field: str) -> Column:
    return F.get_json_object(F.col("body"), f"$.{field}")


def to_bronze(capture: DataFrame, source_file: Column) -> DataFrame:
    """Transforma registros do Capture no layout Bronze. Função pura (testável sem Azure)."""
    return (
        capture.withColumn("body", F.col("Body").cast("string"))
        .select(
            "body",
            _envelope("id").alias("event_id"),
            _envelope("type").alias("event_type"),
            _envelope("source").alias("event_source"),
            F.to_timestamp(_envelope("time")).alias("event_time"),
            _envelope("subject").alias("subject"),
            _envelope("aggregateid").alias("aggregate_id"),
            _envelope("aggregateversion").cast("int").alias("aggregate_version"),
            F.regexp_extract(source_file, r"/(\d+)/\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}\.avro$", 1)
            .cast("int").alias("eh_partition"),
            F.col("Offset").alias("eh_offset"),
            F.col("SequenceNumber").alias("eh_sequence_number"),
            F.to_timestamp(F.col("EnqueuedTimeUtc"), "M/d/yyyy h:mm:ss a").alias("eh_enqueued_time"),
            source_file.alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


def ingest(spark: SparkSession, spec: IngestionSpec) -> None:
    """Auto Loader (availableNow) do Capture para a tabela Bronze."""
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {spec.catalog}.{spec.schema}._checkpoints")
    log.info("Bronze %s <- %s", spec.target, spec.source_glob)

    capture = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "avro")
        .option("cloudFiles.schemaLocation", f"{spec.checkpoint_dir}/_schema")
        .schema(CAPTURE_AVRO_SCHEMA)
        .load(spec.source_glob)
    )
    bronze = to_bronze(capture, F.col("_metadata.file_path"))

    query = (
        bronze.writeStream.option("checkpointLocation", spec.checkpoint_dir)
        .trigger(availableNow=True)
        .toTable(spec.target)
    )
    query.awaitTermination()
    progress = query.lastProgress or {}
    log.info("Bronze %s: %s linhas neste ciclo", spec.target, progress.get("numInputRows", 0))
