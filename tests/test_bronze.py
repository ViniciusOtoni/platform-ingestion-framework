import json

from pyspark.sql import functions as F

from edp_ingestion.bronze import BRONZE_COLUMNS, CAPTURE_AVRO_SCHEMA, to_bronze

EVENT = {
    "specversion": "1.0",
    "id": "0b9a2d4e-8c3f-4f1e-b6a7-1d2c3e4f5a6b",
    "source": "/dominios/pedidos/pedidos-api",
    "type": "pedidos.pedido.criado.v1",
    "subject": "pedido/p-1",
    "time": "2026-10-14T01:07:58.123+00:00",
    "aggregateid": "p-1",
    "aggregateversion": 1,
    "data": {"pedido_id": "p-1"},
}
FILE = "abfss://capture-pedidos@st.dfs.core.windows.net/evhns-edp-dev-x/pedidos.pedido.v1/1/2026/10/14/01/06/46.avro"


def test_to_bronze_extracts_envelope_and_log_position(spark):
    capture = spark.createDataFrame(
        [(3, "4294967296", "10/14/2026 1:07:59 AM", {}, {}, bytearray(json.dumps(EVENT).encode()))],
        CAPTURE_AVRO_SCHEMA,
    )
    bronze = to_bronze(capture, F.lit(FILE))
    [row] = bronze.collect()
    # collect() converte timestamps para o fuso local da máquina: compara formatado no Spark (UTC).
    [times] = bronze.select(
        F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("event_time"),
        F.date_format("eh_enqueued_time", "yyyy-MM-dd'T'HH:mm:ss").alias("enqueued"),
    ).collect()

    assert list(row.asDict()) == BRONZE_COLUMNS
    assert json.loads(row.body) == EVENT  # CloudEvent íntegro: replay possível
    assert (row.event_id, row.event_type, row.aggregate_id, row.aggregate_version) == (
        EVENT["id"], "pedidos.pedido.criado.v1", "p-1", 1)
    assert times.event_time == "2026-10-14T01:07:58.123"
    assert (row.eh_partition, row.eh_sequence_number, row.eh_offset) == (1, 3, "4294967296")
    assert times.enqueued == "2026-10-14T01:07:59"
    assert row._source_file == FILE


def test_non_cloudevent_body_is_kept(spark):
    row = (0, "0", "10/14/2026 1:07:59 AM", {}, {}, bytearray(b"lixo"))
    capture = spark.createDataFrame([row], CAPTURE_AVRO_SCHEMA)
    [row] = to_bronze(capture, F.lit(FILE)).collect()
    assert row.body == "lixo" and row.event_type is None  # entra na Bronze; a Silver põe em quarentena
