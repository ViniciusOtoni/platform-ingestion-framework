# platform-ingestion-framework (`edp-ingestion`)

Framework **Bronze** da plataforma (ADR-0001 / ADR-0006). Lê os arquivos Avro do **Event Hubs Capture** com **Auto Loader** e grava uma tabela Delta **append-only**, num job Databricks disparado por **file arrival**. O domínio só declara um YAML; a plataforma executa.

## Configuração (no repositório `<dominio>-data`)

```yaml
# config/bronze.yml
ingestion:
  - event_hub: pedidos.pedido.v1
    capture_url: ${capture_url}          # external location do Capture (abfss://capture-<dominio>@...)
    target: ${catalog}.bronze.pedido_eventos_raw
```

```yaml
# job no Databricks Asset Bundle
python_wheel_task:
  package_name: edp_ingestion
  entry_point: main
  parameters: ["--config", "${workspace.file_path}/config/bronze.yml",
               "--var", "catalog=${var.catalog}", "--var", "capture_url=${var.capture_url}"]
```

## Layout da Bronze

| Coluna | Origem |
|---|---|
| `body` | CloudEvent completo (corpo do Avro), o que garante o replay |
| `event_id`, `event_type`, `event_source`, `event_time`, `subject`, `aggregate_id`, `aggregate_version` | Envelope CloudEvents |
| `eh_partition`, `eh_offset`, `eh_sequence_number`, `eh_enqueued_time` | Posição no log do Event Hubs |
| `_source_file`, `_ingested_at` | Linhagem da ingestão |

O schema do **payload não é aplicado aqui**: eventos inválidos entram na Bronze e são barrados na Silver, então nada se perde. Os checkpoints ficam em `/Volumes/<catalogo>/bronze/_checkpoints/` (exigência do serverless).

## Desenvolvimento

```bash
pip install -e ".[dev]"     # inclui pyspark para testes locais (Java 17)
ruff check . && pytest
```

Os wheels são publicados como assets de GitHub Release (`cd-python-release`).
