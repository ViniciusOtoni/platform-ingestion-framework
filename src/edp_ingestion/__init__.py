"""Framework de ingestão Bronze (ADR-0001 / ADR-0006).

Event Hubs Capture (Avro) -> Auto Loader -> Delta append-only, num job disparado por file arrival.
O domínio só declara configuração (YAML); a plataforma executa.
"""

__version__ = "0.1.0"
