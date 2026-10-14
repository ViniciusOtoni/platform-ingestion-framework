"""Entry point do job Bronze (python_wheel_task, entry_point ``main``).

    edp-ingestion --config /Workspace/.../config/bronze.yml --var catalog=pedidos_dev --var capture_url=abfss://...
"""

from __future__ import annotations

import argparse
import logging
import sys

from edp_ingestion.config import load


def _parse_vars(items: list[str]) -> dict[str, str]:
    out = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep:
            raise SystemExit(f"--var inválido {item!r}: use nome=valor")
        out[key] = value
    return out


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="edp-ingestion")
    parser.add_argument("--config", required=True)
    parser.add_argument("--var", action="append", default=[])
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    specs = load(args.config, _parse_vars(args.var))

    from pyspark.sql import SparkSession

    from edp_ingestion.bronze import ingest

    spark = SparkSession.builder.getOrCreate()
    for spec in specs:
        ingest(spark, spec)
    return 0


if __name__ == "__main__":
    sys.exit(main())
