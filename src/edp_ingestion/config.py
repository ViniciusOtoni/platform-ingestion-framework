"""Configuração declarada pelo domínio (``<dominio>-data/config/bronze.yml``).

Valores ``${nome}`` são substituídos pelos ``--var nome=valor`` passados pelo job (variáveis do bundle),
o que permite o mesmo YAML em dev e prd.

    ingestion:
      - event_hub: pedidos.pedido.v1
        capture_url: ${capture_url}
        target: ${catalog}.bronze.pedido_eventos_raw
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

_VAR = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
_TABLE = re.compile(r"^[a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+$")


def substitute(text: str, variables: dict[str, str]) -> str:
    def repl(m: re.Match) -> str:
        if m[1] not in variables:
            raise KeyError(f"variável ${{{m[1]}}} não informada (use --var {m[1]}=...)")
        return variables[m[1]]

    return _VAR.sub(repl, text)


@dataclass(frozen=True)
class IngestionSpec:
    event_hub: str
    capture_url: str
    target: str

    @property
    def catalog(self) -> str:
        return self.target.split(".")[0]

    @property
    def schema(self) -> str:
        return self.target.split(".")[1]

    @property
    def table(self) -> str:
        return self.target.split(".")[2]

    @property
    def source_glob(self) -> str:
        # Layout do Capture: <namespace>/<event_hub>/<particao>/AAAA/MM/DD/HH/mm/ss.avro
        return f"{self.capture_url.rstrip('/')}/*/{self.event_hub}/"

    @property
    def checkpoint_dir(self) -> str:
        # Serverless exige checkpoints em Volume do Unity Catalog.
        return f"/Volumes/{self.catalog}/{self.schema}/_checkpoints/{self.table}"


def load(path: str | Path, variables: dict[str, str]) -> list[IngestionSpec]:
    doc = yaml.safe_load(substitute(Path(path).read_text(encoding="utf-8"), variables))
    specs = [IngestionSpec(**item) for item in (doc or {}).get("ingestion", [])]
    if not specs:
        raise ValueError(f"{path}: nenhuma entrada em 'ingestion'")
    for s in specs:
        if not _TABLE.match(s.target):
            raise ValueError(f"target inválido {s.target!r}: use <catalogo>.<schema>.<tabela> em minúsculas")
        if not s.capture_url.startswith("abfss://"):
            raise ValueError(f"capture_url deve ser abfss:// (external location do Capture): {s.capture_url!r}")
    return specs
