import pytest

from edp_ingestion.config import load, substitute

YAML = """
ingestion:
  - event_hub: pedidos.pedido.v1
    capture_url: ${capture_url}
    target: ${catalog}.bronze.pedido_eventos_raw
"""


def test_load_with_variables(tmp_path):
    path = tmp_path / "bronze.yml"
    path.write_text(YAML, encoding="utf-8")
    [spec] = load(path, {"catalog": "pedidos_dev", "capture_url": "abfss://capture-pedidos@st.dfs.core.windows.net/"})
    assert spec.target == "pedidos_dev.bronze.pedido_eventos_raw"
    assert spec.source_glob == "abfss://capture-pedidos@st.dfs.core.windows.net/*/pedidos.pedido.v1/"
    assert spec.checkpoint_dir == "/Volumes/pedidos_dev/bronze/_checkpoints/pedido_eventos_raw"


def test_missing_variable_is_explicit():
    with pytest.raises(KeyError, match="--var catalog"):
        substitute("${catalog}.x.y", {})


def test_validations(tmp_path):
    path = tmp_path / "bronze.yml"
    path.write_text(YAML.replace("${capture_url}", "/tmp/capture"), encoding="utf-8")
    with pytest.raises(ValueError, match="abfss"):
        load(path, {"catalog": "pedidos_dev"})
    path.write_text("ingestion: []", encoding="utf-8")
    with pytest.raises(ValueError, match="nenhuma"):
        load(path, {})
