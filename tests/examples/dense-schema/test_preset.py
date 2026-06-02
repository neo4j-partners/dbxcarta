from __future__ import annotations

import sys

import pytest

from dbxcarta_dense_schema_example.preset import DenseSchemaPreset, preset


def test_preset_targets_dense_data_catalog() -> None:
    assert preset.catalog == "dense-schema_example"


def test_preset_rejects_invalid_identifier() -> None:
    with pytest.raises(ValueError):
        DenseSchemaPreset(catalog="invalid catalog name with spaces")


def test_readiness_queries_the_data_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    def fake_fetch(ws: object, warehouse_id: str, catalog: str) -> list[str]:
        captured["catalog"] = catalog
        return ["dense_1000"]

    # The package re-exports the `preset` instance, which shadows the module
    # for monkeypatch's dotted-string form, so patch the module object directly.
    preset_module = sys.modules[DenseSchemaPreset.__module__]
    monkeypatch.setattr(preset_module, "_fetch_schema_names", fake_fetch)
    monkeypatch.setenv("DBXCARTA_SCHEMAS", "dense_1000")

    report = preset.readiness(ws=None, warehouse_id="wh")  # type: ignore[arg-type]

    assert captured["catalog"] == "dense-schema_example"
    assert report.present == ("dense_1000",)
    assert report.missing_required == ()
