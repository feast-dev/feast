"""The Go Bigtable online store's parity fixtures must match what this SDK writes."""

import importlib.util
import json
import pathlib

import pytest

pytest.importorskip("google.cloud.bigtable")

TESTDATA = (
    pathlib.Path(__file__).resolve().parents[6]
    / "go"
    / "internal"
    / "feast"
    / "onlinestore"
    / "testdata"
)


def test_go_fixtures_match_python_writer() -> None:
    """Fail when the committed fixtures drift from the in-tree Python store."""
    if not TESTDATA.is_dir():
        pytest.skip("the Go sources are not in this checkout")
    spec = importlib.util.spec_from_file_location(
        "gen_bigtable_fixtures", TESTDATA / "gen_bigtable_fixtures.py"
    )
    assert spec is not None and spec.loader is not None
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)

    committed = json.loads(
        (TESTDATA / "bigtable_fixtures.json").read_text(encoding="utf-8")
    )
    assert generator.build_fixtures() == committed, (
        "The Go Bigtable fixtures no longer match the Python writer. Regenerate them "
        "with the command in testdata/gen_bigtable_fixtures.py and check the Go tests."
    )
