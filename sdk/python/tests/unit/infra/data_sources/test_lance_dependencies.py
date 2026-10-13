from __future__ import annotations

from pathlib import Path

import toml  # type: ignore[import-untyped]


def test_lance_extra_installs_native_reader_and_is_included_in_ci() -> None:
    pyproject_path = Path(__file__).resolve().parents[6] / "pyproject.toml"
    pyproject = toml.loads(pyproject_path.read_text())

    extras = pyproject["project"]["optional-dependencies"]

    assert extras["lance"] == [
        "pylance>=12.0.0",
        "lance-namespace>=0.11.1",
    ]
    assert "lance" in extras["ci"][0]
