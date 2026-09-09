from __future__ import annotations

from pathlib import Path

import toml  # type: ignore[import-untyped]


def test_flink_extra_constrains_shared_pyarrow_dependency() -> None:
    pyproject_path = Path(__file__).resolve().parents[7] / "pyproject.toml"
    pyproject = toml.loads(pyproject_path.read_text())

    dependencies = pyproject["project"]["dependencies"]
    flink_dependencies = pyproject["project"]["optional-dependencies"]["flink"]

    assert "pyarrow>=16.1.0" in dependencies
    assert not any(
        dependency.startswith("pyarrow") and "extra" in dependency
        for dependency in dependencies
    )
    assert "pyarrow<21.0.0" in flink_dependencies
    assert "apache-flink>=2.2.1,<3" in flink_dependencies
