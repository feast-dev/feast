from pathlib import Path

import toml  # type: ignore[import-untyped]


def test_ui_runtime_grpc_dependencies_are_base_dependencies() -> None:
    pyproject_path = Path(__file__).resolve().parents[4] / "pyproject.toml"
    pyproject = toml.loads(pyproject_path.read_text())
    dependencies = pyproject["project"]["dependencies"]

    assert "grpcio>=1.56.2" in dependencies
    assert "grpcio-reflection>=1.56.2" in dependencies
    assert "grpcio-health-checking>=1.56.2" in dependencies
