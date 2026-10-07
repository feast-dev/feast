import runpy
from pathlib import Path

import pytest


def test_cpu_hashes_preserve_pypi_hashes_and_pins() -> None:
    script = (
        Path(__file__).resolve().parents[6] / "infra/scripts/add_cpu_torch_hashes.py"
    )
    add_hashes = runpy.run_path(str(script))["add_cpu_hashes"]
    pypi_hash = "a" * 64
    cpu_hash = "b" * 64
    requirements = (
        f"torch==2.13.0 \\\n    --hash=sha256:{pypi_hash}\n"
        "    # via feast\nnumpy==2.2.0\n"
    )
    cpu_requirements = f"torch==2.13.0+cpu \\\n    --hash=sha256:{cpu_hash}\n"
    expected = (
        f"torch==2.13.0 \\\n    --hash=sha256:{pypi_hash} \\\n"
        f"    --hash=sha256:{cpu_hash}\n"
        "    # via feast\nnumpy==2.2.0\n"
    )
    assert add_hashes(requirements, cpu_requirements) == expected
    assert add_hashes(expected, cpu_requirements) == expected


def test_cpu_hashes_reject_different_package_version() -> None:
    script = (
        Path(__file__).resolve().parents[6] / "infra/scripts/add_cpu_torch_hashes.py"
    )
    add_hashes = runpy.run_path(str(script))["add_cpu_hashes"]
    requirements = f"torch==2.13.0 \\\n    --hash=sha256:{'a' * 64}\n"
    cpu_requirements = f"torch==2.12.0+cpu \\\n    --hash=sha256:{'b' * 64}\n"
    with pytest.raises(ValueError, match="Missing CPU hashes for torch==2.13.0"):
        add_hashes(requirements, cpu_requirements)
