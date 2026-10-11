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


def test_cpu_hashes_leave_an_existing_cpu_pin_untouched() -> None:
    """A universal resolve emits a `+cpu` pin of its own, which needs nothing.

    `uv pip compile --universal --torch-backend cpu` splits torch by marker, so
    the lock holds both a PyPI pin for darwin and a `+cpu` pin for everything
    else. The `+cpu` entry is already the CPU wheel; looking it up by appending
    `+cpu` to its version would ask for `2.13.0+cpu+cpu` and fail.
    """
    script = (
        Path(__file__).resolve().parents[6] / "infra/scripts/add_cpu_torch_hashes.py"
    )
    add_hashes = runpy.run_path(str(script))["add_cpu_hashes"]
    pypi_hash = "a" * 64
    cpu_hash = "b" * 64
    requirements = (
        f"torch==2.13.0 ; sys_platform == 'darwin' \\\n"
        f"    --hash=sha256:{pypi_hash}\n"
        f"torch==2.13.0+cpu ; sys_platform != 'darwin' \\\n"
        f"    --hash=sha256:{cpu_hash}\n"
    )
    cpu_requirements = f"torch==2.13.0+cpu \\\n    --hash=sha256:{cpu_hash}\n"
    expected = (
        f"torch==2.13.0 ; sys_platform == 'darwin' \\\n"
        f"    --hash=sha256:{pypi_hash} \\\n"
        f"    --hash=sha256:{cpu_hash}\n"
        f"torch==2.13.0+cpu ; sys_platform != 'darwin' \\\n"
        f"    --hash=sha256:{cpu_hash}\n"
    )
    assert add_hashes(requirements, cpu_requirements) == expected
    assert add_hashes(expected, cpu_requirements) == expected


def test_cpu_hashes_handle_a_marker_split_for_torch_and_torchvision() -> None:
    """The real lock splits both packages, which is what broke the script."""
    script = (
        Path(__file__).resolve().parents[6] / "infra/scripts/add_cpu_torch_hashes.py"
    )
    add_hashes = runpy.run_path(str(script))["add_cpu_hashes"]
    requirements = (
        f"torch==2.14.1 ; sys_platform == 'darwin' \\\n    --hash=sha256:{'a' * 64}\n"
        f"torch==2.14.1+cpu ; sys_platform != 'darwin' \\\n"
        f"    --hash=sha256:{'b' * 64}\n"
        f"torchvision==0.29.1 ; sys_platform == 'darwin' \\\n"
        f"    --hash=sha256:{'c' * 64}\n"
        f"torchvision==0.29.1+cpu ; sys_platform != 'darwin' \\\n"
        f"    --hash=sha256:{'d' * 64}\n"
    )
    cpu_requirements = (
        f"torch==2.14.1+cpu \\\n    --hash=sha256:{'b' * 64}\n"
        f"torchvision==0.29.1+cpu \\\n    --hash=sha256:{'d' * 64}\n"
    )
    result = add_hashes(requirements, cpu_requirements)
    assert f"    --hash=sha256:{'b' * 64}" in result.split("torch==2.14.1+cpu")[0]
    assert f"    --hash=sha256:{'d' * 64}" in result.split("torchvision==0.29.1+cpu")[0]
    assert result.count("torch==2.14.1+cpu ; sys_platform != 'darwin'") == 1
