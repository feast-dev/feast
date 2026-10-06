"""Add CPU wheel hashes to CI locks while retaining their PyPI artifacts and pins."""

from __future__ import annotations

import argparse
import re
import subprocess
import tempfile
from pathlib import Path

TORCH_REQUIREMENT = re.compile(
    r"^(torch|torchvision)==([^\s]+)[^\n]*\n(?:    --hash=[^\n]+\n)+",
    re.MULTILINE,
)


def add_cpu_hashes(requirements: str, cpu_requirements: str) -> str:
    cpu_hashes = {
        (match[1], match[2]): set(re.findall(r"sha256:[0-9a-f]{64}", match[0]))
        for match in TORCH_REQUIREMENT.finditer(cpu_requirements)
    }

    def add_hashes(match: re.Match[str]) -> str:
        name, version = match[1], match[2]
        hashes = cpu_hashes.get((name, f"{version}+cpu"))
        if not hashes:
            raise ValueError(f"Missing CPU hashes for {name}=={version}")
        hashes = hashes | set(re.findall(r"sha256:[0-9a-f]{64}", match[0]))
        header = match[0].splitlines()[0]
        return (
            header
            + "\n"
            + " \\\n".join(f"    --hash={digest}" for digest in sorted(hashes))
            + "\n"
        )

    return TORCH_REQUIREMENT.sub(add_hashes, requirements)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("requirements", type=Path)
    parser.add_argument("--python-version", required=True)
    args = parser.parse_args()
    contents = args.requirements.read_text()
    pins = [f"{match[1]}=={match[2]}" for match in TORCH_REQUIREMENT.finditer(contents)]
    if not pins:
        raise ValueError("No hashed PyTorch requirements found")
    with tempfile.TemporaryDirectory() as tmp:
        source = Path(tmp) / "torch.in"
        output = Path(tmp) / "torch.txt"
        source.write_text("\n".join(pins) + "\n")
        subprocess.run(
            [
                "uv",
                "pip",
                "compile",
                "--no-deps",
                "--torch-backend",
                "cpu",
                "--universal",
                "--python-version",
                args.python_version,
                "--generate-hashes",
                "--no-header",
                "--no-annotate",
                "--quiet",
                str(source),
                "--output-file",
                str(output),
            ],
            check=True,
        )
        updated = add_cpu_hashes(contents, output.read_text())
    args.requirements.write_text(updated)


if __name__ == "__main__":
    main()
