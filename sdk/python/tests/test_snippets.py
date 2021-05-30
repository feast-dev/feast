import pathlib
import subprocess

import docker
import pytest

repo_root = pathlib.Path(
    (
        subprocess.Popen(
            ["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE
        )
        .communicate()[0]
        .rstrip()
        .decode("utf-8")
    )
)


def get_doctest_dirs():
    doctests_dir = repo_root / "sdk" / "python" / "tests" / "doctests"
    return [f.name for f in doctests_dir.iterdir() if f.is_dir()]


@pytest.mark.parametrize("doctests_dir", get_doctest_dirs())
@pytest.mark.integration
def test_snippets(doctests_dir):
    """Tests all documentation snippets generated from docgen/"""

    client = docker.from_env()
    container = client.containers.run(
        image="feastdev/feast-docs-ci",
        command='/bin/bash -c "'
        "cp -r /mnt/feast/ /feast/ && "
        "cd /feast/ && "
        "make install-python-ci-dependencies && "
        f"cd /feast/sdk/python/tests/doctests/{doctests_dir} && "
        './test_script.sh"',
        volumes={repo_root: {"bind": "/mnt/feast", "mode": "ro"}},
        detach=True,
    )
    for line in container.logs(stream=True):
        print(str(line.strip().decode("utf-8")))

    result = container.wait()
    if result["StatusCode"] > 0:
        exit(result["StatusCode"])
