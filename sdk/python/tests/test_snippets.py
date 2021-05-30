import pathlib
import subprocess

import docker


def test_script():
    """Tests all documentation snippets generated from docsource/"""
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

    doctests_dir = repo_root / "sdk" / "python" / "tests" / "doctests"

    for doctest_dir in [f for f in doctests_dir.iterdir() if f.is_dir()]:
        client = docker.from_env()
        container = client.containers.run(
            image="feastdev/feast-docs-ci",
            command='/bin/bash -c "'
                    'cp -r /mnt/feast/ /feast/ && '
                    'cd /feast/ && '
                    'make install-python-ci-dependencies && '
                    f'cd /feast/sdk/python/tests/doctests/{doctest_dir.name} && '
                    './test_script.sh"',
            volumes={repo_root: {"bind": "/mnt/feast", "mode": "ro"}},
            detach=True,
        )
        for line in container.logs(stream=True):
            print(str(line.strip().decode("utf-8")))

        result = container.wait()
        exit(result["StatusCode"])
