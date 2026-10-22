import logging
import subprocess
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestDependencyConflicts:
    def test_install_kserve_with_feast(self):
        """Resolve KServe against the environment Feast is installed in.

        ``--dry-run`` makes pip resolve the full dependency set and report what
        it would install, without installing anything. Resolution is the part
        this test cares about: pip exits non-zero when the versions cannot be
        satisfied together, which is the conflict being guarded against.

        Installing for real would mutate the interpreter running the suite.
        Feast pins ``psutil==5.9.0`` while KServe requires ``psutil>=5.9.6``, so
        pip has to uninstall psutil before reinstalling it, and the unit suite
        runs under ``pytest -n 8`` against a single environment. Any test that
        imports psutil during that window fails, including every test that
        shells out to the CLI, since ``feast.metrics`` imports it at module
        scope. It also left the environment inconsistent for the next run.
        """
        command = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--dry-run",
            "kserve==0.15.2",
        ]

        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        exit_code = process.returncode

        out = stdout.decode()
        err = stderr.decode()

        logger.debug(out)
        logger.debug(err)

        # pip reports an unsatisfiable set on stdout and exits non-zero, so the
        # exit code is the assertion that matters; the message is for the
        # failure output.
        assert exit_code == 0, (
            f"pip could not resolve kserve==0.15.2 against the current "
            f"environment:\n{out}\n{err}"
        )
