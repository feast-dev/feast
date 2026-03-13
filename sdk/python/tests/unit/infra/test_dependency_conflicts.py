import logging
import subprocess
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TestDependencyConflicts:
    def test_install_kserve_with_feast(self):
        """Test installing KServe in the current environment where Feast is already installed.
        Ensures no dependency conflict errors occur.
        """
        # Command to install KServe
        command = [sys.executable, "-m", "pip", "install", "kserve==0.15.2"]

        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        exit_code = process.returncode

        out = stdout.decode()
        err = stderr.decode()

        logger.debug(out)
        logger.debug(err)

        # Assertions
        assert exit_code == 0
        conflict_occurred = "dependency conflicts" in err and "ERROR" not in err
        assert not conflict_occurred, "Dependency conflict detected during installation"
