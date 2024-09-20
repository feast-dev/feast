import os
import sys
import glob
import subprocess
from pathlib import Path

repo_root = str(Path(__file__).resolve().parent)

PROTO_SUBDIRS = ["core", "registry", "serving", "types", "storage"]
PYTHON_CODE_PREFIX = "sdk/python"

class BuildPythonProtosCommand:
    description = "Builds the proto files into Python files."
    user_options = [
        ("inplace", "i", "Write generated proto files to source directory."),
    ]

    def __init__(self):
        self.python_protoc = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
        ]
        self.proto_folder = "protos"
        self.sub_folders = PROTO_SUBDIRS
        self.inplace = 0

    @property
    def python_folder(self):
        return "sdk/python/feast/protos"

    def _generate_python_protos(self, path: str):
        proto_files = glob.glob(os.path.join(self.proto_folder, path))
        Path(self.python_folder).mkdir(parents=True, exist_ok=True)
        subprocess.check_call(
            self.python_protoc
            + [
                "-I",
                self.proto_folder,
                "--python_out",
                self.python_folder,
                "--grpc_python_out",
                self.python_folder,
                "--mypy_out",
                self.python_folder,
            ]
            + proto_files
        )

    def run(self):
        for sub_folder in self.sub_folders:
            self._generate_python_protos(f"feast/{sub_folder}/*.proto")
            # We need the __init__ files for each of the generated subdirs
            # so that they are regular packages, and don't need the `--namespace-packages` flags
            # when being typechecked using mypy.
            with open(f"{self.python_folder}/feast/{sub_folder}/__init__.py", "w"):
                pass

        with open(f"{self.python_folder}/__init__.py", "w"):
            pass
        with open(f"{self.python_folder}/feast/__init__.py", "w"):
            pass

        for path in Path(self.python_folder).rglob("*.py"):
            for folder in self.sub_folders:
                # Read in the file
                with open(path, "r") as file:
                    filedata = file.read()

                # Replace the target string
                filedata = filedata.replace(
                    f"from feast.{folder}", f"from feast.protos.feast.{folder}"
                )

                # Write the file out again
                with open(path, "w") as file:
                    file.write(filedata)

if __name__ == "__main__":
    BuildPythonProtosCommand().run()