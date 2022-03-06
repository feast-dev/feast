# This script will bump the versions found in files (charts, pom.xml) during the Feast release process.

import pathlib
import sys

USAGE = f"Usage: python {sys.argv[0]} [--help] | current_semver_version new_semver_version]"
VERSIONS_TO_BUMP = 26


def main() -> None:
    args = sys.argv[1:]
    if not args or len(args) != 2:
        raise SystemExit(USAGE)

    current_version = args[0].strip()
    new_version = args[1].strip()

    if current_version == new_version:
        raise SystemExit(f"Current and new versions are the same: {current_version} == {new_version}")

    # Validate that the input arguments are semver versions
    if not is_semantic_version(current_version):
        raise SystemExit(f"Current version is not a valid semantic version: {current_version}")

    if not is_semantic_version(new_version):
        raise SystemExit(f"New version is not a valid semantic version: {new_version}")

    # Get git repo root directory
    repo_root = pathlib.Path(__file__).resolve().parent.parent.parent.parent
    path_to_file_list = repo_root.joinpath("infra", "scripts", "release", "files_to_bump.txt")

    # Get files to bump versions within
    with open(path_to_file_list, "r") as f:
        files_to_bump = f.read().splitlines()

    # The current version should be 0.18.0 or 0.19.0 or 0.20.0 etc, but we should also make sure to support the
    # occasional patch release on the master branch like 0.18.1 or 0.18.2
    versions_in_files = 0
    if current_version[-2:] != ".0":
        print(current_version[-2:])
        versions_in_files = count_version(current_version, files_to_bump, repo_root)
        if versions_in_files != VERSIONS_TO_BUMP:
            raise SystemExit(f"Found {versions_in_files} occurrences of {current_version} in files to bump, but "
                             f"expected {VERSIONS_TO_BUMP}")
    else:
        found = False

        # Lets make sure the files don't contain a patch version (e.g, 0.x.0 -> 0.x.20)
        for patch_version in range(0, 20):
            current_version_patch = current_version[:-1] + str(patch_version)
            versions_in_files = count_version(current_version_patch, files_to_bump, repo_root)

            # We are using a patch version, let's change our version number
            if versions_in_files == VERSIONS_TO_BUMP:
                print(f"Found {versions_in_files} occurrences of {current_version_patch}, changing current version to "
                      f"{current_version_patch}")
                current_version = current_version_patch
                found = True
                break
        if not found:
            raise SystemExit(f"Could not find {VERSIONS_TO_BUMP} versions of {current_version} in {path_to_file_list}")

    print(f"Found {versions_in_files} occurrences of {current_version} in files to bump {path_to_file_list}")

    # Bump the version in the files
    updated_count = 0
    for file in files_to_bump:
        with open(repo_root.joinpath(file), "r") as f:
            file_contents = f.read()
            file_contents = file_contents.replace(current_version, new_version)

        with open(repo_root.joinpath(file), "w") as f:
            f.write(file_contents)
            updated_count += 1

    print(f"Updated {updated_count} files with new version {new_version}")


def is_semantic_version(version: str) -> bool:
    components = version.split(".")
    if len(components) != 3:
        return False
    for component in components:
        if not component.isdigit():
            return False
    return True


def count_version(current_version, files_to_bump, repo_root):
    # Count how many of the existing versions we find
    total = 0
    for file in files_to_bump:
        with open(repo_root.joinpath(file), "r") as f:
            file_contents = f.read()
            total += file_contents.count(current_version)
    return total


if __name__ == "__main__":
    main()
