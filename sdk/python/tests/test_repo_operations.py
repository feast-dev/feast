from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Optional

import assertpy

from feast.repo_operations import get_ignore_files, get_repo_files, read_feastignore


@contextmanager
def feature_repo(feastignore_contents: Optional[str]):
    with TemporaryDirectory() as tmp_dir:
        repo_root = Path(tmp_dir)
        (repo_root / "foo").mkdir()
        (repo_root / "foo1").mkdir()
        (repo_root / "foo1/bar").mkdir()
        (repo_root / "bar").mkdir()
        (repo_root / "bar/subdir1").mkdir()
        (repo_root / "bar/subdir1/subdir2").mkdir()

        (repo_root / "a.py").touch()
        (repo_root / "foo/b.py").touch()
        (repo_root / "foo1/c.py").touch()
        (repo_root / "foo1/bar/d.py").touch()
        (repo_root / "bar/e.py").touch()
        (repo_root / "bar/subdir1/f.py").touch()
        (repo_root / "bar/subdir1/subdir2/g.py").touch()

        if feastignore_contents:
            with open(repo_root / ".feastignore", "w") as f:
                f.write(feastignore_contents)

        yield repo_root


def test_feastignore_no_file():
    # Tests feature repo without .feastignore file
    with feature_repo(None) as repo_root:
        assertpy.assert_that(read_feastignore(repo_root)).is_equal_to([])
        assertpy.assert_that(get_ignore_files(repo_root, [])).is_equal_to(set())
        assertpy.assert_that(get_repo_files(repo_root)).is_equal_to(
            [
                (repo_root / "a.py").resolve(),
                (repo_root / "bar/e.py").resolve(),
                (repo_root / "bar/subdir1/f.py").resolve(),
                (repo_root / "bar/subdir1/subdir2/g.py").resolve(),
                (repo_root / "foo/b.py").resolve(),
                (repo_root / "foo1/bar/d.py").resolve(),
                (repo_root / "foo1/c.py").resolve(),
            ]
        )


def test_feastignore_no_stars():
    # Tests .feastignore that doesn't contain "*" in paths
    feastignore_contents = dedent(
        """
        # We can put some comments here

        foo # match directory
        bar/subdir1/f.py # match specific file
    """
    )
    with feature_repo(feastignore_contents) as repo_root:
        ignore_paths = ["foo", "bar/subdir1/f.py"]
        assertpy.assert_that(read_feastignore(repo_root)).is_equal_to(ignore_paths)
        assertpy.assert_that(get_ignore_files(repo_root, ignore_paths)).is_equal_to(
            {
                (repo_root / "foo/b.py").resolve(),
                (repo_root / "bar/subdir1/f.py").resolve(),
            }
        )
        assertpy.assert_that(get_repo_files(repo_root)).is_equal_to(
            [
                (repo_root / "a.py").resolve(),
                (repo_root / "bar/e.py").resolve(),
                (repo_root / "bar/subdir1/subdir2/g.py").resolve(),
                (repo_root / "foo1/bar/d.py").resolve(),
                (repo_root / "foo1/c.py").resolve(),
            ]
        )


def test_feastignore_with_stars():
    # Tests .feastignore that contains "*" and "**" in paths
    feastignore_contents = dedent(
        """
        foo/*.py # match python files directly under foo/
        bar/**   # match everything (recursively) under bar/
        */c.py   # match c.py in any directory
        */d.py   # match d.py in any directory (this shouldn't match anything)
    """
    )
    with feature_repo(feastignore_contents) as repo_root:
        ignore_paths = ["foo/*.py", "bar/**", "*/c.py", "*/d.py"]
        assertpy.assert_that(read_feastignore(repo_root)).is_equal_to(ignore_paths)
        assertpy.assert_that(get_ignore_files(repo_root, ignore_paths)).is_equal_to(
            {
                (repo_root / "foo/b.py").resolve(),
                (repo_root / "bar/subdir1/f.py").resolve(),
                (repo_root / "bar/e.py").resolve(),
                (repo_root / "bar/subdir1/f.py").resolve(),
                (repo_root / "bar/subdir1/subdir2/g.py").resolve(),
                (repo_root / "foo1/c.py").resolve(),
            }
        )
        assertpy.assert_that(get_repo_files(repo_root)).is_equal_to(
            [(repo_root / "a.py").resolve(), (repo_root / "foo1/bar/d.py").resolve()]
        )


def test_feastignore_with_stars2():
    # Another test of .feastignore that contains "**" in paths
    feastignore_contents = dedent(
        """
        # match everything (recursively) that has "bar" in its path
        **/bar/**
    """
    )
    with feature_repo(feastignore_contents) as repo_root:
        ignore_paths = ["**/bar/**"]
        assertpy.assert_that(read_feastignore(repo_root)).is_equal_to(ignore_paths)
        assertpy.assert_that(get_ignore_files(repo_root, ignore_paths)).is_equal_to(
            {
                (repo_root / "bar/subdir1/f.py").resolve(),
                (repo_root / "bar/e.py").resolve(),
                (repo_root / "bar/subdir1/f.py").resolve(),
                (repo_root / "bar/subdir1/subdir2/g.py").resolve(),
                (repo_root / "foo1/bar/d.py").resolve(),
            }
        )
        assertpy.assert_that(get_repo_files(repo_root)).is_equal_to(
            [
                (repo_root / "a.py").resolve(),
                (repo_root / "foo/b.py").resolve(),
                (repo_root / "foo1/c.py").resolve(),
            ]
        )
