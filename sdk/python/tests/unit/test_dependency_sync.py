# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Test to verify that dependencies are in sync between pyproject.toml and setup.py.

Both files list the same dependencies but are maintained separately. This test ensures
they stay synchronized to avoid subtle bugs when one file is updated but the other isn't.
"""

import ast
import importlib
import re
from pathlib import Path
from typing import Dict, List, Set

import pytest

try:
    import tomllib as toml
except ImportError:
    toml = importlib.import_module("tomli")


def get_repo_root() -> Path:
    """Get the repository root directory."""
    # Starting from this test file, go up to the repo root
    current = Path(__file__).resolve()
    while current != current.parent:
        if (current / "pyproject.toml").exists() and (current / "setup.py").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find repository root")


def normalize_dependency(dep: str) -> str:
    """
    Normalize a dependency specification for comparison.
    
    This handles:
    - Removing extra whitespace
    - Lowercasing package names (case-insensitive per PEP 503)
    - Preserving version specifiers and extras
    """
    # Split on semicolon to handle environment markers
    parts = dep.split(";", 1)
    main_dep = parts[0].strip()
    marker = parts[1].strip() if len(parts) > 1 else None
    
    # Extract package name and rest
    # Match: package[extras]>=version or package>=version
    match = re.match(r'^([a-zA-Z0-9_-]+)(\[.*?\])?(.*)$', main_dep)
    if not match:
        # If it doesn't match expected pattern, just normalize whitespace
        normalized = re.sub(r'\s+', '', main_dep).lower()
        if marker:
            normalized = f"{normalized}; {marker}"
        return normalized
    
    pkg_name, extras, version_spec = match.groups()
    pkg_name = pkg_name.lower()
    extras = extras if extras else ""
    version_spec = re.sub(r'\s+', '', version_spec) if version_spec else ""
    
    normalized = f"{pkg_name}{extras}{version_spec}"
    if marker:
        # Normalize marker spacing
        marker = re.sub(r'\s+', ' ', marker)
        normalized = f"{normalized}; {marker}"
    
    return normalized


def parse_pyproject_toml() -> Dict[str, List[str]]:
    """Parse dependencies from pyproject.toml."""
    repo_root = get_repo_root()
    pyproject_path = repo_root / "pyproject.toml"
    
    with open(pyproject_path, "rb") as f:
        data = toml.load(f)
    
    result = {}
    
    # Core dependencies
    core_deps = data.get("project", {}).get("dependencies", [])
    result["core"] = sorted([normalize_dependency(dep) for dep in core_deps])
    
    # Optional dependencies
    optional_deps = data.get("project", {}).get("optional-dependencies", {})
    for group_name, deps in optional_deps.items():
        # Skip composite groups that reference other extras (like 'dev', 'ci', 'nlp', etc.)
        # BUT extract the pure dependencies from groups that have both feast[] refs and regular deps
        filtered_deps = [dep for dep in deps if not dep.startswith("feast[")]
        if not filtered_deps:
            # Skip if all deps are feast[] references
            continue
        result[group_name] = sorted([normalize_dependency(dep) for dep in filtered_deps])
    
    return result


def parse_setup_py() -> Dict[str, List[str]]:
    """Parse dependencies from setup.py by evaluating variable assignments."""
    repo_root = get_repo_root()
    setup_path = repo_root / "setup.py"
    
    with open(setup_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Parse the AST
    tree = ast.parse(content)
    
    # Extract variable assignments
    variables = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    var_name = target.id
                    # We only care about dependency lists
                    if "_REQUIRED" in var_name or var_name in ["REQUIRED", "OPENTELEMETRY"]:
                        try:
                            # Evaluate the value
                            value = ast.literal_eval(node.value)
                            if isinstance(value, list):
                                variables[var_name] = value
                        except (ValueError, TypeError):
                            # Skip complex expressions we can't evaluate
                            pass
    
    result = {}
    
    # Map setup.py variable names to pyproject.toml extra names
    mapping = {
        "REQUIRED": "core",
        "AWS_REQUIRED": "aws",
        "REDIS_REQUIRED": "redis",
        "GCP_REQUIRED": "gcp",
        "KUBERNETES_REQUIRED": "k8s",
        "SNOWFLAKE_REQUIRED": "snowflake",
        "SPARK_REQUIRED": "spark",
        "SQLITE_VEC_REQUIRED": "sqlite_vec",
        "TRINO_REQUIRED": "trino",
        "POSTGRES_REQUIRED": "postgres",
        "POSTGRES_C_REQUIRED": "postgres-c",
        "OPENTELEMETRY": "opentelemetry",
        "OPENLINEAGE_REQUIRED": "openlineage",
        "MYSQL_REQUIRED": "mysql",
        "HBASE_REQUIRED": "hbase",
        "CASSANDRA_REQUIRED": "cassandra",
        "GE_REQUIRED": "ge",
        "AZURE_REQUIRED": "azure",
        "IKV_REQUIRED": "ikv",
        "HAZELCAST_REQUIRED": "hazelcast",
        "IBIS_REQUIRED": "ibis",
        "GRPCIO_REQUIRED": "grpcio",
        "DUCKDB_REQUIRED": "duckdb",
        "DELTA_REQUIRED": "delta",
        "DOCLING_REQUIRED": "docling",
        "ELASTICSEARCH_REQUIRED": "elasticsearch",
        "SINGLESTORE_REQUIRED": "singlestore",
        "COUCHBASE_REQUIRED": "couchbase",
        "MSSQL_REQUIRED": "mssql",
        "FAISS_REQUIRED": "faiss",
        "QDRANT_REQUIRED": "qdrant",
        "GO_REQUIRED": "go",
        "MILVUS_REQUIRED": "milvus",
        "DBT_REQUIRED": "dbt",
        "TORCH_REQUIRED": "pytorch",
        "CLICKHOUSE_REQUIRED": "clickhouse",
        "MCP_REQUIRED": "mcp",
        "RAG_REQUIRED": "rag",
        "RAY_REQUIRED": "ray",
        "SETUPTOOLS_REQUIRED": "setuptools",
        "IMAGE_REQUIRED": "image",
    }
    
    # Groups to skip in setup.py because they're composite in pyproject.toml
    skip_groups_in_comparison = {"ci", "minimal-sdist-build"}
    
    for var_name, extra_name in mapping.items():
        if var_name in variables and extra_name not in skip_groups_in_comparison:
            deps = variables[var_name]
            result[extra_name] = sorted([normalize_dependency(dep) for dep in deps])
    
    return result


def test_core_dependencies_in_sync():
    """Test that core dependencies match between pyproject.toml and setup.py."""
    pyproject_deps = parse_pyproject_toml()
    setup_deps = parse_setup_py()
    
    pyproject_core = set(pyproject_deps.get("core", []))
    setup_core = set(setup_deps.get("core", []))
    
    # Find differences
    only_in_pyproject = pyproject_core - setup_core
    only_in_setup = setup_core - pyproject_core
    
    error_messages = []
    
    if only_in_pyproject:
        error_messages.append(
            f"Dependencies in pyproject.toml but NOT in setup.py:\n  - "
            + "\n  - ".join(sorted(only_in_pyproject))
        )
    
    if only_in_setup:
        error_messages.append(
            f"Dependencies in setup.py but NOT in pyproject.toml:\n  - "
            + "\n  - ".join(sorted(only_in_setup))
        )
    
    if error_messages:
        pytest.fail(
            "Core dependencies are OUT OF SYNC!\n\n" + "\n\n".join(error_messages)
        )


def test_optional_dependencies_in_sync():
    """Test that optional dependencies match between pyproject.toml and setup.py."""
    pyproject_deps = parse_pyproject_toml()
    setup_deps = parse_setup_py()
    
    # Get all extra groups (excluding core)
    pyproject_extras = set(pyproject_deps.keys()) - {"core"}
    setup_extras = set(setup_deps.keys()) - {"core"}
    
    # Skip composite groups that are managed differently in each file
    skip_comparison = {"ci", "minimal-sdist-build", "minimal", "nlp", "dev", "docs", "image"}
    pyproject_extras = pyproject_extras - skip_comparison
    setup_extras = setup_extras - skip_comparison
    
    # Check for missing groups
    missing_in_setup = pyproject_extras - setup_extras
    missing_in_pyproject = setup_extras - pyproject_extras
    
    error_messages = []
    
    if missing_in_setup:
        error_messages.append(
            f"Extra groups in pyproject.toml but NOT in setup.py:\n  - "
            + "\n  - ".join(sorted(missing_in_setup))
        )
    
    if missing_in_pyproject:
        error_messages.append(
            f"Extra groups in setup.py but NOT in pyproject.toml:\n  - "
            + "\n  - ".join(sorted(missing_in_pyproject))
        )
    
    # Check dependencies within each common group
    common_extras = pyproject_extras & setup_extras
    for extra in sorted(common_extras):
        pyproject_group = set(pyproject_deps[extra])
        setup_group = set(setup_deps[extra])
        
        only_in_pyproject = pyproject_group - setup_group
        only_in_setup = setup_group - pyproject_group
        
        if only_in_pyproject or only_in_setup:
            msg_parts = [f"Extra group '{extra}' is OUT OF SYNC:"]
            
            if only_in_pyproject:
                msg_parts.append(
                    f"  In pyproject.toml but NOT in setup.py:\n    - "
                    + "\n    - ".join(sorted(only_in_pyproject))
                )
            
            if only_in_setup:
                msg_parts.append(
                    f"  In setup.py but NOT in pyproject.toml:\n    - "
                    + "\n    - ".join(sorted(only_in_setup))
                )
            
            error_messages.append("\n".join(msg_parts))
    
    if error_messages:
        pytest.fail(
            "Optional dependencies are OUT OF SYNC!\n\n" + "\n\n".join(error_messages)
        )


def test_dependency_parser_works():
    """Sanity check that our parsers are working correctly."""
    pyproject_deps = parse_pyproject_toml()
    setup_deps = parse_setup_py()
    
    # Both should have core dependencies
    assert "core" in pyproject_deps, "Failed to parse core deps from pyproject.toml"
    assert "core" in setup_deps, "Failed to parse core deps from setup.py"
    
    # Both should have some common extras
    assert "aws" in pyproject_deps, "Failed to parse aws extra from pyproject.toml"
    assert "aws" in setup_deps, "Failed to parse aws extra from setup.py"
    
    # Should have found at least some dependencies
    assert len(pyproject_deps["core"]) > 10, "Core deps seem too few in pyproject.toml"
    assert len(setup_deps["core"]) > 10, "Core deps seem too few in setup.py"


def test_normalization():
    """Test that dependency normalization works correctly."""
    # Test basic normalization
    assert normalize_dependency("Click>=7.0.0") == normalize_dependency("click>=7.0.0")
    assert normalize_dependency("numpy >= 2.0.0") == normalize_dependency("numpy>=2.0.0")
    
    # Test with extras
    assert normalize_dependency("SQLAlchemy[mypy]>1") == normalize_dependency("sqlalchemy[mypy]>1")
    
    # Test with environment markers
    dep1 = 'ray>=2.47.0; python_version == "3.10"'
    dep2 = 'ray>=2.47.0;python_version=="3.10"'
    # These should normalize to the same form
    norm1 = normalize_dependency(dep1)
    norm2 = normalize_dependency(dep2)
    assert "ray" in norm1 and "3.10" in norm1
    assert "ray" in norm2 and "3.10" in norm2
