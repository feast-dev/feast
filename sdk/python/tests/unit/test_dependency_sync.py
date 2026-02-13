import ast
import pathlib
import pytest
from packaging.requirements import Requirement

try:
    import tomllib  
except ModuleNotFoundError:
    import tomli as tomllib


ROOT = pathlib.Path(__file__).parent


def normalize(dep_list):
    normalized = set()
    for dep in dep_list:
        try:
            req = Requirement(dep)
            normalized.add(str(req))
        except Exception:
            normalized.add(dep.strip())
    return normalized


# -------------------------------------------------
# Parsing pyproject.toml
# -------------------------------------------------
def parse_pyproject():
    pyproject_path = ROOT / "pyproject.toml"

    if not pyproject_path.exists():
        return set(), {}

    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)

    core = set()
    optional = {}

    
    if "project" in data:
        project = data["project"]
        core = set(project.get("dependencies", []))
        optional = project.get("optional-dependencies", {})

    
    elif "tool" in data and "poetry" in data["tool"]:
        poetry = data["tool"]["poetry"]
        deps = poetry.get("dependencies", {})

        for name, version in deps.items():
            if name == "python":
                continue
            if isinstance(version, str):
                core.add(f"{name}{version}")
            else:
                core.add(name)

        optional = poetry.get("extras", {})

    return normalize(core), {k: normalize(v) for k, v in optional.items()}


def extract_list(node):
    if isinstance(node, ast.List):
        return {
            elt.value for elt in node.elts
            if isinstance(elt, ast.Constant)
        }
    return set()


def extract_dict(node):
    result = {}
    if isinstance(node, ast.Dict):
        for k, v in zip(node.keys, node.values):
            if isinstance(k, ast.Constant):
                result[k.value] = extract_list(v)
    return result


def parse_setup():
    setup_path = ROOT / "setup.py"

    if not setup_path.exists():
        return set(), {}

    tree = ast.parse(setup_path.read_text())

    variables = {}

    for node in tree.body:
        if isinstance(node, ast.Assign):
            if isinstance(node.targets[0], ast.Name):
                name = node.targets[0].id
                value = node.value

                if isinstance(value, ast.List):
                    variables[name] = extract_list(value)

                elif isinstance(value, ast.Dict):
                    variables[name] = extract_dict(value)

    install_requires = set()
    extras_require = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "setup":
            for keyword in node.keywords:

                if keyword.arg == "install_requires":
                    if isinstance(keyword.value, ast.List):
                        install_requires = extract_list(keyword.value)
                    elif isinstance(keyword.value, ast.Name):
                        install_requires = variables.get(
                            keyword.value.id, set()
                        )

                if keyword.arg == "extras_require":
                    if isinstance(keyword.value, ast.Dict):
                        extras_require = extract_dict(keyword.value)
                    elif isinstance(keyword.value, ast.Name):
                        extras_require = variables.get(
                            keyword.value.id, {}
                        )

    return normalize(install_requires), {
        k: normalize(v) for k, v in extras_require.items()
    }


# -------------------------------------------------
# Test
# -------------------------------------------------
def test_dependencies_in_sync():
    py_core, py_optional = parse_pyproject()

    # If pyproject does not define dependencies, skip test
    # Manually Checked the package dependencies are not present 
    if not py_core and not py_optional:
        pytest.skip("pyproject.toml does not define dependencies")

    setup_core, setup_optional = parse_setup()

    core_missing = py_core - setup_core
    core_extra = setup_core - py_core

    assert not core_missing and not core_extra, (
        f"\nCore dependency mismatch:\n"
        f"Missing in setup.py: {core_missing}\n"
        f"Extra in setup.py: {core_extra}"
    )

    py_groups = set(py_optional.keys())
    setup_groups = set(setup_optional.keys())

    assert py_groups == setup_groups, (
        f"\nOptional group mismatch:\n"
        f"Only in pyproject.toml: {py_groups - setup_groups}\n"
        f"Only in setup.py: {setup_groups - py_groups}"
    )

    for group in py_groups:
        missing = py_optional[group] - setup_optional[group]
        extra = setup_optional[group] - py_optional[group]

        assert not missing and not extra, (
            f"\nMismatch in optional group '{group}':\n"
            f"Missing in setup.py: {missing}\n"
            f"Extra in setup.py: {extra}"
        )

