import yaml


def yaml_loader(yml, load_single=False):
    """
    Loads one or more Feast resources from a YAML path or string. Multiple resources
    can be divided by three hyphens '---'

    Args:
        yml: A path ending in .yaml or .yml, or a YAML string
        load_single: Expect only a single YAML resource, fail otherwise

    Returns:
        Either a single YAML dictionary or a list of YAML dictionaries

    """

    yml_content = _get_yaml_contents(yml)
    yaml_strings = yml_content.strip("---").split("---")

    # Return a single resource dict
    if load_single:
        if len(yaml_strings) > 1:
            raise Exception(
                f"More than one YAML file is being loaded when only a single file is supported: ${yaml_strings}"
            )
        return _yaml_to_dict(yaml_strings[0])

    # Return a list of resource dicts
    resources = []
    for yaml_string in yaml_strings:
        resources.append(_yaml_to_dict(yaml_string))
    return resources


def _get_yaml_contents(yml: str) -> str:
    """
    Returns the YAML contents from an object. If a path ending with .yaml or
    .yml is passed, it will be read for its contents. If a string containing
    YAML is passed, that will be returned.

    Args:
        yml: Path of YAML file or string containing YAML

    Returns:
        String object containing YAML
    """
    if (
        isinstance(yml, str)
        and yml.count("\n") == 0
        and (".yaml" in yml.lower() or ".yml" in yml.lower())
    ):
        with open(yml, "r") as f:
            yml_content = f.read()

    elif isinstance(yml, str):
        yml_content = yml
    else:
        raise Exception(
            f"Invalid YAML provided. Please provide either a file path or YAML string.\n"
            f"Provided YAML: {yml}"
        )
    return yml_content


def _yaml_to_dict(yaml_string):
    """
    Converts a yaml string to dictionary

    Args:
        yaml_string: String containing YAML

    Returns:
        Dictionary containing the same object
    """

    return yaml.safe_load(yaml_string)
