import yaml


def yaml_loader(yml, load_single=False):
    """
    Loads one or more Feast resources from a YAML path or string. Multiple resources
    can be divided by three hyphens '---'
    :param yml: A path ending in .yaml or .yml, or a YAML string
    :param load_single: Expect only a single YAML resource, fail otherwise
    :return: Either a single YAML dictionary or a list of YAML dictionaries
    """
    if (
        isinstance(yml, str)
        and yml.count("\n") == 0
        and (".yaml" in yml.lower() or ".yml" in yml.lower())
    ):
        with open(yml, "r") as f:
            yml_content = f.read()

    elif isinstance(yml, str) and "kind" in yml.lower():
        yml_content = yml
    else:
        raise Exception(
            f"Invalid YAML provided. Please provide either a file path or YAML string: ${yml}"
        )

    yaml_strings = yml_content.strip("---").split("---")

    # Return a single resource dict
    if load_single:
        if len(yaml_strings) > 1:
            raise Exception(
                f"More than one YAML file is being loaded when only a single file is supported: ${yaml_strings}"
            )
        return yaml_to_dict(yaml_strings[0])

    # Return a list of resource dicts
    resources = []
    for yaml_string in yaml_strings:
        resources.append(yaml_to_dict(yaml_string))
    return resources


def yaml_to_dict(yaml_string):
    yaml_dict = yaml.safe_load(yaml_string)
    if not isinstance(yaml_dict, dict) or not "kind" in yaml_dict:
        raise Exception(f"Could not detect YAML kind from resource: ${yaml_string}")
    return yaml_dict
