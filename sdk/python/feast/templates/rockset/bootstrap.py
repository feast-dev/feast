import click

from feast.file_utils import replace_str_in_file


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`
    import pathlib

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    config_file = repo_path / "feature_store.yaml"
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    rockset_apikey = click.prompt(
        "Rockset Api Key (If blank will be read from ROCKSET_APIKEY in ENV):",
        default="",
    )

    rockset_host = click.prompt(
        "Rockset Host (If blank will be read from ROCKSET_APISERVER in ENV):",
        default="",
    )

    replace_str_in_file(config_file, "ROCKSET_APIKEY", rockset_apikey)
    replace_str_in_file(config_file, "ROCKSET_APISERVER", rockset_host)


if __name__ == "__main__":
    bootstrap()
