import click

tagsOption = click.option(
    "--tags",
    help="Filter by tags (e.g. --tags 'key:value' --tags 'key:value, key:value, ...'). Items return when ALL tags match.",
    default=[""],
    multiple=True,
)
