import logging

import click

from data_inclusion.tasks import process


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
def cli(verbose: int):
    "data-inclusion cli"
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])


@cli.command(name="validate")
@click.argument("src", type=click.STRING)
@click.option(
    "--format",
    type=click.Choice(list(process.DataFormat)),
    default=process.DataFormat.JSON.value,
    show_default=True,
)
@click.option(
    "--src-type",
    type=click.Choice(list(process.SourceType)),
    default=process.SourceType.STANDARD.value,
    show_default=True,
)
@click.option("--error-output-path", type=click.Path())
def validate(
    src: str,
    format: process.DataFormat,
    src_type: process.SourceType,
    error_output_path: str,
):
    "Extract, (transform,) and validate data from a given source"
    process.process_inclusion_dataset(
        src=src,
        src_type=src_type,
        format=format,
        error_output_path=error_output_path,
    )


@cli.command(name="import")
@click.argument("src", type=click.STRING)
@click.argument("di-api-url")
@click.option(
    "--format",
    type=click.Choice(list(process.DataFormat)),
    default=process.DataFormat.JSON.value,
    show_default=True,
)
@click.option(
    "--src-type",
    type=click.Choice(list(process.SourceType)),
    default=process.SourceType.STANDARD.value,
    show_default=True,
)
@click.option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
)
@click.option("--error-output-path", type=click.Path())
def import_(
    src: str,
    di_api_url: str,
    format: process.DataFormat,
    src_type: process.SourceType,
    dry_run: bool,
    error_output_path: str,
):
    "Extract, (transform,) validate and load data from a given source to data-inclusion"
    process.process_inclusion_dataset(
        src=src,
        di_api_url=di_api_url if not dry_run else None,
        src_type=src_type,
        format=format,
        error_output_path=error_output_path,
    )
