import logging

import click

from data_inclusion import settings
from data_inclusion.tasks import geocoding, process

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
def cli(verbose: int):
    "data-inclusion cli"
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])


@cli.command(name="preprocess")
@click.argument("src", type=click.STRING)
@click.argument("output-path", type=click.Path())
@click.option(
    "--format",
    type=click.Choice(list(process.DataFormat)),
    default=process.DataFormat.JSON.value,
    show_default=True,
)
@click.option(
    "--src-type",
    type=click.Choice(list(process.SourceType)),
    default=process.SourceType.V0.value,
    show_default=True,
)
def preprocess(
    src: str,
    format: process.DataFormat,
    src_type: process.SourceType,
    output_path: str,
):
    "Extract from the given datasource and reshape the data to data.inclusion format."
    df = process.preprocess_datasource(
        src=src,
        src_type=src_type,
        format=format,
    )

    logger.info(f"Writing preprocessed file to {output_path}")
    df.to_json(output_path, orient="records")


@cli.command(name="validate")
@click.argument("filepath", type=click.Path(exists=True, readable=True))
@click.option("--error-output-path", type=click.Path())
def validate(
    filepath: str,
    error_output_path: str,
):
    "Validate a data file that should be structured in the data.inclusion format."
    process.validate_normalized_dataset(
        filepath=filepath,
        error_output_path=error_output_path,
    )


@cli.command(name="import")
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
    default=process.SourceType.V0.value,
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
    format: process.DataFormat,
    src_type: process.SourceType,
    dry_run: bool,
    error_output_path: str,
):
    "Extract, (transform,) validate and load data from a given source to data-inclusion"
    process.process_datasource(
        src=src,
        src_type=src_type,
        format=format,
        error_output_path=error_output_path,
        dry_run=dry_run,
        geocoding_backend=geocoding.BaseAdresseNationaleBackend(
            base_url=settings.BAN_API_URL
        ),
    )
