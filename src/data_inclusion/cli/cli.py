import logging
from pathlib import Path

import click

from data_inclusion import settings
from data_inclusion.tasks import (
    constants,
    extract,
    geocoding,
    load,
    reshape,
    services,
    siretisation,
    validate,
)

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
def cli(verbose: int):
    "data-inclusion cli"
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])


@cli.command(name="process")
@click.argument("src", type=click.STRING)
@click.option(
    "--src-type",
    type=click.Choice(list(constants.SourceType)),
    show_default=True,
)
@click.option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
)
def process(
    src: str,
    src_type: constants.SourceType,
    dry_run: bool,
):
    """ETL a given source to data-inclusion."""
    services.full_processing(
        src=src,
        src_type=src_type,
        geocoding_backend=geocoding.BaseAdresseNationaleBackend(
            base_url=settings.BAN_API_URL
        ),
        dry_run=dry_run,
    )


@cli.command(name="extract")
@click.argument("src", type=click.STRING)
@click.option(
    "--src-type",
    type=click.Choice(list(constants.SourceType)),
    show_default=True,
)
def _extract(
    src: str,
    src_type: constants.SourceType,
):
    """Extract data from a given source type and path."""
    extract.extract(src=src, src_type=src_type)


@cli.command(name="reshape")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True),
)
@click.option(
    "--src-type",
    type=click.Choice(list(constants.SourceType)),
    show_default=True,
)
def _reshape(
    filepath: str,
    src_type: constants.SourceType,
):
    """Reshape a data file that should be structured in the data.inclusion format."""
    reshape.reshape(path=Path(filepath), src_type=src_type)


@cli.command(name="geocode")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True),
)
def geocode(
    filepath: str,
):
    "Geocode a data file that should be structured in the data.inclusion format."
    geocoding.geocode_normalized_data(
        path=Path(filepath),
        geocoding_backend=geocoding.BaseAdresseNationaleBackend(
            base_url=settings.BAN_API_URL
        ),
    )


@cli.command(name="siretize")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True),
)
def siretize(
    filepath: str,
):
    """Siretize a data file that should be structured in the data.inclusion format."""
    siretisation.siretize_normalized_data(path=Path(filepath))


@cli.command(name="validate")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True),
)
def _validate(
    filepath: str,
):
    """Validate a data file that should be structured in the data.inclusion format."""
    validate.validate_normalized_data(path=Path(filepath))


@cli.command(name="load")
@click.argument(
    "filepath",
    type=click.Path(exists=True, readable=True),
)
def _load(
    filepath: str,
):
    """Load a data file that structured and validated to data.inclusion."""
    load.load_data(path=Path(filepath))
