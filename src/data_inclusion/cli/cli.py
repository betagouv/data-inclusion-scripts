import logging

import click

from data_inclusion.core import service


@click.group()
@click.version_option()
@click.option("--verbose", "-v", count=True)
def cli(verbose: int):
    "data-inclusion cli"
    logging.basicConfig(level=[logging.INFO, logging.INFO, logging.DEBUG][verbose])


@cli.command(name="import")
@click.argument("format", type=click.Choice(list(service.DataFormat)))
@click.argument("source_label", type=click.STRING)
@click.argument("url", type=click.STRING)
def import_(source_label: str, format: str, url: str):
    "import data"
    service.process(
        source_label=source_label,
        format=format,
        url=url,
    )
