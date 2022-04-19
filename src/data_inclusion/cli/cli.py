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
@click.argument("url", type=click.STRING)
def import_(format: str, url: str):
    "import data"
    service.process(
        format=format,
        url=url,
    )
