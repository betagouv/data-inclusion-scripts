import enum
import logging

import pandas as pd

from data_inclusion.core.validation import validate_schema


logger = logging.getLogger(__name__)


class DataFormat(str, enum.Enum):
    CSV = "csv"
    JSON = "json"


def process(source_label: str, format: DataFormat, url: str):
    # extraction
    if format == DataFormat.JSON:
        df = pd.read_json(url)
    else:
        df = pd.read_csv(url)

    # validation
    if not validate_schema(source_label=source_label, df=df):
        logger.info("Les données ne respectent pas le schéma de l'inclusion")
        return
    else:
        logger.info("Les données sont conformes.")

    # TODO(vmttn): envoi sur data inclusion
