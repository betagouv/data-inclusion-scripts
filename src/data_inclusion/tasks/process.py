import enum
import logging
from typing import Optional

import pandas as pd

from data_inclusion.tasks.transform import dora
from data_inclusion.tasks import load, validate


logger = logging.getLogger(__name__)


class SourceType(str, enum.Enum):
    """Les types de source gérés.

    Liste les différents classes de source rencontrées afin de distinguer leurs
    traitements.

    * STANDARD: source générique respectant le schéma de l'insertion.
    * DORA: données issues de DORA et nécéssitant une normalisation.
    """

    DORA = "dora"
    STANDARD = "standard"


class DataFormat(str, enum.Enum):
    CSV = "csv"
    JSON = "json"


TRANSFORM_TASKS = {
    SourceType.DORA: dora.transform_data,
    SourceType.STANDARD: lambda x: x,  # noop
}


def extract(src: str, format: DataFormat) -> pd.DataFrame:
    if format == DataFormat.JSON:
        df = pd.read_json(src, dtype=False)
    else:
        df = pd.read_csv(src)

    return df


def process_inclusion_dataset(
    src: str,
    di_api_url: Optional[str] = None,
    src_type: SourceType = SourceType.STANDARD,
    format: DataFormat = DataFormat.JSON,
):
    # extraction
    df = extract(src=src, format=format)

    # transformation
    df = TRANSFORM_TASKS[src_type](df)

    # validation
    if not validate.validate_schema(df=df, src_name=src):
        logger.info("Les données ne respectent pas le schéma de l'inclusion")
        return
    else:
        logger.info("Les données sont conformes.")

    if di_api_url is None:
        return

    # versement
    load.load_to_data_inclusion(df, api_url=di_api_url)
