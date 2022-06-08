import enum
import logging
from typing import Optional

import numpy as np
import pandas as pd

from data_inclusion.tasks import load, validate
from data_inclusion.tasks.transform import dora

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
        df = pd.read_csv(src).replace(["", np.nan], None)

    return df.set_index("id")


def process_inclusion_dataset(
    src: str,
    di_api_url: Optional[str] = None,
    src_type: SourceType = SourceType.STANDARD,
    format: DataFormat = DataFormat.JSON,
    error_output_path: Optional[str] = None,
    di_api_token: Optional[str] = None,
):
    logger.info("Extraction...")
    df = extract(src=src, format=format)

    logger.info("Transformation...")
    df = TRANSFORM_TASKS[src_type](df)

    logger.info("Validation...")
    df, errors_df = validate.validate(df)

    if di_api_url is not None:
        logger.info("Versement...")
        load.load_to_data_inclusion(
            df.loc[df.is_valid].drop(columns=["is_valid"]),
            api_url=di_api_url,
            api_token=di_api_token,
        )

    if error_output_path is not None:
        errors_df.to_csv(error_output_path)
