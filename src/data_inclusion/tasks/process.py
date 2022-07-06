import enum
import logging
from typing import Optional

import numpy as np
import pandas as pd

from data_inclusion.tasks import dora, geocoding, itou, load, validate

logger = logging.getLogger(__name__)


class SourceType(str, enum.Enum):
    """Les types de source gérés.

    Liste les différents classes de source rencontrées afin de distinguer leurs
    traitements.

    * DORA: données issues de DORA.
    * ITOU: données issues d'ITOU.
    * V0: source générique respectant le schéma de l'inclusion en version v0.
    """

    DORA = "dora"
    ITOU = "itou"
    V0 = "v0"


class DataFormat(str, enum.Enum):
    CSV = "csv"
    JSON = "json"


def extract(src: str, format: DataFormat) -> pd.DataFrame:
    if format == DataFormat.JSON:
        df = pd.read_json(src, dtype=False)
    else:
        df = pd.read_csv(src).replace(["", np.nan], None)

    return df


def preprocess_itou_datasource(
    src: str,
    format: DataFormat = DataFormat.JSON,
) -> pd.DataFrame:
    logger.info("Extraction...")
    df = itou.extract_data(src)
    logger.info("Transformation...")
    return itou.transform_data(df)


def preprocess_dora_datasource(
    src: str,
    format: DataFormat = DataFormat.JSON,
) -> pd.DataFrame:
    logger.info("Extraction...")
    df = extract(src=src, format=format)
    logger.info("Transformation...")
    return dora.transform_data(df)


def preprocess_generic_v0_datasource(
    src: str,
    format: DataFormat = DataFormat.JSON,
) -> pd.DataFrame:
    logger.info("Extraction...")
    return extract(src=src, format=format)


PREPROCESS_BY_SOURCE_TYPE = {
    SourceType.DORA: preprocess_dora_datasource,
    SourceType.ITOU: preprocess_itou_datasource,
    SourceType.V0: preprocess_generic_v0_datasource,
}


def preprocess_datasource(
    src: str,
    src_type: SourceType = SourceType.V0,
    format: DataFormat = DataFormat.JSON,
) -> pd.DataFrame:
    return PREPROCESS_BY_SOURCE_TYPE[src_type](src=src, format=format).replace(
        np.nan, None
    )


def validate_normalized_dataset(
    filepath: str,
    error_output_path: Optional[str] = None,
):
    df = pd.read_json(filepath, dtype=False).replace(np.nan, None)
    _, errors_df = validate.validate(df)

    if error_output_path is not None:
        errors_df.to_csv(error_output_path)


def process_datasource(
    geocoding_backend: geocoding.GeocodingBackend,
    src: str,
    src_type: SourceType = SourceType.V0,
    format: DataFormat = DataFormat.JSON,
    error_output_path: Optional[str] = None,
    dry_run: bool = False,
):
    df = preprocess_datasource(src=src, src_type=src_type, format=format)

    logger.info("Geocodage...")
    df = geocoding.geocode_normalized_dataset(df, geocoding_backend=geocoding_backend)

    logger.info("Validation...")
    df, errors_df = validate.validate(df)

    if error_output_path is not None:
        errors_df.to_csv(error_output_path)

    if not dry_run:
        logger.info("Versement...")
        load.load_to_data_inclusion(df.loc[df.is_valid].drop(columns=["is_valid"]))


def geocode(
    filepath: str,
    geocoding_backend: geocoding.GeocodingBackend,
) -> pd.DataFrame:
    df = pd.read_json(filepath, dtype=False).replace(np.nan, None)
    logger.info("Geocodage...")
    return geocoding.geocode_normalized_dataset(df, geocoding_backend=geocoding_backend)
