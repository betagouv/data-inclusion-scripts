import enum
import logging
from typing import Optional

import numpy as np
import pandas as pd

from data_inclusion.tasks import (
    cd35,
    dora,
    geocoding,
    itou,
    load,
    siretisation,
    validate,
)

logger = logging.getLogger(__name__)


class SourceType(str, enum.Enum):
    """Types of datasources.

    Different types of source (and their corresponding format) are handled.

    Extraction and transformation will vary according to the source.
    """

    # generic source which can be simply extracted (from a file or HTTP endpoint) and
    # which respects a priori the data.inclusion schema.
    V0 = "v0"

    # custom sources which requires ad hoc extraction/transformation.
    CD35 = "cd35"
    DORA = "dora"
    ITOU = "itou"


class DataFormat(str, enum.Enum):
    CSV = "csv"
    JSON = "json"


def extract(src: str, format: DataFormat) -> pd.DataFrame:
    if format == DataFormat.JSON:
        df = pd.read_json(src, dtype=False)
    else:
        df = pd.read_csv(src, dtype=str)

    return df.replace(["", np.nan], None)


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


def preprocess_cd35_datasource(
    src: str,
    format: DataFormat = DataFormat.CSV,
) -> pd.DataFrame:
    logger.info("Extraction...")
    df = cd35.extract_data(src)
    logger.info("Transformation...")
    return cd35.transform_data(df)


PREPROCESS_BY_SOURCE_TYPE = {
    SourceType.DORA: preprocess_dora_datasource,
    SourceType.ITOU: preprocess_itou_datasource,
    SourceType.CD35: preprocess_cd35_datasource,
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
        errors_df.to_csv(error_output_path, index=False)


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
        errors_df.to_csv(error_output_path, index=False)

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


def siretize(
    filepath: str,
) -> pd.DataFrame:
    df = pd.read_json(filepath, dtype=False).replace(np.nan, None)
    logger.info("Siretisation...")
    return siretisation.siretize_normalized_dataset(df)
