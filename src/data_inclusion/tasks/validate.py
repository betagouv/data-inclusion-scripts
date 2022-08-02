import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pydantic

from data_inclusion import schema

logger = logging.getLogger(__name__)


def validate_normalized_data(
    path: Path,
) -> Path:
    logger.info("[VALIDATION]")
    output_path = Path(f"./{path.stem}.validated.json")
    input_df = pd.read_json(path, dtype=False).replace(np.nan, None)
    output_df = validate_dataframe(input_df)
    output_df.to_json(output_path, orient="records", force_ascii=False)
    return output_path


def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(errors=lambda x: x.apply(validate_row, axis=1))

    errors_df = (
        df.pipe(lambda x: x[["id", "errors"]])
        .pipe(
            lambda x: pd.json_normalize(
                x.to_dict(orient="records"), record_path="errors", meta="id"
            )
        )
        .rename(
            columns={
                "loc": "location",
                "msg": "message",
                "type": "type",
            }
        )
        .pipe(lambda x: x.drop(columns=x.columns[x.columns.str.startswith("ctx.")]))
    )

    df = df.assign(is_valid=lambda x: x.errors.isna()).drop(columns="errors")

    logger.info("Résultats de la validation:")
    logger.info(f"\t{len(errors_df)} erreurs détectées")
    logger.info(f"\t{(~df.is_valid).sum()} lignes non conformes")
    logger.info(f"\t{df.is_valid.sum()} lignes conformes")
    return df


def validate_row(data: dict) -> Optional[list]:
    try:
        schema.Structure(**data)
    except pydantic.ValidationError as exc:
        return exc.errors()

    return None
