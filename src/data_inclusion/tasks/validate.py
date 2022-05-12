import logging
from typing import Optional, Tuple

import pandas as pd
import pydantic

from data_inclusion import schema

logger = logging.getLogger(__name__)


def validate_row(data: dict) -> Optional[list]:
    try:
        schema.Structure(**data)
    except pydantic.ValidationError as exc:
        return exc.errors()

    return None


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    # valide les lignes individuellement
    df = (
        df.reset_index()
        .assign(errors=lambda x: x.apply(validate_row, axis=1))
        .set_index("id")
    )
    # valide les références au sein du dataframe
    # TODO(vmttn)

    return df


def validate(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Valide que les données sont conformes au schéma de données.

    Args:
        df: Un dataframe contenant des données de structures respectant à priori le
            format standard.

    Returns:
        Un dataframe contenant des violations du schéma standard.
    """
    df = validate_df(df)

    errors_df = (
        df.pipe(lambda x: x.reset_index()[["id", "errors"]])
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
        .set_index("id")
        .pipe(lambda x: x.drop(columns=x.columns[x.columns.str.startswith("ctx.")]))
    )

    df = df.assign(is_valid=lambda x: x.errors.isna()).drop(columns="errors")

    logger.info("Résultats de la validation:")
    logger.info(f"\t{len(errors_df)} erreurs détectées")
    logger.info(f"\t{(~df.is_valid).sum()} lignes non conformes")
    logger.info(f"\t{df.is_valid.sum()} lignes conformes")

    return (
        df,
        errors_df,
    )
