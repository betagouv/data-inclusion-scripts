import re
from datetime import datetime

import numpy as np
import pandas as pd
import pytz

DORA_SOURCE = "dora"


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Convertit les données du format DORA vers le format standard.

    Args:
        df: Un dataframe contenant des données de structures au format de DORA.

    Returns:
        Un dataframe contenant les mêmes données converties au format standard à priori.
    """

    return (
        df.rename(columns=lambda h: re.sub(r"(?<!^)(?=[A-Z])", "_", h).lower())
        .drop(
            columns=[
                "code_safir_pe",
                "ape",
                "creation_date",
                "link_on_source",
                "services",
                "department",
            ]
        )
        .rename(
            columns={
                "typology": "typologie",
                "name": "nom",
                "short_desc": "presentation_resume",
                "full_desc": "presentation_detail",
                "url": "site_web",
                "phone": "telephone",
                "email": "courriel",
                "postal_code": "code_postal",
                "city_code": "code_insee",
                "city": "commune",
                "address1": "adresse",
                "address2": "complement_adresse",
                "modification_date": "date_maj",
            }
        )
        # conversion pour simplifier la sérialisation et la manipulation des valeurs
        # nulles
        .replace(np.nan, None)
        # normalisations des chaînes de caractères vides, qui ne sont pas considérées
        # comme des valeurs nulles du point de vue du schéma standard
        .replace("", None)
        .assign(
            typologie=lambda x: x.typologie.map(
                lambda v: v["value"] if v is not None else None
            ),
            source=DORA_SOURCE,
            rna=None,
            date_maj=lambda x: x.date_maj.apply(
                lambda x: datetime.fromisoformat(x).astimezone(pytz.UTC).isoformat()
            ),
        )
    )
