import re

import pandas as pd
import numpy as np


DORA_SOURCE = "dora"


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
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
        .dropna(
            subset=[
                "siret",
                "nom",
                "commune",
                "code_insee",
                "code_postal",
                "adresse",
                "date_maj",
            ]
        )
        .assign(
            typologie=lambda x: pd.Categorical(
                x.typologie.map(lambda v: v["value"], na_action="ignore")
            ),
            source=DORA_SOURCE,
            rna=lambda x: None,
        )
    )
