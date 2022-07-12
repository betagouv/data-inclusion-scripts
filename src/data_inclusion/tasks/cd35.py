from datetime import datetime

import numpy as np
import pandas as pd

from data_inclusion.schema import models

CD35_SOURCE_STR = "cd35"


def extract_data(src: str) -> pd.DataFrame:
    return pd.read_csv(
        src,
        sep=",",
        encoding_errors="replace",
        on_bad_lines="warn",
        dtype=str,
    ).replace(["", np.nan], None)


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    res = pd.DataFrame()

    # id
    res = res.assign(id=df.ORG_ID)

    # siret
    res = res.assign(siret=lambda _: df.siret)

    # rna
    res = res.assign(rna=None)

    # nom
    res = res.assign(nom=df.ORG_NOM)

    # commune
    res = res.assign(commune=df.ORG_VILLE)

    # code_postal
    res = res.assign(code_postal=df.ORG_CP)

    # code_insee
    res = res.assign(code_insee=None)

    # adresse
    res = res.assign(adresse=df.ORG_ADRES)

    # complement_adresse
    res = res.assign(complement_adresse=None)

    # longitude
    res = res.assign(longitude=df.ORG_LONGITUDE)

    # latitude
    res = res.assign(latitude=df.ORG_LATITUDE)

    # typologie
    DI_STRUCT_TYPE_BY_SIGLE = {
        "CCAS": models.Typologie.CCAS.value,
        "MAIRIE": models.Typologie.MUNI.value,
        "EPHAD": models.Typologie.Autre.value,
        "SAAD": models.Typologie.Autre.value,
    }
    res = res.assign(
        typologie=lambda _: df.ORG_SIGLE.map(
            lambda s: DI_STRUCT_TYPE_BY_SIGLE.get(s, None)
        )
    )

    # telephone
    res = res.assign(telephone=df.ORG_TEL)

    # courriel
    res = res.assign(courriel=df.ORG_MAIL)

    # site_web
    res = res.assign(site_web=df.ORG_WEB)

    # presentation_resume
    # presentation_detail
    res = res.assign(
        presentation_resume=lambda _: df.ORG_DESC.map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        ),
        presentation_detail=lambda _: df.ORG_DESC.map(
            lambda s: (None if len(s) <= 280 else s) if s is not None else None
        ),
    )

    # source
    res = res.assign(source=CD35_SOURCE_STR)

    # date_maj
    res = res.assign(
        date_maj=lambda _: df.apply(
            lambda row: row.ORG_DATEMAJ or row.ORG_DATECREA, axis=1
        ).map(lambda s: datetime.strptime(s, "%d-%m-%Y").date().isoformat())
    )

    # structure_parente
    res = res.assign(structure_parente=None)

    # lien_source
    res = res.assign(lien_source=df.URL)

    # horaires_ouverture
    res = res.assign(horaires_ouverture=df.ORG_HORAIRE)

    res = res.dropna(subset=["typologie"])

    return res
