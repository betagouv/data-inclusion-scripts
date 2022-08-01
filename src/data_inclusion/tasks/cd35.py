from datetime import datetime

import numpy as np
import pandas as pd

from data_inclusion.schema import models

CD35_SOURCE_STR = "cd35"


def extract_data(src: str) -> pd.DataFrame:
    return pd.read_csv(
        src,
        sep=";",
        encoding_errors="replace",
        on_bad_lines="warn",
        dtype=str,
    ).replace(["", np.nan], None)


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    output_df = pd.DataFrame()

    # id
    output_df = output_df.assign(id=input_df.ORG_ID)

    # siret
    output_df = output_df.assign(siret=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df.ORG_NOM)

    # commune
    output_df = output_df.assign(commune=input_df.ORG_VILLE)

    # code_postal
    output_df = output_df.assign(code_postal=input_df.ORG_CP)

    # code_insee
    output_df = output_df.assign(code_insee=None)

    # adresse
    output_df = output_df.assign(adresse=input_df.ORG_ADRES)

    # complement_adresse
    output_df = output_df.assign(complement_adresse=None)

    # longitude
    output_df = output_df.assign(longitude=input_df.ORG_LONGITUDE)

    # latitude
    output_df = output_df.assign(latitude=input_df.ORG_LATITUDE)

    # typologie
    DI_STRUCT_TYPE_BY_SIGLE = {
        "CCAS": models.Typologie.CCAS.value,
        "MAIRIE": models.Typologie.MUNI.value,
        "EPHAD": models.Typologie.Autre.value,
        "SAAD": models.Typologie.Autre.value,
    }
    output_df = output_df.assign(
        typologie=input_df.ORG_SIGLE.map(
            lambda s: DI_STRUCT_TYPE_BY_SIGLE.get(s, None) or s
        )
    )

    # telephone
    output_df = output_df.assign(telephone=input_df.ORG_TEL)

    # courriel
    output_df = output_df.assign(courriel=input_df.ORG_MAIL)

    # site_web
    output_df = output_df.assign(site_web=input_df.ORG_WEB)

    # presentation_resume
    # presentation_detail
    output_df = output_df.assign(
        presentation_resume=input_df.ORG_DESC.map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        ),
        presentation_detail=input_df.ORG_DESC.map(
            lambda s: (None if len(s) <= 280 else s) if s is not None else None
        ),
    )

    # source
    output_df = output_df.assign(source=CD35_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df.apply(
            lambda row: row.ORG_DATEMAJ or row.ORG_DATECREA, axis=1
        ).map(lambda s: datetime.strptime(s, "%d-%m-%Y").date().isoformat())
    )

    # structure_parente
    output_df = output_df.assign(structure_parente=None)

    # lien_source
    output_df = output_df.assign(lien_source=input_df.URL)

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=input_df.ORG_HORAIRE)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    return output_df
