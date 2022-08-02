from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from data_inclusion.schema import models

ODSPEP_SOURCE_STR = "odspep"


def transform_data(path: Path) -> Path:
    output_path = Path(f"./{path.stem}.reshaped.json")
    input_df = pd.read_excel(str(path), dtype=str).replace(np.nan, None)
    output_df = transform_dataframe(input_df)
    output_df.to_json(output_path, orient="records", force_ascii=False)
    return output_path


def transform_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    # TODO: only services are properly identified in the file. There is no reliable way
    # to prevent duplicated structures rows at this point or to detect antennas.
    input_df = input_df.drop_duplicates("ID_RES")

    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    # id
    output_df = output_df.assign(id=input_df["ID_RES"])  # TODO

    # siret
    output_df = output_df.assign(siret=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df["STRUCTURE"])

    # commune
    output_df = output_df.assign(commune=input_df["LIBELLE_COMMUNE_ADR"])

    # code_postal
    output_df = output_df.assign(code_postal=input_df["CODE_POSTAL_ADR"])

    # code_insee
    output_df = output_df.assign(code_insee=input_df["CODE_COMMUNE_ADR"])

    # adresse
    output_df = output_df.assign(adresse=input_df["L4_NUMERO_LIB_VOIE_ADR"])

    # complement_adresse
    output_df = output_df.assign(complement_adresse=input_df["L3_COMPLEMENT_ADR"])

    # longitude
    output_df = output_df.assign(longitude=input_df["LONGITUDE_ADR"])

    # latitude
    output_df = output_df.assign(latitude=input_df["LATITUDE_ADR"])

    # typologie
    def potential_typologie_from_name(s: str):
        # get a plausible typologie from the name
        # there could be several : get the first match
        potential_typologies_dict = {
            models.Typologie.CAF: "caf" in s.split()
            or ("caisse" in s and "allocation" in s and "fami" in s),
            models.Typologie.CC: "communaut" in s
            and "commune" in s
            and "maternelle" not in s,
            models.Typologie.ASSO: "association" in s.split(),
            models.Typologie.CCAS: "ccas" in s.split()
            or "social" in s
            and "action" in s,
            models.Typologie.CHRS: "chrs" in s.split()
            or ("bergement" in s and "insertion" in s),
            models.Typologie.RS_FJT: ("sidence" in s and "social" in s)
            or "fjt" in s
            or ("foyer" in s and "jeune" in s and "travail" in s),
            models.Typologie.CS: "centre social" in s,
            models.Typologie.MDS: "maison" in s and "solidarit" in s,
            models.Typologie.ML: "mission" in s and "local" in s,
        }
        return next(
            (k for k, v in potential_typologies_dict.items() if v),
            models.Typologie.Autre,
        ).value

    output_df = output_df.assign(
        typologie=input_df["STRUCTURE"]
        .str.lower()
        .map(lambda s: potential_typologie_from_name(s))
    )

    # telephone
    output_df = output_df.assign(telephone=input_df["TEL_1_CTC"])

    # courriel
    output_df = output_df.assign(courriel=input_df["MAIL_CTC"])

    # site_web
    output_df = output_df.assign(site_web=input_df["SITE_INTERNET_CTC"])

    # presentation_resume
    output_df = output_df.assign(presentation_resume=None)

    # presentation_detail
    output_df = output_df.assign(presentation_detail=None)

    # source
    output_df = output_df.assign(source=ODSPEP_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df["DATE DERNIERE MAJ"].map(
            lambda s: s or datetime.strptime(s, "%Y-%m-%d %H:%M:%S").isoformat()
        )
    )

    # structure_parente
    output_df = output_df.assign(structure_parente=None)  # TODO

    # lien_source
    output_df = output_df.assign(lien_source=None)  # TODO

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)  # TODO

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)  # TODO

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    return output_df
