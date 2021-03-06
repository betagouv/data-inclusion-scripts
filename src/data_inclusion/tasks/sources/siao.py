from pathlib import Path

import numpy as np
import pandas as pd

from data_inclusion.schema import models

SIAO_SOURCE_STR = "siao"


def transform_data(path: Path) -> Path:
    output_path = Path(f"./{path.stem}.reshaped.json")
    input_df = pd.read_excel(str(path), dtype=str).replace(np.nan, None)
    output_df = transform_dataframe(input_df)
    output_df.to_json(output_path, orient="records", force_ascii=False)
    return output_path


def transform_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    # TODO: currently, if 2 rows share the same SIRET, there is no reliable way of
    # knowing if there is a single structure with 2 services or if there are 2
    # structures which are antennas of the same parent structure. Therefore, any rows
    # that have duplicated SIRET are excluded.
    input_df = input_df.drop_duplicates(subset="Code SIRET", keep=False)

    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    # siret
    output_df = output_df.assign(
        siret=input_df["Code SIRET"]
        .replace(r"\.0", "", regex=True)
        .replace(r"\D", "", regex=True)
        .replace("", None)
        .map(lambda s: s and f"{s:0>14}")
        .replace(r"0{14}", None, regex=True)
    )

    # id
    # TODO: find a proper id field in the input data, and then put this block first
    output_df = output_df.assign(id=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df["Nom de la structure"])

    # commune
    output_df = output_df.assign(commune=input_df["Ville"])

    # code_postal
    output_df = output_df.assign(
        code_postal=input_df["Code postal"]
        .replace(r"\.0", "", regex=True)
        .replace(r"\D", "", regex=True)
        .replace("", None)
        .map(lambda s: s and f"{s:0>5}")
    )

    # code_insee
    # the field is very poorly filled in the source
    output_df = output_df.assign(code_insee=None)

    # adresse
    output_df = output_df.assign(adresse=input_df["Adresse"])

    # complement_adresse
    output_df = output_df.assign(complement_adresse=None)

    # longitude
    output_df = output_df.assign(longitude=None)

    # latitude
    output_df = output_df.assign(latitude=None)

    # typologie
    def potential_typologie_from_name(s: str):
        # get a plausible typologie from the name
        # there could be several : get the first match
        potential_typologies_dict = {
            models.Typologie.CCAS: "ccas" in s,
            models.Typologie.CADA: "cada" in s.split(),
            models.Typologie.CHRS: "chrs" in s or "c.h.r.s" in s,
            models.Typologie.CHU: "chu" in s.split(),
            models.Typologie.CPH: "cph" in s.split() or "c.p.h" in s,
            models.Typologie.HUDA: "huda" in s or "h.u.d.a" in s,
            models.Typologie.SPIP: "spip" in s
            or "s.p.i.p" in s
            or "P??nitentiaire" in s,
            models.Typologie.PJJ: "pjj" in s
            or "p.j.j" in s
            or "protection judiciaire" in s,
            models.Typologie.RS_FJT: "fjt" in s
            or "f.j.t" in s
            or ("sidence" in s and "sociale" in s)
            or s.startswith("rs "),
            models.Typologie.UDAF: "udaf" in s,
            models.Typologie.PLIE: "plie" in s.split(),
            models.Typologie.CS: "centre" in s and "social" in s,
            models.Typologie.CIAS: "cias" in s.split(),
            models.Typologie.CAVA: "cava" in s.split(),
            models.Typologie.MUNI: "mairie" in s and "ccas" not in s,
            models.Typologie.CAARUD: "caarud" in s or "c.a.a.r.u.d" in s,
            models.Typologie.PE: s.startswith("pole emploi"),
            models.Typologie.ASSO: "association" in s
            or "asso" in s.split()
            or "ass" in s.split()
            or "association" in s.split(),
        }
        return next(
            (k for k, v in potential_typologies_dict.items() if v),
            models.Typologie.Autre,
        ).value

    output_df = output_df.assign(
        typologie=input_df["Nom de la structure"]
        .str.lower()
        .map(lambda s: potential_typologie_from_name(s))
    )

    # telephone
    output_df = output_df.assign(telephone=input_df["T??l??phone"])

    # courriel
    output_df = output_df.assign(courriel=input_df["Mail"])

    # site_web
    output_df = output_df.assign(site_web=None)

    # presentation_resume
    output_df = output_df.assign(presentation_resume=None)

    # presentation_detail
    output_df = output_df.assign(presentation_detail=None)

    # source
    output_df = output_df.assign(source=SIAO_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(date_maj=None)  # TODO

    # structure_parente
    output_df = output_df.assign(structure_parente=None)  # TODO

    # lien_source
    output_df = output_df.assign(lien_source=None)

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    def potential_labels_from_name(s: str):
        # get plausible labels from the structure name
        potential_labels_dict = {
            models.LabelNational.MISSION_LOCALE: "mission" in s and "locale" in s,
        }
        return [k.value for k, v in potential_labels_dict.items() if v]

    output_df = output_df.assign(
        labels_nationaux=input_df["Nom de la structure"]
        .str.lower()
        .map(lambda s: potential_labels_from_name(s))
    )

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    return output_df
