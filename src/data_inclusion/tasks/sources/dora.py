from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytz

DORA_SOURCE_STR = "dora"


def extract_data(src: str):
    dt = datetime.now(tz=pytz.UTC).isoformat(timespec="seconds")
    output_path = Path(f"./dora.{dt}.json")
    df = pd.read_json(src, dtype=False)
    df.to_json(output_path, orient="records", force_ascii=False)
    return output_path


def transform_data(path: Path) -> Path:
    output_path = Path(f"./{path.stem}.reshaped.json")
    input_df = pd.read_json(path, dtype=False).replace(np.nan, None)
    output_df = transform_dataframe(input_df)
    output_df.to_json(output_path, orient="records", force_ascii=False)
    return output_path


def transform_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    # id
    output_df = output_df.assign(id=input_df.id)

    # siret
    output_df = output_df.assign(siret=input_df.siret)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df.name)

    # commune
    output_df = output_df.assign(commune=input_df.city)

    # code_postal
    output_df = output_df.assign(code_postal=input_df.postalCode)

    # code_insee
    output_df = output_df.assign(code_insee=input_df.cityCode)

    # adresse
    output_df = output_df.assign(adresse=input_df.address1)

    # complement_adresse
    output_df = output_df.assign(complement_adresse=input_df.address2)

    # longitude
    output_df = output_df.assign(longitude=input_df.longitude)

    # latitude
    output_df = output_df.assign(latitude=input_df.latitude)

    # typologie
    output_df = output_df.assign(
        typologie=input_df.typology.map(lambda v: v["value"] if v is not None else None)
    )

    # telephone
    output_df = output_df.assign(telephone=input_df.phone)

    # courriel
    output_df = output_df.assign(courriel=input_df.email)

    # site_web
    output_df = output_df.assign(site_web=input_df.url)

    # presentation_resume
    output_df = output_df.assign(presentation_resume=input_df.shortDesc)

    # presentation_detail
    output_df = output_df.assign(presentation_detail=input_df.fullDesc)

    # source
    output_df = output_df.assign(source=DORA_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df.modificationDate.map(
            lambda v: datetime.fromisoformat(v).astimezone(pytz.UTC).isoformat()
        )
    )

    # structure_parente
    output_df = output_df.assign(structure_parente=None)

    # lien_source
    output_df = output_df.assign(lien_source=input_df.linkOnSource)

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
