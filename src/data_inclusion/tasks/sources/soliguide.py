import logging
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pytz
import requests
from dateutil.parser import parse as dateutil_parse
from tqdm import tqdm

from data_inclusion import settings
from data_inclusion.schema import models

SOLIGUIDE_SOURCE_STR = "soliguide"

logger = logging.getLogger(__name__)


class APIClient:
    # Documentation on the soliguide API is available here:
    # https://apisolidarite.soliguide.fr/Documentation-technique-de-l-API-Solidarit-ecaf8198f0e9400d93140b8043c9f2ce

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"JWT {settings.SOLIGUIDE_API_TOKEN}"}
        )
        if settings.SOLIGUIDE_API_USER_AGENT is not None:
            self.session.headers.update(
                {"User-Agent": settings.SOLIGUIDE_API_USER_AGENT}
            )

    def search(
        self,
        location_geo_type: str,
        location_geo_value: Optional[str] = None,
    ) -> dict:
        default_data = {
            "location": {
                "geoType": location_geo_type,
            },
            "options": {
                "limit": 15000,
            },
        }
        if location_geo_value is not None:
            default_data["location"]["geoValue"] = location_geo_value

        places_data = []
        page_number = 1
        pbar = None

        while True:
            data = deepcopy(default_data)
            data["options"]["page"] = page_number
            response = self.session.post(
                f"{self.base_url}/new-search",
                json=data,
            )
            response_data = response.json()

            if pbar is None:
                pbar = tqdm(
                    total=response_data["nbResults"],
                    initial=len(response_data["places"]),
                )
            else:
                pbar.update(len(response_data["places"]))

            places_data += response_data["places"]
            page_number += 1

            if len(places_data) >= response_data["nbResults"]:
                break
            elif len(response_data["places"]) == 0:
                break

            # give some slack to the soliguide api
            time.sleep(10)

        if pbar is not None:
            pbar.close()

        return places_data


def extract_data(src: str) -> Path:
    client = APIClient(base_url=src)
    all_places_data = client.search(
        location_geo_type="pays",
        location_geo_value="france",
    )

    dt = datetime.now(tz=pytz.UTC).isoformat(timespec="seconds")
    output_path = Path(f"./soliguide.{dt}.json")
    all_places_df = pd.DataFrame.from_records(data=all_places_data)
    all_places_df.to_json(output_path, orient="records", force_ascii=False)

    return output_path


def transform_data(path: Path) -> Path:
    output_path = Path(f"./{path.stem}.reshaped.json")
    input_df = pd.read_json(path, dtype=False).replace(np.nan, None)
    output_df = transform_dataframe(input_df)
    output_df.to_json(output_path, orient="records", force_ascii=False)
    return output_path


def transform_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = pd.json_normalize(input_df.to_dict(orient="records"))

    output_df = pd.DataFrame()

    input_df = input_df.replace("", None)

    # id
    output_df = output_df.assign(id=input_df["lieu_id"].astype(str))

    # siret
    output_df = output_df.assign(siret=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df["name"])

    # commune
    output_df = output_df.assign(commune=input_df["ville"])

    # code_postal
    output_df = output_df.assign(code_postal=input_df["position.codePostal"])

    # code_insee
    # the field is very poorly filled in the source
    output_df = output_df.assign(code_insee=None)

    # adresse
    output_df = output_df.assign(adresse=input_df["position.adresse"])

    # complement_adresse
    output_df = output_df.assign(
        complement_adresse=input_df["position.complementAdresse"]
    )

    # longitude
    output_df = output_df.assign(longitude=input_df["position.coordinates.x"])

    # latitude
    output_df = output_df.assign(latitude=input_df["position.coordinates.y"])

    # typologie
    def potential_typologie_from_name(s: str):
        # get a plausible typologie from the structure name
        # there could be several : get the first match
        potential_typologies_dict = {
            models.Typologie.MUNI: s.startswith("mairie de")
            and "ccas" not in s
            and "service" not in s,
            models.Typologie.PE: "pôle emploi" in s,
            models.Typologie.CAF: "caf" in s.split()
            or "allocation" in s
            and "familiales" in s,
            models.Typologie.ASSO: "association" in s,
            models.Typologie.CCAS: "ccas" in s,
            models.Typologie.CAARUD: "caarud" in s,
            models.Typologie.HUDA: "huda" in s.split(),
            models.Typologie.RS_FJT: "fjt" in s
            or "f.j.t" in s
            or ("sidence sociale" in s)
            or s.startswith("rs "),
        }
        return next(
            (k for k, v in potential_typologies_dict.items() if v),
            models.Typologie.Autre,
        ).value

    output_df = output_df.assign(
        typologie=input_df["name"]
        .str.lower()
        .map(lambda s: potential_typologie_from_name(s))
    )

    # telephone
    output_df = output_df.assign(
        telephone=input_df["entity.phones"].map(
            lambda o: o[0]["phoneNumber"] if len(o) > 0 else None
        )
    )

    # courriel
    output_df = output_df.assign(courriel=input_df["entity.mail"])

    # site_web
    output_df = output_df.assign(site_web=input_df["entity.website"])

    # presentation_resume
    # presentation_detail
    output_df = output_df.assign(
        presentation_resume=input_df["description"].map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        ),
        presentation_detail=input_df["description"].map(
            lambda s: (None if len(s) <= 280 else s) if s is not None else None
        ),
    )

    # source
    output_df = output_df.assign(source=SOLIGUIDE_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df["updatedAt"].map(lambda s: dateutil_parse(s).isoformat())
    )

    # structure_parente
    output_df = output_df.assign(structure_parente=None)  # TODO

    # lien_source
    output_df = output_df.assign(
        lien_source=input_df.seo_url.map(lambda s: f"https://soliguide.fr/fiche/{s}")
    )

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    def potential_labels_from_name(s: str):
        # get plausible labels from the structure name
        potential_labels_dict = {
            models.LabelNational.MISSION_LOCALE: "mission" in s and "locale" in s,
            models.LabelNational.CAF: "caf" in s.split()
            or "allocation" in s
            and "familiales" in s,
            models.LabelNational.SECOURS_POPULAIRE: "secours populaire" in s,
            models.LabelNational.CROIX_ROUGE: "croix rouge" in s,
            models.LabelNational.FRANCE_SERVICE: "france service" in s,
        }
        return [k.value for k, v in potential_labels_dict.items() if v]

    output_df = output_df.assign(
        labels_nationaux=input_df["name"]
        .str.lower()
        .map(lambda s: potential_labels_from_name(s))
    )

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    return output_df
