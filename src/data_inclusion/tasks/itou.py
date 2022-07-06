import logging

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from data_inclusion import settings

ITOU_SOURCE = "itou"

logger = logging.getLogger(__name__)


class ItouClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(total=2, backoff_factor=120, status_forcelist=[429])
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {"Authorization": f"Token {settings.ITOU_API_TOKEN}"}
        )

    def list_structures(self) -> list:
        next_url = self.url
        structures_data = []

        pbar = None

        while True:
            response = self.session.get(next_url)
            data = response.json()

            if pbar is None:
                pbar = tqdm(total=data["count"], initial=len(data["results"]))
            else:
                pbar.update(len(data["results"]))
            structures_data += data["results"]
            next_url = data["next"]
            if next_url is None:
                break

        if pbar is not None:
            pbar.close()

        return structures_data


def extract_data(src: str) -> pd.DataFrame:
    client = ItouClient(url=src)
    structures_data = client.list_structures()
    return pd.DataFrame.from_records(data=structures_data)


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Convertit les données exposée par ITOU vers le format standard v0.

    Args:
        df: Un dataframe contenant des données de structures dans le format proposé sur
        l'api des emplois de l'inclusion.

    Returns:
        Un dataframe contenant les mêmes données converties au format standard à priori.
    """

    df = (
        # conversion pour simplifier la sérialisation et la manipulation des valeurs
        # nulles
        df.replace(np.nan, None)
        # normalisations des chaînes de caractères vides, qui ne sont pas considérées
        # comme des valeurs nulles du point de vue du schéma standard
        .replace("", None).assign(
            source=ITOU_SOURCE,
        )
    )

    return df
