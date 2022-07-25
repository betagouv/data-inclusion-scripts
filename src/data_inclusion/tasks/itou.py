import logging

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from data_inclusion import settings

ITOU_SOURCE_STR = "itou"

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


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    # data exposed by itou should be serialized in the data.inclusion schema
    output_df = input_df.copy(deep=True)

    # source
    output_df = output_df.assign(source=ITOU_SOURCE_STR)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    output_df = output_df.assign(labels_nationaux=None)

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
