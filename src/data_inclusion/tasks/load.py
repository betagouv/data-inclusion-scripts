import logging
import json

import pandas as pd
import requests
from tqdm import tqdm


logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class DataInclusionAPIV0Client:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/") + "/v0"
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]

    def report_structure(self, data: dict):
        resp = self.session.post(f"{self.base_url}/reports/", json=data)
        return resp.json()


def load_to_data_inclusion(df: pd.DataFrame, api_url: str):
    client = DataInclusionAPIV0Client(base_url=api_url)

    for _, row in tqdm(df.iterrows(), total=len(df)):
        # sérialisation/désérialisation pour profiter du fait que
        # `.to_json()` convertit les `np.nan` en `null`
        client.report_structure(data=json.loads(row.to_json()))
