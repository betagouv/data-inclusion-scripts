import json
import logging
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from data_inclusion import settings

logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class DataInclusionAPIV0Client:
    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url.rstrip("/") + "/v0"
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]
        if token is not None:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def report_structure(self, data: dict):
        resp = self.session.post(f"{self.base_url}/reports/", data=data)
        return resp.json()


def load_to_data_inclusion(df: pd.DataFrame):
    if settings.DI_API_URL is None:
        logger.error(
            "La variable d'environnement DI_API_URL doit être configurée pour verser "
            "les données dans data.inclusion"
        )
        raise SystemExit()

    client = DataInclusionAPIV0Client(
        base_url=settings.DI_API_URL,
        token=settings.DI_API_TOKEN,
    )

    # antennas will be sent after their parent structures
    df = df.sort_values("structure_parente", na_position="first")

    for _, row in tqdm(df.iterrows(), total=len(df)):
        # serialize/deserialize to ensure `np.nan` are converted to `null`
        try:
            client.report_structure(data=json.loads(row.to_json(force_ascii=False)))
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                continue
            raise SystemExit(e)
