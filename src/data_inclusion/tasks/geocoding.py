import csv
import dataclasses
import io
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from data_inclusion.tasks import utils

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class GeocodingInput:
    id: str
    adresse: str
    code_postal: str
    commune: str


@dataclasses.dataclass(frozen=True)
class GeocodingOutput:
    id: str
    code_insee: str
    score: float


class GeocodingBackend:
    def geocode_batch(
        self, geocoding_input_list: list[GeocodingInput]
    ) -> list[GeocodingOutput]:
        raise NotImplementedError


class BaseAdresseNationaleBackend(GeocodingBackend):
    def __init__(self, base_url: str):
        self.base_url = base_url.strip("/")

    def geocode_batch(
        self, geocoding_input_list: list[GeocodingInput]
    ) -> list[GeocodingOutput]:
        with io.BytesIO() as buf:
            wrapper = io.TextIOWrapper(buf, newline="", write_through=True)

            writer = csv.DictWriter(
                wrapper, fieldnames=["id", "adresse", "code_postal", "commune"]
            )
            writer.writeheader()
            writer.writerows(
                (
                    dataclasses.asdict(geocoding_input)
                    for geocoding_input in geocoding_input_list
                )
            )

            try:
                response = requests.post(
                    self.base_url + "/search/csv/",
                    files={"data": ("data.csv", buf.getvalue(), "text/csv")},
                    data={
                        "columns": ["adresse", "code_postal", "commune"],
                        "postcode": "code_postal",
                        "result_columns": ["result_citycode", "result_score"],
                    },
                )
            except requests.RequestException as e:
                logger.info("Error while fetching `%s`: %s", e.request.url, e)
                return []

        with io.StringIO() as f:
            f.write(response.text)
            f.seek(0)

            reader = csv.DictReader(f)
            geocoding_results = [
                GeocodingOutput(
                    id=row["id"],
                    code_insee=row["result_citycode"],
                    score=float(row["result_score"]),
                )
                for row in reader
                if row.get("result_citycode", "") != ""
            ]

        return geocoding_results


def geocode_normalized_data(
    path: Path,
    geocoding_backend: GeocodingBackend,
) -> Path:
    logger.info("[G??OCODING]")
    output_path = Path(f"./{path.stem}.geocoded.json")
    input_df = pd.read_json(path, dtype=False).replace(np.nan, None)
    output_df = geocode_normalized_dataframe(
        input_df, geocoding_backend=geocoding_backend
    )
    output_df.to_json(output_path, orient="records", force_ascii=False)
    return output_path


def geocode_normalized_dataframe(
    df: pd.DataFrame,
    geocoding_backend: GeocodingBackend,
) -> pd.DataFrame:
    utils.log_df_info(df, logger)

    geocoding_outputs = geocoding_backend.geocode_batch(
        [
            GeocodingInput(
                id=id, adresse=adresse, code_postal=code_postal, commune=commune
            )
            for id, adresse, code_postal, commune in zip(
                df.id, df.adresse, df.code_postal, df.commune
            )
        ]
    )

    geocoding_output_by_id = {o.id: o for o in geocoding_outputs}

    def fill_code_insee(row):
        # keep the code_insee provided by the source
        if row["code_insee"] is not None:
            return row["code_insee"]

        if row.id not in geocoding_output_by_id:
            return None

        geocoding_output = geocoding_output_by_id[row.id]

        # skip geocoding if the score is low
        if geocoding_output.score < 0.4:
            return None

        return geocoding_output.code_insee

    df["code_insee"] = df.apply(fill_code_insee, axis="columns")

    utils.log_df_info(df, logger)

    return df
