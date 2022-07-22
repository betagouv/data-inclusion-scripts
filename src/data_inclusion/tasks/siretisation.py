import logging
import textwrap
from typing import Optional

import pandas as pd
import sqlalchemy as slqa
from sqlalchemy.engine import Engine
from tqdm import tqdm

from data_inclusion import settings

logging.basicConfig()
logger = logging.getLogger(__name__)

tqdm.pandas()


def search_establishment(
    nom: str,
    adresse: str,
    code_insee: str,
    latitude: float,
    longitude: float,
    engine: Engine,
) -> Optional[dict]:
    if not all([nom, adresse, code_insee]):
        logger.debug("Missing data")
        return None
    elif not all([latitude, longitude]):
        logger.debug("Missing coordinates")
        return None

    cog_department_str = code_insee[:2]
    normalized_name_str = nom.replace("'", " ")
    normalized_address_str = adresse.replace("'", " ")
    location_within_meters_int = 1000
    name_similarity_threshold_float = 0.6

    nearest_establishments_query_str = textwrap.dedent(
        f"""
        SELECT
            *,
            ST_Distance(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) :: geography,ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326) :: geography) AS distance,
            similarity(address1, '{normalized_address_str}') as address_similarity,
            similarity(name, '{normalized_name_str}') as name_similarity
        FROM
            sirene_establishment
        WHERE
            city_code ~ '^{cog_department_str}'
            AND ST_Distance(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) :: geography,ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326) :: geography) < {location_within_meters_int}
        ORDER BY name_similarity DESC;
        """  # noqa: E501
    )

    logger.debug(nearest_establishments_query_str)

    establishments_df = pd.read_sql(
        nearest_establishments_query_str,
        engine,
    )

    if len(establishments_df) == 0:
        logger.debug("No establishment with similar address within 1km")
        return None

    # considering only establishments that would match the position,
    # is there any close match on the name ?
    if establishments_df.iloc[0].name_similarity < name_similarity_threshold_float:
        logger.debug("No establishment with similar address and name within 1km")
        return None

    return establishments_df.iloc[0].to_dict()


def siretize_normalized_dataset(
    structures_df: pd.DataFrame,
) -> pd.DataFrame:
    if settings.SIRENE_DATABASE_URL is None:
        raise Exception("SIRENE_DATABASE_URL not configured.")

    engine = slqa.create_engine(settings.SIRENE_DATABASE_URL)

    establishments_df = structures_df.progress_apply(
        lambda row: search_establishment(
            nom=row.nom,
            adresse=row.adresse,
            code_insee=row.code_insee,
            latitude=row.latitude,
            longitude=row.longitude,
            engine=engine,
        )
        or {},
        axis="columns",
        result_type="expand",
    )

    return structures_df.assign(siret=lambda _: establishments_df.siret)
