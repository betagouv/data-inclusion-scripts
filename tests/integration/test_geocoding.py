from unittest import mock

import pytest

from data_inclusion.tasks import geocoding

pytestmark = pytest.mark.ban_api


@pytest.fixture
def ban_backend():
    return geocoding.BaseAdresseNationaleBackend(
        base_url="https://api-adresse.data.gouv.fr"
    )


@pytest.mark.parametrize(
    "geocoding_inputs,expected_result",
    [
        (
            [
                geocoding.GeocodingInput(
                    id="1",
                    adresse="27 Impasse Lefebvre",
                    code_postal="59260",
                    commune="Hellemmes",
                )
            ],
            [
                geocoding.GeocodingOutput(
                    id="1",
                    code_insee="59350",
                    score=mock.ANY,
                )
            ],
        ),
    ],
)
def test_ban_geocode(
    ban_backend: geocoding.BaseAdresseNationaleBackend,
    geocoding_inputs: list[geocoding.GeocodingInput],
    expected_result: str,
):
    assert ban_backend.geocode_batch(geocoding_inputs) == expected_result
