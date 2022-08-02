import logging

from data_inclusion.tasks import constants, extract, geocoding, load, reshape, validate

logger = logging.getLogger(__name__)


def full_processing(
    src: str,
    src_type: constants.SourceType,
    geocoding_backend: geocoding.GeocodingBackend,
    dry_run: bool = False,
):
    path = extract.extract(src=src, src_type=src_type)
    path = reshape.reshape(path=path, src_type=src_type)
    path = geocoding.geocode_normalized_data(path, geocoding_backend=geocoding_backend)
    path = validate.validate_normalized_data(path)

    if not dry_run:
        load.load_data(path=path)
