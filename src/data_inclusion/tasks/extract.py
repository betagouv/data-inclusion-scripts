import logging
from pathlib import Path

from data_inclusion.tasks import constants
from data_inclusion.tasks.sources import dora, itou, soliguide

logger = logging.getLogger(__name__)

EXTRACT_BY_SOURCE_TYPE = {
    constants.SourceType.DORA: dora.extract_data,
    constants.SourceType.ITOU: itou.extract_data,
    constants.SourceType.SOLIGUIDE: soliguide.extract_data,
}


def extract(src: str, src_type: constants.SourceType) -> Path:
    logger.info("[EXTRACTION]")
    return EXTRACT_BY_SOURCE_TYPE.get(src_type, lambda src: Path(src))(src=src)
