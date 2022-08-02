import logging
from pathlib import Path

from data_inclusion.tasks import constants
from data_inclusion.tasks.sources import cd35, dora, itou, odspep, siao, soliguide

logger = logging.getLogger(__name__)

RESHAPE_BY_SOURCE_TYPE = {
    constants.SourceType.CD35: cd35.transform_data,
    constants.SourceType.DORA: dora.transform_data,
    constants.SourceType.ITOU: itou.transform_data,
    constants.SourceType.ODSPEP: odspep.transform_data,
    constants.SourceType.SIAO: siao.transform_data,
    constants.SourceType.SOLIGUIDE: soliguide.transform_data,
}


def reshape(path: Path, src_type: constants.SourceType):
    logger.info("[REMODÉLISATION]")
    return RESHAPE_BY_SOURCE_TYPE.get(src_type, lambda path: path)(path=path)
