import enum


class SourceType(str, enum.Enum):
    """Types of datasources.

    Different types of source (and their corresponding format) are handled.

    Extraction and transformation will vary according to the source.
    """

    # custom sources which requires ad hoc extraction/transformation.
    CD35 = "cd35"
    DORA = "dora"
    ITOU = "itou"
    SIAO = "siao"
    ODSPEP = "odspep"
    SOLIGUIDE = "soliguide"
