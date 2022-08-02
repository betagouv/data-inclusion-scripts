import io
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def log_df_info(df: pd.DataFrame, logger: logging.Logger = logger):
    buf = io.StringIO()
    df.info(buf=buf)
    for line in buf.getvalue().splitlines():
        logger.info(line)
