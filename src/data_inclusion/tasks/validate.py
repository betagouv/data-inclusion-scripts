from datetime import datetime
import logging
from pathlib import Path
import pytz

import great_expectations as ge
import pandas as pd


logger = logging.getLogger(__name__)


GE_CONTEXT_PATH = Path(__file__).parent.parent / "great_expectations"


def validate_schema(df: pd.DataFrame, src_name: str) -> bool:
    context = ge.DataContext(context_root_dir=str(GE_CONTEXT_PATH))
    now = datetime.now(pytz.utc)
    result = context.run_checkpoint(
        checkpoint_name="schema_checkpoint",
        batch_request={
            "data_asset_name": f"{src_name}-{now.date()}",
            "runtime_parameters": {"batch_data": df},
            "batch_identifiers": {"timestamp": str(now)},
        },
    )

    logger.debug(result)
    return result["success"]
