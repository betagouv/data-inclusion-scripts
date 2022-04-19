from datetime import datetime
import logging
from pathlib import Path
import pytz

import great_expectations as ge
import pandas as pd


logger = logging.getLogger(__name__)


GE_CONTEXT_PATH = Path(__file__).parent / "great_expectations"


def validate_schema(source_label: str, df: pd.DataFrame) -> bool:
    context = ge.DataContext(context_root_dir=GE_CONTEXT_PATH)
    result = context.run_checkpoint(
        checkpoint_name="schema_checkpoint",
        batch_request={
            "data_asset_name": source_label,
            "runtime_parameters": {"batch_data": df},
            "batch_identifiers": {"timestamp": str(datetime.now(pytz.utc))},
        },
    )

    logger.debug(result)
    return result["success"]
