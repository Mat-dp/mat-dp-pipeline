from enum import Enum
from functools import cache
from pathlib import Path

import pandas as pd

TECH_MAP_FILE = Path(__file__).parent / "Technology_Codes.xlsx"
MATDB_COL = "tech_matdb"


class TechMapTypes(Enum):
    CODE = "Code"
    TMBA = "tmba_variable"
    IAM = "Variable"
    MATDB = "tech_matdb"


@cache
def tech_map_frame() -> pd.DataFrame:
    return pd.read_excel(TECH_MAP_FILE, header=1)


def create_tech_map(
    from_type: TechMapTypes, to_type: TechMapTypes = TechMapTypes.MATDB
) -> dict[str, str]:
    df = tech_map_frame()
    from_col = from_type.value
    from_series = df[from_type.value]
    return (
        df[~from_series.isna()].set_index(from_col)[to_type.value].fillna("").to_dict()
    )
