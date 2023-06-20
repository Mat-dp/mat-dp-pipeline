from enum import Enum
from functools import cache
from pathlib import Path

import pandas as pd

TECH_MAP_FILE = Path(__file__).parent / "Technology_Codes.xlsx"


TechMap = dict[str, tuple[str, str]]


class TechMapTypes(Enum):
    CODE = "Code"
    TMBA = "tmba_variable"
    IAM = "Variable"
    MATDB = "tech_matdb"
    IAMc = "Variable"


@cache
def tech_map_frame() -> pd.DataFrame:
    return pd.read_excel(TECH_MAP_FILE, header=1)


def create_tech_map(
    from_type: TechMapTypes, to_type: TechMapTypes = TechMapTypes.MATDB
) -> TechMap:
    """Create technology map from Technology_Codes.xlsx file.

    Args:
        from_type (TechMapTypes): source encoding
        to_type (TechMapTypes, optional): Destination encoding. Defaults to TechMapTypes.MATDB.

    Returns:
        TechMap: Dictionary from tech to tuples (Category, specified tech encoding).
    """
    df = tech_map_frame()
    from_col = from_type.value
    from_series = df[from_type.value]
    return (
        df[~from_series.isna()]
        .set_index(from_col)[["Category", to_type.value]]
        .dropna()
        .apply(tuple, axis=1)
        .to_dict()
    )
