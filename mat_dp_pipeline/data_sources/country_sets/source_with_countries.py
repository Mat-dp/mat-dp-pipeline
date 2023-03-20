from pathlib import Path
from typing import ClassVar

import pandas as pd

from .create_country_to_path import CountrySets, create_country_to_path

__all__ = ["SourceWithCountries"]


class SourceWithCountries:
    country_to_path: ClassVar[dict[str, Path]]
    names_to_paths: ClassVar[dict[str, Path]]
    missing_countries: ClassVar[set[str]]
    country_code_df: ClassVar[pd.DataFrame] = pd.read_csv("./country_codes.csv")

    def __init_subclass__(
        cls,
        *,
        country_sets: CountrySets,
        names_to_paths: dict[str, Path] | dict[str, str] | dict[str, str | Path] = {},
    ) -> None:
        cls.country_to_path = create_country_to_path(country_sets)
        cls.names_to_paths = {k: Path(v) for k, v in names_to_paths.items()}
        full_set = set(cls.country_code_df["name"])
        new_set = set(cls.country_to_path.keys())
        cls.missing_countries = full_set - new_set
