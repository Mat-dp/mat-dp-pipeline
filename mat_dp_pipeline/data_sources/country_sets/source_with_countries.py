from pathlib import Path
from typing import ClassVar, Optional

import pandas as pd

from .create_country_to_path import CountrySets, create_country_to_path
from .identifier import Identifier

__all__ = ["SourceWithCountries"]


class SourceWithCountries:
    _country_name_to_path: ClassVar[dict[str, Path]]
    _alpha2_to_path: ClassVar[dict[str, Path]]
    _alpha3_to_path: ClassVar[dict[str, Path]]
    _names_to_paths: ClassVar[dict[str, Path]]
    _missing_countries: ClassVar[Optional[set[str]]] = None
    country_code_df: ClassVar[pd.DataFrame] = pd.read_csv(
        Path(__file__).parent / "country_codes.csv"
    )
    main_label: ClassVar[str] = "Country"

    def __init_subclass__(
        cls,
        *,
        country_sets: CountrySets,
        names_to_paths: dict[str, Path] | dict[str, str] | dict[str, str | Path] = {},
    ) -> None:
        cls._country_name_to_path = create_country_to_path(
            country_sets, cls.country_code_df, Identifier.country_name
        )
        cls._alpha2_to_path = create_country_to_path(
            country_sets, cls.country_code_df, Identifier.alpha_2
        )
        cls._alpha3_to_path = create_country_to_path(
            country_sets, cls.country_code_df, Identifier.alpha_3
        )
        cls._names_to_paths = {k: Path(v) for k, v in names_to_paths.items()}

    @classmethod
    def country_to_path(cls, identifier: Identifier = Identifier.country_name):
        if identifier == Identifier.country_name:
            return cls._country_name_to_path
        elif identifier == Identifier.alpha_2:
            return cls._alpha2_to_path
        elif identifier == Identifier.alpha_3:
            return cls._alpha3_to_path
        else:
            assert False

    @classmethod
    def missing_countries(cls) -> set[str]:
        if cls._missing_countries is None:
            full_set = set(cls.country_code_df["name"])
            new_set = set(cls.country_to_path.keys())
            cls._missing_countries = full_set - new_set
        return cls._missing_countries

    def name_to_path(self, name: str) -> Path:
        if name in self._names_to_paths:
            return self._names_to_paths[name]
        elif name in self._country_name_to_path:
            return self._country_name_to_path[name]
        elif name in self._alpha2_to_path:
            return self._alpha2_to_path[name]
        elif name in self._alpha3_to_path:
            return self._alpha3_to_path[name]
        else:
            raise ValueError(f"Country {name} not found")
