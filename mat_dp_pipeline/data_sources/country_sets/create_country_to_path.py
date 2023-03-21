from pathlib import Path

import pandas as pd

from .country_set import CountrySet
from .identifier import Identifier

__all__ = ["CountrySets", "create_country_to_path"]

CountrySets = dict[str, "CountrySet | CountrySets"]


def create_country_to_path(
    country_sets: CountrySets, country_code_df: pd.DataFrame, identifier: Identifier
) -> dict[str, Path]:
    final_dict: dict[str, list[str]] = {}

    def dfs(country_sets: CountrySets, prefix: list[str]):
        for k, v in country_sets.items():
            if isinstance(v, CountrySet):
                for country, country_name in v.all_countries(
                    country_code_df, identifier
                ):
                    if country in final_dict:
                        raise ValueError(
                            f"Overlapped disjoint set detected for {country} location is already {final_dict[country]}"
                        )
                    else:
                        if k == "__root__":
                            final_dict[country] = prefix + [country_name]
                        else:
                            final_dict[country] = prefix + [k, country_name]
            else:
                dfs(v, prefix=prefix + [k])

    dfs(country_sets, prefix=[])
    path_dict = {k: Path(*(["/"] + v)) for k, v in final_dict.items()}
    return path_dict
