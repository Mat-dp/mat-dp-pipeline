from pathlib import Path

from .country_set import CountrySet

__all__ = ["CountrySets", "create_country_to_path"]

CountrySets = dict[str, "CountrySet | CountrySets"]


def create_country_to_path(country_sets: CountrySets) -> dict[str, Path]:
    final_dict: dict[str, list[str]] = {}

    def dfs(country_sets: CountrySets, prefix: list[str]):
        for k, v in country_sets.items():
            if isinstance(v, CountrySet):
                all_countries = v.all_countries
                for country in all_countries:
                    if country in final_dict:
                        raise ValueError(
                            f"Overlapped disjoint set detected for {country}"
                        )
                    else:
                        if k == "__root__":
                            final_dict[country] = prefix
                        else:
                            final_dict[country] = prefix + [k]
            else:
                dfs(v, prefix=prefix + [k])

    dfs(country_sets, prefix=[])
    path_dict = {k: Path(*v) for k, v in final_dict.items()}
    return path_dict
