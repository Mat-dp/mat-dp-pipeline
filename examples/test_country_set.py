from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import pandas as pd


@dataclass
class AbstractCountrySet:
    country_code_df: ClassVar[pd.DataFrame]
    countries: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    sub_regions: list[str] = field(default_factory=list)
    intermediate_regions: list[str] = field(default_factory=list)
    _all_countries: list[str] = field(init=False, repr=False)

    def __init_subclass__(cls, *, country_code_df: pd.DataFrame) -> None:
        cls.country_code_df = country_code_df

    @property
    def all_countries(self):
        if not hasattr(self, "_all_countries"):
            total_countries = []
            available_countries = list(self.country_code_df["name"])
            for country in self.countries:
                if country not in available_countries:
                    raise ValueError(f"Country {country} not found")
                total_countries.append(country)

            for region in self.regions:
                rel_countries = list(
                    self.country_code_df[self.country_code_df["region"] == region][
                        "name"
                    ]
                )
                if len(rel_countries) == 0:
                    raise ValueError(f"Region {region} not found")
                total_countries += rel_countries

            for sub_region in self.sub_regions:
                rel_countries = list(
                    self.country_code_df[
                        self.country_code_df["sub-region"] == sub_region
                    ]["name"]
                )
                if len(rel_countries) == 0:
                    raise ValueError(f"Sub region {sub_region} not found")
                total_countries += rel_countries

            for intermediate_region in self.intermediate_regions:
                rel_countries = list(
                    self.country_code_df[
                        self.country_code_df["intermediate-region"]
                        == intermediate_region
                    ]["name"]
                )
                if len(rel_countries) == 0:
                    raise ValueError(
                        f"Intermediate region {intermediate_region} not found"
                    )
                total_countries += rel_countries
            self._all_countries = total_countries
        return self._all_countries


CountrySets = dict[str, "AbstractCountrySet | CountrySets"]


def create_country_to_path(country_sets: CountrySets) -> dict[str, Path]:
    final_dict: dict[str, list[str]] = {}

    def dfs(country_sets: CountrySets, prefix: list[str]):
        for k, v in country_sets.items():
            if isinstance(v, AbstractCountrySet):
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


class IntensitiesDataSourceWithCountries:
    country_to_path: ClassVar[dict[str, Path]]
    names_to_paths: ClassVar[dict[str, Path]]

    def __init_subclass__(
        cls, *, country_sets: CountrySets, names_to_paths: dict[str, Path] = {}
    ) -> None:
        cls.country_to_path = create_country_to_path(country_sets)
        cls.names_to_paths = names_to_paths


# Example really begins

countries = pd.read_csv("./country_codes.csv")


class CountrySet(AbstractCountrySet, country_code_df=countries):
    pass


country_sets: CountrySets = {
    "General": {
        "The north americas": CountrySet(regions=["Americas"]),
        "Europe": CountrySet(regions=["Europe"]),
        "__root__": CountrySet(regions=["Africa"]),
    }
}

from pprint import pprint
pprint(create_country_to_path(country_sets))

names_to_paths = {
    "The north americas": Path("/The north americas"),
    "The Great Candad": Path("/The north americas/Canada"),
    "General": Path("/"),
}


class MatDpIntensitiesSource(
    IntensitiesDataSourceWithCountries,
    country_sets=country_sets,
    names_to_paths=names_to_paths,
):
    def __init__(self, materials_spreadsheet: Path):
        self._materials_spreadsheet = materials_spreadsheet

    def _raw(self) -> pd.DataFrame:
        # This is where the names_to_paths would be used to translate etc
        # alternatively to the init subclass, they could be just defined
        # as properties in MatDpIntensitiesSource

        # Notably the country sets are not actually needed here - they could just as easily go in targets.
        # The motivation for having them in intensities is that it forms part of the same definition.
        raise NotImplementedError


class MatDpTargetsSource:
    def __init__(self, targets_file: Path, country_to_path: dict[str, Path]) -> None:
        # Here the country to path mapping can then be used to assign the destination
        raise NotImplementedError


int_source = MatDpIntensitiesSource(Path("Material_intensities_database.xlsx"))
target_source = MatDpTargetsSource(
    Path("examples/results_1.5deg.csv"), int_source.country_to_path
)
