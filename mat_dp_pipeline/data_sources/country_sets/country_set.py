from dataclasses import dataclass, field
from typing import ClassVar

import pandas as pd

__all__ = ["CountrySet"]


@dataclass
class CountrySet:
    country_code_df: ClassVar[pd.DataFrame] = pd.read_csv("./country_codes.csv")
    countries: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    sub_regions: list[str] = field(default_factory=list)
    intermediate_regions: list[str] = field(default_factory=list)
    custom_countries: list[str] = field(default_factory=list)
    _all_countries: list[str] = field(init=False, repr=False)

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

            for custom_country in self.custom_countries:
                total_countries.append(custom_country)

            self._all_countries = list(set(total_countries))
        return self._all_countries
