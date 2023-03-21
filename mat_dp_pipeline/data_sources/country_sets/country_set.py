from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from .identifier import Identifier

__all__ = ["CountrySet", "CustomCountry"]


@dataclass
class CustomCountry:
    name: str
    alpha_2: Optional[str]
    alpha_3: Optional[str]


@dataclass
class CountrySet:
    countries: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    sub_regions: list[str] = field(default_factory=list)
    intermediate_regions: list[str] = field(default_factory=list)
    custom_countries: list[CustomCountry] = field(default_factory=list)
    _all_countries: dict[Identifier, list[str]] = field(
        default_factory=dict, init=False, repr=False
    )

    def all_countries(self, country_code_df: pd.DataFrame, identifier: Identifier):
        if identifier not in self._all_countries:
            identifier_value: str = identifier.value
            total_countries = []
            available_countries = list(country_code_df[identifier_value])
            for country in self.countries:
                if country not in available_countries:
                    raise ValueError(f"Country {country} not found")
                total_countries.append(country)
            for region in self.regions:
                rel_countries = list(
                    country_code_df[country_code_df["region"] == region][
                        identifier_value
                    ]
                )

                if len(rel_countries) == 0:
                    raise ValueError(f"Region {region} not found")
                total_countries += rel_countries

            for sub_region in self.sub_regions:
                rel_countries = list(
                    country_code_df[country_code_df["sub-region"] == sub_region][
                        identifier_value
                    ]
                )
                if len(rel_countries) == 0:
                    raise ValueError(f"Sub region {sub_region} not found")
                total_countries += rel_countries

            for intermediate_region in self.intermediate_regions:
                rel_countries = list(
                    country_code_df[
                        country_code_df["intermediate-region"] == intermediate_region
                    ][identifier_value]
                )
                if len(rel_countries) == 0:
                    raise ValueError(
                        f"Intermediate region {intermediate_region} not found"
                    )
                total_countries += rel_countries

            for custom_country in self.custom_countries:
                if identifier == Identifier.country_name:
                    total_countries.append(custom_country.name)
                elif (
                    identifier == Identifier.alpha_2
                    and custom_country.alpha_2 is not None
                ):
                    total_countries.append(custom_country.alpha_2)
                elif (
                    identifier == Identifier.alpha_3
                    and custom_country.alpha_3 is not None
                ):
                    total_countries.append(custom_country.alpha_3)

            self._all_countries[identifier] = list(set(total_countries))
            return self._all_countries[identifier]
        else:
            return self._all_countries[identifier]
