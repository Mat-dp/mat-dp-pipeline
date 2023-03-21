__all__ = ["CountrySet", "CustomCountry"]

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from .identifier import Identifier


@dataclass
class CustomCountry:
    name: str
    alpha_2: Optional[str]
    alpha_3: Optional[str]


def _append_with_regions(
    regions: list[str],
    region_key: str,
    country_code_df: pd.DataFrame,
    identifier: Identifier,
    total_countries: list[str],
    country_names: list[str],
):
    for region in regions:
        rel_countries = list(
            country_code_df[country_code_df[region_key] == region][identifier.value]
        )

        if len(rel_countries) == 0:
            raise ValueError(f"{region_key} {region} not found")
        total_countries += rel_countries

        rel_names = list(
            country_code_df[country_code_df[region_key] == region][
                Identifier.country_name.value
            ]
        )
        country_names += rel_names


@dataclass
class CountrySet:
    countries: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    sub_regions: list[str] = field(default_factory=list)
    intermediate_regions: list[str] = field(default_factory=list)
    custom_countries: list[CustomCountry] = field(default_factory=list)
    _all_countries: dict[Identifier, list[tuple[str, str]]] = field(
        default_factory=dict, init=False, repr=False
    )

    def all_countries(self, country_code_df: pd.DataFrame, identifier: Identifier):
        if identifier not in self._all_countries:
            total_countries = []
            country_names = []
            available_countries = list(country_code_df[Identifier.country_name.value])
            for country in self.countries:
                if country not in available_countries:
                    raise ValueError(f"Country {country} not found")
                total_countries.append(country)
                country_names.append(country)

            _append_with_regions(
                self.regions,
                "region",
                country_code_df,
                identifier,
                total_countries,
                country_names,
            )
            _append_with_regions(
                self.sub_regions,
                "sub-region",
                country_code_df,
                identifier,
                total_countries,
                country_names,
            )
            _append_with_regions(
                self.intermediate_regions,
                "intermediate-region",
                country_code_df,
                identifier,
                total_countries,
                country_names,
            )

            for custom_country in self.custom_countries:
                if identifier == Identifier.country_name:
                    total_countries.append(custom_country.name)
                    country_names.append(custom_country.name)
                elif (
                    identifier == Identifier.alpha_2
                    and custom_country.alpha_2 is not None
                ):
                    total_countries.append(custom_country.alpha_2)
                    country_names.append(custom_country.name)
                elif (
                    identifier == Identifier.alpha_3
                    and custom_country.alpha_3 is not None
                ):
                    total_countries.append(custom_country.alpha_3)
                    country_names.append(custom_country.name)

            self._all_countries[identifier] = list(
                set(zip(total_countries, country_names))
            )
            return self._all_countries[identifier]
        else:
            return self._all_countries[identifier]
