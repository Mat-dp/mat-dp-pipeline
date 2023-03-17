
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar
import pandas as pd


@dataclass
class CountrySet:
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
        if not hasattr(self, '_all_countries'):
            total_countries = []
            available_countries = list(self.country_code_df['name'])
            for country in self.countries:
                if country not in available_countries:
                    raise ValueError(f'Country {country} not found')
                total_countries.append(country)

            for region in self.regions:
                rel_countries = list(self.country_code_df[self.country_code_df["region"] == region]["name"])
                if len(rel_countries) == 0:
                    raise ValueError(f'Region {region} not found')
                total_countries += rel_countries

            for sub_region in self.sub_regions:
                rel_countries = list(self.country_code_df[self.country_code_df["sub-region"] == sub_region]["name"])
                if len(rel_countries) == 0:
                    raise ValueError(f'Sub region {sub_region} not found')
                total_countries += rel_countries

            for intermediate_region in self.intermediate_regions:
                rel_countries = list(self.country_code_df[self.country_code_df["intermediate-region"] == intermediate_region]["name"])
                if len(rel_countries) == 0:
                    raise ValueError(f'Intermediate region {intermediate_region} not found')
                total_countries += rel_countries
            self._all_countries = total_countries
        return self._all_countries



countries = pd.read_csv('./country_codes.csv')
class MyCountrySet(CountrySet, country_code_df=countries):
    pass

test = MyCountrySet(regions=['Africa'])
CountrySetDescriptorType = dict[str, "CountrySet | CountrySetDescriptorType"]

structure: CountrySetDescriptorType = {
	'General': {
		'The north americas': MyCountrySet(regions=['Americas']),
		'Europe': MyCountrySet(regions=['Europe']),
		'__root__': MyCountrySet(regions=['Africa'])
	}
}

def f(country_sets: CountrySetDescriptorType) -> dict[str, Path]:
    Interdtype = dict[str, "list[str] | Interdtype"]
    final_dict: dict[str, list[str]] = {}
    def dfs(country_sets: CountrySetDescriptorType, prefix: list[str]):
        for k, v in country_sets.items():
            if isinstance(v, CountrySet):
                all_countries = v.all_countries
                for country in all_countries:
                    if country in final_dict:
                        raise ValueError(f'Overlapped disjoint set detected for {country}')
                    else:
                        if k =='__root__':
                            final_dict[country] = prefix
                        else:
                            final_dict[country] = prefix + [k]
            else:
                dfs(v, prefix=prefix + [k])

    dfs(country_sets, prefix = [])
    path_dict = {k: Path(*v) for k, v in final_dict.items()}
    return path_dict
	
print(f(structure))
