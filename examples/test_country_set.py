from pathlib import Path
from pprint import pprint

import pandas as pd

from mat_dp_pipeline.data_sources.country_sets import (
    CountrySet,
    CountrySets,
    SourceWithCountries,
)

# Example really begins

country_sets: CountrySets = {
    "General": {
        "The north americas": CountrySet(regions=["Americas"]),
        "Europe": CountrySet(regions=["Europe"]),
        "__root__": CountrySet(regions=["Africa"]),
    }
}


names_to_paths = {
    "The north americas": Path("/The north americas"),
    "The Great Candad": Path("/The north americas/Canada"),
    "General": Path("/"),
}


class MatDpIntensitiesSource(
    SourceWithCountries,
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


# int_source = MatDpIntensitiesSource(Path("Material_intensities_database.xlsx"))
# target_source = MatDpTargetsSource(
#     Path("examples/results_1.5deg.csv"), int_source.country_to_path
# )


mat_dp_names_to_paths = {
    "General": "/",
    "North America": "/North America",
    "Canada": "/North America/Canada",
    "Central and South America": "/Central and South America",
    "CHL": "/Central and South America/Chile",
    "Africa": "/Africa",
    "Europe": "/Europe",
    "Middle East and Central Asia": "/Middle East and Central Asia",
    "South and East Asia": "/South and East Asia",
    "Oceania": "/Oceania",
}

mat_dp_country_sets: CountrySets = {
    "North America": CountrySet(sub_regions=["Northern America"]),
    "Central and South America": CountrySet(
        sub_regions=["Latin America and the Caribbean"]
    ),
    "Africa": CountrySet(regions=["Africa"], custom_countries=["Africa"]),
    "Europe": CountrySet(regions=["Europe"]),
    "Middle East and Central Asia": CountrySet(
        sub_regions=["Western Asia", "Central Asia"]
    ),
    "South and East Asia": CountrySet(
        sub_regions=["Eastern Asia", "Southern Asia", "South-eastern Asia"]
    ),
    "Oceania": CountrySet(regions=["Oceania"]),
}


class MatDpTestSource(
    SourceWithCountries,
    country_sets=mat_dp_country_sets,
    names_to_paths=mat_dp_names_to_paths,
):
    pass


pprint(MatDpTestSource.country_to_path)


print(MatDpTestSource.missing_countries)
