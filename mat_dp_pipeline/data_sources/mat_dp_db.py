from pathlib import Path

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import IndicatorsSource, IntensitiesSource
from mat_dp_pipeline.data_sources.country_sets import (
    CountrySet,
    CountrySets,
    CustomCountry,
    SourceWithCountries,
)

mat_dp_names_to_paths = {
    "General": "/",
    "North America": "/North America",
    "Central and South America": "/Central and South America",
    "Africa": "/Africa",
    "Europe": "/Europe",
    "Middle East and Central Asia": "/Middle East and Central Asia",
    "South and East Asia": "/South and East Asia",
    "Oceania": "/Oceania",
}


mat_dp_country_sets: CountrySets = {
    "North America": CountrySet(
        sub_regions=["Northern America"],
    ),
    "Central and South America": CountrySet(
        sub_regions=["Latin America and the Caribbean"],
        custom_countries=[
            CustomCountry(name="Central and South America", alpha_2=None, alpha_3="CSA")
        ],
    ),
    "Africa": CountrySet(
        regions=["Africa"],
        custom_countries=[CustomCountry(name="Africa", alpha_2=None, alpha_3="AFR")],
    ),
    "Europe": CountrySet(
        regions=["Europe"],
        custom_countries=[
            CustomCountry(name="Former Soviet Union", alpha_2=None, alpha_3="FSU"),
            CustomCountry(name="Europe", alpha_2="EU", alpha_3=None),
            CustomCountry(name="Eastern Europe", alpha_2=None, alpha_3="EEU"),
            CustomCountry(name="Western Europe", alpha_2=None, alpha_3="WEU"),
        ],
    ),
    "Middle East and Central Asia": CountrySet(
        sub_regions=["Western Asia", "Central Asia"],
        custom_countries=[
            CustomCountry(name="Middle-east", alpha_2=None, alpha_3="MEA"),
        ],
    ),
    "South and East Asia": CountrySet(
        sub_regions=["Eastern Asia", "Southern Asia", "South-eastern Asia"],
        custom_countries=[
            CustomCountry(name="Other Developing Asia", alpha_2=None, alpha_3="ODA"),
        ],
    ),
    "Oceania": CountrySet(
        regions=["Oceania"],
        custom_countries=[
            CustomCountry(name="Oceania", alpha_2="OC", alpha_3=None),
        ],
    ),
    "World": CountrySet(
        custom_countries=[CustomCountry(name="World", alpha_2="World", alpha_3="World")]
    ),
}


class MatDPDBIntensitiesSource(
    SourceWithCountries,
    IntensitiesSource,
    country_sets=mat_dp_country_sets,
    names_to_paths=mat_dp_names_to_paths,
):
    _materials_spreadsheet: Path

    def __init__(self, materials_spreadsheet: Path):
        self._materials_spreadsheet = materials_spreadsheet

    def _raw(self) -> pd.DataFrame:
        df = (
            pd.read_excel(
                self._materials_spreadsheet,
                sheet_name="Material intensities",
                header=1,
            )
            .drop(
                columns=[
                    "Total",
                    "Comments",
                    "Data collection responsible",
                    "Data collection date",
                    "Vehicle/infrastructure primary purpose",
                ]
            )
            .rename(
                columns={
                    "Technology category": "Category",
                    "Technology name": "Specific",
                    "Technology description": "Description",
                }
            )
        )
        units = df["Units"].str.split("/", n=1, expand=True)
        df.pop("Units")
        df.insert(3, "Production Unit", units.iloc[:, 1])
        df.insert(3, "Material Unit", units.iloc[:, 0])
        return df

    def __call__(self, output_dir: Path) -> None:
        ## Drop NaN based on resource value columns only
        # df = df.dropna(subset=df.columns[6:])
        for location, intensities in self._raw().groupby("Location"):
            path = self.name_to_path(str(location))
            df = intensities.drop(columns=["Location"])
            location_dir = output_dir / path.relative_to("/")
            location_dir.mkdir(exist_ok=True, parents=True)
            df.to_csv(location_dir / self.base_file_name, index=False)


class MatDPDBIndicatorsSource(IndicatorsSource):
    _materials_spreadsheet: Path

    def __init__(self, materials_spreadsheet: Path):
        self._materials_spreadsheet = materials_spreadsheet

    def __call__(self, output_dir: Path) -> None:
        output_dir.mkdir(exist_ok=True, parents=True)
        (
            pd.read_excel(self._materials_spreadsheet, sheet_name="Material emissions")
            .drop(
                columns=[
                    "Material description",
                    "Object title in Ecoinvent",
                    "Location of dataset",
                    "Notes",
                ]
            )
            .rename(columns={"Material code": "Resource"})
            .dropna()
            .to_csv(output_dir / self.base_file_name, index=False)
        )
