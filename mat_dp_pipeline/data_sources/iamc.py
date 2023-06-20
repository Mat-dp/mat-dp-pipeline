import logging
from pathlib import Path
from typing import ClassVar, Final
import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource
from mat_dp_pipeline.data_sources.country_sets import Identifier, SourceWithCountries

from .common import map_technologies, match_countries
from .tech_map import TechMap, TechMapTypes, create_tech_map


class IntegratedAssessmentModelc(TargetsSource):
    _targets: pd.DataFrame
    _parameters: list[str]
    _country_to_path: dict[str, Path]
    _grouping: Final[list[str]] = ["Region", "Model", "Scenario", "Parameter"]
    _tech_map: ClassVar[TechMap] = create_tech_map(TechMapTypes.IAMc)
    tail_labels: ClassVar[list[str]] = ["Model", "Scenario", "Parameter"]
    conversion_table: dict[str, float] = {
        "GW/yr": 1000,  # MW/year
    }

    def __init__(
        self,
        targets: pd.DataFrame,
        parameters: list[str],
        country_source: type[SourceWithCountries],
    ) -> None:
        """Targets Data Sources for Integrated Assessment Model (IAM).

        Args:
            targets (pd.DataFrame): The dataframe for targets.
            parameters (list[str]): List of parameters such as "Capacity Additions|Electricity".
                                    Essentially, these are prefixes of values in the `IAMvariable` column
                                    of the spreadsheet.
            country_source (type[SourceWithCountries]):
                Data sources with countries. This normally would be the type of intensities source
                used. This is required to guarantee correct country/region mapping to the paths between
                the intensities/indicators and the targets.
        """
        if not parameters:
            raise ValueError("You must specify parameters.")

        self._targets = targets
        self._parameters = parameters
        self._country_to_path = country_source.country_to_path(
            identifier=Identifier.alpha_2
        ) | country_source.country_to_path(identifier=Identifier.alpha_3)

        for i, p1 in enumerate(parameters):
            for j, p2 in enumerate(parameters):
                if i != j and (p1.startswith(p2) or p2.startswith(p1)):
                    raise ValueError(
                        f"Overlapping definition of parameters: {p1}, {p2}!"
                    )
    
    
    @classmethod
    def from_csv(
        cls,
        csv: str | Path,
        parameters: list[str],
        country_source: type[SourceWithCountries],
        **pandas_kwargs,
    ):
        source = pd.read_csv(csv, **pandas_kwargs)
        print(source.head())
        source['Region'] = match_countries(source['Region'])
        print(source.head())
        return cls(
            source,
            country_source=country_source,
            parameters=parameters,
        )
    
    def __call__(self, output_dir) -> None:
        targets = self._targets

        # Scale the units as required and remove the Unit column
        first_year_col_name = targets.columns[
            targets.columns.to_list().index("Unit") + 1
        ]
        for unit, factor in self.conversion_table.items():
            targets.loc[targets["Unit"] == unit, first_year_col_name:] *= factor
        targets.pop("Unit")

        # targets = targets.iloc[:, 1:]  # drop the first columns
        targets.dropna(subset=["Region", "Scenario", "Model"], inplace=True)
        targets.fillna(0, inplace=True)

        # Create an empty Parameter column. We'll be adding the prefixes
        # of the Variable column defined in self._parameters
        targets["Parameter"] = None

        mask = pd.Series(False, index=targets.index)
        # Select only the variables requested by self._parameters
        for parameter in self._parameters:
            param_mask = targets["Variable"].str.startswith(parameter + "|")
            targets.loc[param_mask, "Parameter"] = parameter
            # Remove the Parameter| prefix from the Variable
            targets.loc[param_mask, "Variable"] = targets.loc[
                param_mask, "Variable"
            ].apply(lambda v: v[len(parameter) + 1:])
            mask |= param_mask

        # Narrow down targets to the requested parameters
        targets = targets[mask]
        assert not any(targets["Parameter"].isna())
        if targets.empty:
            logging.warning("Targets for selected parameters are empty!")
            return

        targets = map_technologies(targets[mask], "Variable", self._tech_map)

        for key, targets_frame in targets.groupby(self._grouping):
            # The first element of grouping is Region!
            try:
                path = (self._country_to_path[key[0]],) + key[1:]
            except KeyError as e:
                logging.warning(
                    f"Unknown location: {e}. Consider adding it to the intensities' country set"
                )
                continue
            path = Path(*path)
            location_dir = output_dir / path.relative_to("/")
            location_dir.mkdir(exist_ok=True, parents=True)
            targets_frame.drop(columns=self._grouping).to_csv(
                location_dir / self.file_name, index=False
            )
