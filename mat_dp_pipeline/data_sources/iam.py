import logging
from pathlib import Path
from typing import ClassVar, Final

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource
from mat_dp_pipeline.data_sources.country_sets.country_set import Identifier
from mat_dp_pipeline.data_sources.country_sets.source_with_countries import (
    SourceWithCountries,
)

from .common import map_technologies
from .tech_map import TechMap, TechMapTypes, create_tech_map


class IntegratedAssessmentModel(TargetsSource):
    _spreadsheet_path: Path
    _parameters: list[str]
    _country_to_path: dict[str, Path]
    _grouping: Final[tuple[str, ...]] = ("Region", "Model", "Scenario", "Parameter")
    _tech_map: ClassVar[TechMap] = create_tech_map(TechMapTypes.IAM)
    tail_labels: ClassVar[list[str]] = ["Model", "Scenario", "Parameter"]

    def __init__(
        self,
        spreadsheet_path: Path,
        parameters: list[str],
        country_source: type[SourceWithCountries],
    ) -> None:
        """Targets Data Sources for Integrated Assement Model (TIAM).

        The spreadsheet must have a tab called "DATA_TIAM".

        Args:
            spreadsheet_path (Path): path to the spreadsheet on the harddrive.
            parameters (list[str]): list of parameters such as "Primary Energy", "Final Energy".
                                    Essentially these are prefixes of values in `variable` column
                                    of the spreadsheet.
            country_source (type[SourceWithCountries]):
                Data sources with countries. This normally would be the type of intensities source
                used. This is required to guarantee correct country/region mapping to the paths between
                the intensities/indicators and the targets.
        """
        assert len(parameters) > 0
        self._spreadsheet_path = spreadsheet_path
        self._parameters = parameters
        self._country_to_path = (
            country_source.country_to_path(identifier=Identifier.alpha_2)
            | country_source.country_to_path(identifier=Identifier.alpha_3)
            | {"World": Path("/World")}
        )

        for i, p1 in enumerate(parameters):
            for j, p2 in enumerate(parameters):
                if i != j and (p1.startswith(p2) or p2.startswith(p1)):
                    raise ValueError(
                        f"Overlapping definition of parameters: {p1}, {p2}!"
                    )
        # TODO: add scaling based on unit - as a dict parameter with some defaults containing EJ/yr etc?
        # alternativelly, validate the units?

    def __call__(self, output_dir) -> None:
        targets = pd.read_excel(self._spreadsheet_path, sheet_name="DATA_TIAM")
        targets = targets.iloc[:, 1:]  # drop first columns
        targets.pop("Unit")
        targets.dropna(subset=["Region", "Scenario", "Model"], inplace=True)
        targets.fillna(0, inplace=True)

        # Create empty Parameter column. We'll be adding here the prefixes
        # of Variable column defined in self._parameters
        targets["Parameter"] = ""

        mask = pd.Series(False, index=targets.index)
        # Select only the variables requesteds by self._parameters
        for parameter in self._parameters:
            param_mask = targets["Variable"].str.startswith(parameter + "|")
            targets.loc[param_mask, "Parameter"] = parameter
            # Remove Parameter| prefix from Variable
            targets.loc[param_mask, "Variable"] = targets.loc[
                param_mask, "Variable"
            ].apply(lambda v: v[len(parameter) + 1 :])
            mask |= param_mask
        targets = targets[mask]
        print(targets)
        targets = map_technologies(targets, "Variable", self._tech_map)

        grouping = list(self._grouping)  # list needed
        for key, targets_frame in targets.groupby(grouping):
            # First element of grouping is Region!
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
            targets_frame.drop(columns=grouping).to_csv(
                location_dir / self.file_name, index=False
            )
