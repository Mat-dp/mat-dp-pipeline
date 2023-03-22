import logging
from pathlib import Path
from typing import ClassVar, Final

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource
from mat_dp_pipeline.data_sources.country_sets.country_set import Identifier
from mat_dp_pipeline.data_sources.country_sets.source_with_countries import (
    SourceWithCountries,
)

from .tech_map import TechMap, TechMapTypes, create_tech_map


class TMBATargetsSource(TargetsSource):
    _targets_csv: Path
    _targets_parameters: list[str]
    _grouping: Final[tuple[str, ...]] = ("country", "parameter")
    _tech_map: ClassVar[TechMap] = create_tech_map(TechMapTypes.TMBA)

    tail_labels: ClassVar[list[str]] = ["Parameter"]

    def __init__(
        self,
        target_csv: Path,
        targets_parameters: list[str],
        country_source: type[SourceWithCountries],
    ):
        self._targets_csv = target_csv
        self._targets_parameters = targets_parameters
        self._country_to_path = country_source.country_to_path(
            identifier=Identifier.alpha_2
        )

    def __call__(self, output_dir: Path) -> None:
        targets = pd.read_csv(self._targets_csv)
        targets = targets[targets["parameter"].isin(self._targets_parameters)]
        targets = targets.drop(columns=[targets.columns[0], "scenario"]).dropna()

        tech_tuples = targets["variable"].map(self._tech_map)
        if unmapped_variables := set(
            targets.loc[tech_tuples[tech_tuples.isna()].index, "variable"]
        ):
            logging.warning(
                f"The following variables don't have a corresponding mapping in the Tech Map: {unmapped_variables}. Ignoring them."
            )
            tech_tuples = tech_tuples.dropna()

        targets.pop("variable")  # no longer needed
        techs = pd.DataFrame.from_records(
            tech_tuples.to_list(),
            index=tech_tuples.index,
            columns=["Category", "Specific"],
        )
        targets = targets.join(techs, how="inner")

        grouping = list(self._grouping)  # list needed
        for key, targets_frame in targets.groupby(grouping):
            if key[0] == "NM":
                # Special case for namibia, as we override alpha 2
                new_key = "NA"
            else:
                new_key = key[0]
            path = (self._country_to_path[new_key],) + key[1:]
            path = Path(*path)
            location_dir = output_dir / path.relative_to("/")
            location_dir.mkdir(exist_ok=True, parents=True)
            targets_frame.drop(columns=grouping).to_csv(
                location_dir / self.file_name, index=False
            )
