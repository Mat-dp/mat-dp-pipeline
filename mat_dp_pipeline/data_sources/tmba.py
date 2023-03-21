from pathlib import Path
from typing import ClassVar, Final

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource
from mat_dp_pipeline.data_sources.country_sets.country_set import Identifier
from mat_dp_pipeline.data_sources.country_sets.source_with_countries import (
    SourceWithCountries,
)

from .tech_map import TechMapTypes, create_tech_map

PARAMETER_TO_CATEGORY = {
    "Power Generation (Aggregate)": "Power plant",
    "Power Generation Capacity (Aggregate)": "Power plant",
    "New Power Generation Capacity (Aggregate)": "Power plant",
}


class TMBATargetsSource(TargetsSource):
    _targets_csv: Path
    _targets_parameters: list[str]
    _parameter_to_category: dict[str, str]
    _grouping: Final[tuple[str, ...]] = ("country", "parameter")
    _variable_to_specific: ClassVar[dict[str, str]] = create_tech_map(TechMapTypes.TMBA)

    tail_labels: ClassVar[list[str]] = ["Parameter"]

    def __init__(
        self,
        target_csv: Path,
        targets_parameters: list[str],
        country_source: type[SourceWithCountries],
        parameter_to_category: dict[str, str] | None = None,
    ):
        self._targets_csv = target_csv
        self._targets_parameters = targets_parameters
        self._parameter_to_category = (
            parameter_to_category if parameter_to_category else PARAMETER_TO_CATEGORY
        )
        self._country_to_path = country_source.country_to_path(
            identifier=Identifier.alpha_2
        )

    def __call__(self, output_dir: Path) -> None:
        targets = pd.read_csv(self._targets_csv)
        targets = targets[targets["parameter"].isin(self._targets_parameters)]
        targets = (
            targets.drop(columns=[targets.columns[0], "scenario"])
            .rename(columns={"variable": "Specific"})
            .dropna()
        )
        category = targets["parameter"].map(self._parameter_to_category)
        targets.insert(0, "Category", category)
        for pattern, replacement in self._variable_to_specific.items():
            # Remove (ignore) if there is no replacement, else replace
            if not replacement:
                targets = targets[targets["Specific"] != pattern]
            else:
                targets["Specific"] = targets["Specific"].str.replace(
                    pattern, replacement
                )

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
