from pathlib import Path
from typing import ClassVar, Final

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource
from mat_dp_pipeline.data_sources.utils import location_to_path

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
        parameter_to_category: dict[str, str] | None = None,
    ):
        self._targets_csv = target_csv
        self._targets_parameters = targets_parameters
        self._parameter_to_category = (
            parameter_to_category if parameter_to_category else PARAMETER_TO_CATEGORY
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
            # We know that _grouping[0] is country
            path_parts = (location_to_path(key[0]),) + key[1:]
            path = Path(*path_parts)

            location_dir = output_dir / path
            location_dir.mkdir(exist_ok=True, parents=True)
            targets_frame.drop(columns=grouping).to_csv(
                location_dir / self.file_name, index=False
            )
