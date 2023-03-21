import pdb
from pathlib import Path
from typing import ClassVar

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource
from mat_dp_pipeline.data_sources.country_sets.country_set import Identifier
from mat_dp_pipeline.data_sources.country_sets.source_with_countries import (
    SourceWithCountries,
)
from mat_dp_pipeline.data_sources.utils import location_to_path

PARAMETER_TO_CATEGORY = {
    "Power Generation (Aggregate)": "Power plant",
    "Power Generation Capacity (Aggregate)": "Power plant",
}

# From targets' "variable" to intensities "specific" name(s)
VARIABLE_TO_SPECIFIC: dict[str, str | None] = {
    "Biomass with ccs": "Biomass + CCS",
    "Coal with ccs": "Coal + CCS",
    "Gas with ccs": "Gas + CCS",
    "Hydro": "Hydro (medium)",
    "Wind": "Offshore wind",
    "power_trade": None,  # Don't keep it
}


class TMBATargetsSource(TargetsSource):
    _targets_csv: Path
    _targets_parameters: list[str]
    _parameter_to_category: dict[str, str]
    _variable_to_specific: dict[str, str | None]

    tail_labels: ClassVar[list[str]] = ["Parameter"]

    def __init__(
        self,
        target_csv: Path,
        targets_parameters: list[str],
        country_source: type[SourceWithCountries],
        parameter_to_category: dict[str, str] | None = None,
        variable_to_specific: dict[str, str | None] | None = None,
    ):
        self._targets_csv = target_csv
        self._targets_parameters = targets_parameters
        self._parameter_to_category = (
            parameter_to_category if parameter_to_category else PARAMETER_TO_CATEGORY
        )
        self._variable_to_specific = (
            variable_to_specific if variable_to_specific else VARIABLE_TO_SPECIFIC
        )
        self._country_to_path = country_source.country_to_path(
            identifier=Identifier.alpha_2
        )
        print(country_source.country_to_path(identifier=Identifier.country_name))

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
            # Remove (ignore) if None replacement, else replace
            if replacement is None:
                targets = targets[targets["Specific"] != pattern]
            else:
                targets["Specific"] = targets["Specific"].str.replace(
                    pattern, replacement
                )

        # TODO: move this. to __init__?
        grouping = ["country", "parameter"]
        for key, targets_frame in targets.groupby(grouping):
            # print(self._country_to_path)
            if key[0] == "NM":
                list(self._country_to_path.keys())
                pdb.set_trace()
                new_key = "NA"
            else:
                new_key = key[0]
            path = (self._country_to_path[new_key],) + key[1:]
            path = Path(*path)

            location_dir = output_dir / path
            location_dir.mkdir(exist_ok=True, parents=True)
            targets_frame.drop(columns=grouping).to_csv(
                location_dir / "targets.csv", index=False
            )
