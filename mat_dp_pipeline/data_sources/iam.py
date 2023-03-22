from pathlib import Path
from typing import Final

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource

# variable to Category, Specific
VariableToTech = dict[str, tuple[str, str]]


def location_to_path(location: str) -> Path:
    if location == "AFR":
        return Path("World/Africa")
    else:
        return Path("World/Other")
    # TODO:


class IntegratedAssessmentModel(TargetsSource):
    _spreadsheet_path: Path
    _parameters: list[str]
    _variable_to_tech: VariableToTech
    _grouping: Final[tuple[str, ...]] = ("Region", "Model", "Scenario", "Parameter")

    def __init__(
        self,
        spreadsheet_path: Path,
        parameters: list[str],
        variable_to_tech: VariableToTech,
    ) -> None:
        """Targets Data Sources for Integrated Assement Model (TIAM).

        The spreadsheet must have a tab called "DATA_TIAM".

        Args:
            spreadsheet_path (Path): path to the spreadsheet on the harddrive.
            parameters (list[str]): list of parameters such as "Primary Energy", "Final Energy".
                                    Essentially these are prefixes of values in `variable` column
                                    of the spreadsheet.
            variable_to_tech (VariableToTech): mapping from `Variable` to (Category, Specific)
        """
        assert len(parameters) > 0
        self._spreadsheet_path = spreadsheet_path
        self._parameters = parameters
        self._variable_to_tech = variable_to_tech
        # TODO: add scaling based on unit - as a dict parameter with some defaults containing EJ/yr etc?

    def __call__(self, output_dir) -> None:
        targets = pd.read_excel(self._spreadsheet_path, sheet_name="DATA_TIAM")
        targets = targets.iloc[:, 1:]  # drop first columns
        targets.pop("Unit")  # TODO:should we validate somehow?
        targets.dropna(subset=["Region", "Scenario", "Model"], inplace=True)
        targets.fillna(0, inplace=True)

        # Select only the variables requesteds by self._parameters
        mask = targets["Variable"].str.startswith(self._parameters[0])
        for parameter in self._parameters[1:]:
            mask |= targets["Variable"].str.startswith(parameter)
        targets = targets[mask]
        # TODO: move the prefix of Variable into another column - call it "Parameter"

        # drop Variable, add Category + Specific
        # TODO: do not ignore unmapped!
        tech_tuples = targets.pop("Variable").map(self._variable_to_tech).dropna()
        techs = pd.DataFrame.from_records(
            tech_tuples.to_list(),
            index=tech_tuples.index,
            columns=["Category", "Specific"],
        )
        targets = targets.join(techs).dropna()  # TODO: remove dropna!

        grouping = list(self._grouping)  # list needed
        for key, targets_frame in targets.groupby(grouping):
            # First element of grouping is Region!
            path_parts = (location_to_path(key[0]),) + key[1:]
            path = Path(*path_parts)

            location_dir = output_dir / path
            location_dir.mkdir(exist_ok=True, parents=True)
            targets_frame.drop(columns=grouping).to_csv(
                location_dir / self.file_name, index=False
            )
