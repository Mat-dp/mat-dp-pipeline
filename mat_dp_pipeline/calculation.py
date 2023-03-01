from dataclasses import dataclass
import itertools

import numpy as np
import pandas as pd

from mat_dp_pipeline.sdf_to_input import ProcessableInput


@dataclass(frozen=True)
class ProcessedOutput:
    required_resources: pd.DataFrame
    emissions: pd.DataFrame

    @property
    def indicators(self) -> set[str]:
        """Get a set of indicators stored in `emissions`.

        Returns:
            set[str]: Set of indicator names
        """
        return set(self.emissions.index.get_level_values(0).to_list())

    def emissions_for_indicator(self, indicator: str):
        return self.emissions.loc[indicator]


def calculate(inpt: ProcessableInput) -> ProcessedOutput:
    required_resources = inpt.intensities.mul(inpt.targets, axis="index").rename_axis(
        index=["Category", "Specific"], columns=["Resource"]
    )

    index = pd.MultiIndex.from_tuples(
        (
            (ind, *tech)
            for ind, tech in itertools.product(
                inpt.indicators.columns, inpt.intensities.index
            )
        ),
        names=["Indicator", "Category", "Specific"],
    )
    emissions = pd.DataFrame(
        np.einsum(
            "ij,jk->kij", required_resources.values, inpt.indicators.values
        ).reshape(len(index), -1),
        index=index,
        columns=inpt.intensities.columns,
    ).rename_axis(columns="Resource")

    # emissions_dict: dict[str, pd.DataFrame] = {
    #     str(indicator): required_resources.mul(inpt.indicators[indicator])
    #     for indicator in inpt.indicators.columns
    # }

    # Move indicators to cols
    # emissions = pd.concat(emissions_dict, names=["Indicator"]).rename_axis(
    #     columns="Resource"
    # )
    assert isinstance(emissions, pd.DataFrame)
    return ProcessedOutput(required_resources=required_resources, emissions=emissions)
