import logging

import pandas as pd

from .tech_map import TechMap


def map_technologies(
    targets: pd.DataFrame, variable_column: str, tech_map: TechMap
) -> pd.DataFrame:
    tech_tuples = targets[variable_column].map(tech_map)
    if unmapped_variables := set(
        targets.loc[tech_tuples[tech_tuples.isna()].index, variable_column]
    ):
        logging.warning(
            f"The following variables don't have a corresponding mapping in the Tech Map: {unmapped_variables}. Ignoring them."
        )
        tech_tuples = tech_tuples.dropna()

    targets.pop(variable_column)  # no longer needed
    techs = pd.DataFrame.from_records(
        tech_tuples.to_list(),
        index=tech_tuples.index,
        columns=["Category", "Specific"],
    )
    return targets.join(techs, how="inner")
