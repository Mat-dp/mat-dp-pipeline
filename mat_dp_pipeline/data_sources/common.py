import logging

import pandas as pd
import country_converter as coco

from .tech_map import TechMap


def map_technologies(
    targets: pd.DataFrame, variable_column: str, tech_map: TechMap
) -> pd.DataFrame:
    """Map `variable_column` of `targets` into technologies by using `tech_map`.
    Attach the resulting Category & Specific columns to `targets` in the result.

    Unmapped values are ignored and warning is produced.

    Args:
        targets (pd.DataFrame): Targets DataFrame
        variable_column (str): column in `targets` to be used as a key in `tech_map`
        tech_map (TechMap): Technology mapping

    Returns:
        pd.DataFrame: `targets` with mapped Category & Specific columns.
    """
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

def match_countries(list_names: list[str]):
    shorts = coco.convert(names = list_names, to = 'name_short', not_found = None)
    iso3_codes = coco.convert(names = list_names, to = 'ISO3', not_found = None)
    
    tis_d = pd.DataFrame(list(zip(list_names,shorts)), columns = ['Country','aCountry'])
    tis_d = tis_d.set_index('Country')['aCountry'].to_dict()

    ti3_d = pd.DataFrame(list(zip(shorts,iso3_codes)), columns = ['Region','ISO3'])
    ti3_d = ti3_d.set_index('Region')['ISO3'].to_dict()
    
    list_names_a = list_names.map(tis_d)
    list_names_ISO3 = list_names_a.map(ti3_d)
    
    return list_names_ISO3
