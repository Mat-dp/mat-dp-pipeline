import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterator

import pandas as pd

from mat_dp_pipeline.common import Tree, create_path_tree
from mat_dp_pipeline.pipeline.common import SparseYearsInput
from mat_dp_pipeline.sdf import StandardDataFormat, Year


def stringify_tree(tree: Tree, collapse_limit: int | None = None) -> list[str]:
    def add_node_to_result(node: Tree, result: list[str], prefix: str):
        """
        Adds the specified node to the result list, via a depth first search.
        """
        if node is None:
            return None

        for i, (k, v) in enumerate(node.items()):
            if i == 0:
                new_prefix = f"{prefix} -> {k}"
            else:
                new_prefix = f"{len(prefix)*' '} -> {k}"
            if v is None:
                result.append(new_prefix)
            elif len(v) > (collapse_limit or float("inf")):
                result.append(new_prefix + f" -> ({len(v)} items)")
            else:
                add_node_to_result(v, result, new_prefix)

    result = []
    add_node_to_result(tree, result, "")

    return result


def overlay_in_order(
    df: pd.DataFrame,
    base_overlay: pd.DataFrame,
    yearly_overlays: dict[Year, pd.DataFrame],
) -> pd.DataFrame:
    overlaid = df

    # Overlays sorted by year, first one being base_overlay (year 0)
    sorted_overlays = sorted(({Year(0): base_overlay} | yearly_overlays).items())

    for year, overlay in sorted_overlays:
        if overlay.empty:
            continue

        # Add "Year" level to the index. Concat is idiomatic way of doing it
        update_df = pd.concat({year: overlay}, names=["Year"])

        if overlaid.empty:
            overlaid = update_df.copy()
        else:
            overlaid = update_df.combine_first(overlaid)

    return overlaid


def flatten_hierarchy(
    root_sdf: StandardDataFormat,
) -> list[tuple[Path, SparseYearsInput]]:
    def dfs(
        sdf: StandardDataFormat, sparse_years: SparseYearsInput, label: Path
    ) -> Iterator[tuple[Path, SparseYearsInput, set[str]]]:
        if not (
            sdf.base_indicators.empty
            or sparse_years.indicators.empty
            or list(sparse_years.indicators.columns)
            == list(sdf.base_indicators.columns)
        ):
            raise ValueError(
                f"{label}: Indicators' names on each level have to be the same!"
            )

        overlaid = sparse_years.copy()
        overlaid.intensities = overlay_in_order(
            overlaid.intensities, sdf.base_intensities, sdf.intensities_yearly
        )
        overlaid.indicators = overlay_in_order(
            overlaid.indicators, sdf.base_indicators, sdf.indicators_yearly
        )
        if overlaid.tech_metadata.empty:
            overlaid.tech_metadata = sdf.tech_metadata
        else:
            overlaid.tech_metadata = (
                pd.concat([overlaid.tech_metadata, sdf.tech_metadata])
                .groupby(level=(0, 1))
                .last()
            )

        # Go down in the hierarchy
        for name, directory in sdf.children.items():
            yield from dfs(directory, overlaid, label / name)

        # Yield only leaves
        if not sdf.children:
            assert sdf.targets is not None
            overlaid.targets = sdf.targets
            # Trim tech_meta to the techs specified in targets
            overlaid.tech_metadata = overlaid.tech_metadata.reindex(
                overlaid.targets.index
            )

            mismatched_resources = overlaid.validate()
            yield label, overlaid, mismatched_resources

    initial = SparseYearsInput(
        intensities=pd.DataFrame(),
        targets=pd.DataFrame(),
        indicators=pd.DataFrame(),
        tech_metadata=pd.DataFrame(),
    )

    flattened = []
    all_mismatched_resources: dict[tuple[str, ...], list[Path]] = defaultdict(list)
    for label, sparse_years, mismatched_resources in dfs(
        root_sdf, initial, Path(root_sdf.name)
    ):
        flattened.append((label, sparse_years))
        if mismatched_resources:
            all_mismatched_resources[tuple(sorted(mismatched_resources))].append(label)

    for resources, paths in all_mismatched_resources.items():
        tree_lines = "\n        ".join(
            stringify_tree(create_path_tree(paths), collapse_limit=7)
        )
        logging.warning(f"Mismatched resources: {resources}!\n        {tree_lines}")

    return flattened
