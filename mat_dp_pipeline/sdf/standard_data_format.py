import logging
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pydantic

from mat_dp_pipeline.common import FileOrPath

Year = int

SDF_METADATA_FILE_NAME = "metadata.json"


def validate_tech_units(tech_metadata: pd.DataFrame) -> None:
    if tech_metadata.empty:
        return

    if (tech_metadata.loc[:, "Material Unit"].groupby(level=0).nunique() > 1).any():
        raise ValueError("There are tech categories with non-unique Material Unit!")

    if (tech_metadata.loc[:, "Production Unit"].groupby(level=0).nunique() > 1).any():
        raise ValueError("There are tech categories with non-unique Production Unit!")


class InputReader(ABC):
    @property
    @abstractmethod
    def file_pattern(self) -> re.Pattern:
        ...

    @abstractmethod
    def read(self, path: FileOrPath) -> pd.DataFrame:
        ...


class IntensitiesReader(InputReader):
    @property
    def file_pattern(self) -> re.Pattern:
        return re.compile(r"intensities_?([0-9]{4})?.csv")

    def read(self, path: FileOrPath) -> pd.DataFrame:
        str_cols = [
            "Category",
            "Specific",
            "Description",
            "Material Unit",
            "Production Unit",
        ]
        return pd.read_csv(
            path,
            index_col=["Category", "Specific"],
            dtype=defaultdict(np.float64, {c: str for c in str_cols}),
            na_values={c: "" for c in str_cols},
        )


class TargetsReader(InputReader):
    @property
    def file_pattern(self) -> re.Pattern:
        return re.compile("targets.csv")

    def read(self, path: FileOrPath) -> pd.DataFrame:
        return pd.read_csv(
            path,
            index_col=["Category", "Specific"],
            dtype=defaultdict(
                np.float64,
                {
                    "Category": str,
                    "Specific": str,
                },
            ),
        )


class IndicatorsReader(InputReader):
    @property
    def file_pattern(self) -> re.Pattern:
        return re.compile(r"indicators_?([0-9]{4})?.csv")

    def read(self, path: FileOrPath) -> pd.DataFrame:
        return pd.read_csv(
            path,
            index_col="Resource",
            dtype=defaultdict(np.float64, {"Resource": str}),
        )


class SDFMetadata(pydantic.BaseModel):
    """Metadata of the Standard Data Format.

    Args:
        main_label (str): Description of what the path in the SDF represents. Defaults to "Location"
        tail_labels (list[str]):
            Names of the lowest levels in the SDF. If none are provided, the whole
            path is described by `main_label`. If provided, for example -- ["Scenario", "Parameter"],
            it means that the lowest levels in the SDF refer to a name of a Scenario and a Parameter.
    """

    main_label: str = "Location"
    tail_labels: list[str] = []


@dataclass(frozen=True, eq=False, order=False)
class StandardDataFormat:
    name: str

    base_intensities: pd.DataFrame
    intensities_yearly: dict[Year, pd.DataFrame]

    base_indicators: pd.DataFrame
    indicators_yearly: dict[Year, pd.DataFrame]

    targets: pd.DataFrame | None
    children: dict[str, "StandardDataFormat"]

    tech_metadata: pd.DataFrame

    metadata: SDFMetadata

    def __post_init__(self):
        self.validate()

    def is_leaf(self) -> bool:
        return self.targets is not None

    def validate(self) -> None:
        def validate_yearly_keys(base: pd.DataFrame, yearly: dict[Year, pd.DataFrame]):
            base_keys = set(base.index.unique())

            for year, df in yearly.items():
                if not set(df.index.unique()) <= base_keys:
                    raise ValueError(
                        f"{self.name}: Yearly file ({year}) introduces new items!"
                    )

        if self.base_intensities is None and self.intensities_yearly is not None:
            raise ValueError(
                f"{self.name}: No base intensities, while yearly files provided!"
            )

        if self.base_indicators is None and self.indicators_yearly is not None:
            raise ValueError(
                f"{self.name}: No base indicators, while yearly files provided!"
            )

        validate_yearly_keys(self.base_intensities, self.intensities_yearly)
        validate_yearly_keys(self.base_indicators, self.indicators_yearly)

        try:
            validate_tech_units(self.tech_metadata)
        except ValueError as e:
            # This isn't a problem just yet - it's possible that the ones with
            # more than one distinct unit won't be in the targets. It won't bother
            # us then. Just warn for now. We'll validate again for the calculation.
            logging.warning(e)

    def _prepare_output_dir(self, root_dir: Path, is_root: bool) -> Path:
        output_dir = root_dir
        if not is_root:
            output_dir /= self.name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _save_intensities(self, root_dir: Path, is_root: bool = False) -> None:
        def add_metadata(intensities: pd.DataFrame):
            df = intensities.join(self.tech_metadata)
            assert len(df) == len(intensities)
            cols = self.tech_metadata.columns.to_list() + intensities.columns.to_list()
            return df.loc[:, cols]

        output_dir = self._prepare_output_dir(root_dir, is_root)

        if not self.base_intensities.empty:
            add_metadata(self.base_intensities).to_csv(output_dir / "intensities.csv")
        for year, intensities in self.intensities_yearly.items():
            add_metadata(intensities).to_csv(output_dir / f"intensities_{year}.csv")

        for sdf in self.children.values():
            sdf._save_intensities(output_dir)

    def save_intensities(self, root_dir: Path) -> None:
        self._save_intensities(root_dir, is_root=True)

    def _save_indicators(self, root_dir: Path, is_root: bool = False) -> None:
        output_dir = self._prepare_output_dir(root_dir, is_root)

        if not self.base_indicators.empty:
            self.base_indicators.to_csv(output_dir / "indicators.csv")
        for year, indicators in self.indicators_yearly.items():
            indicators.to_csv(output_dir / f"indicators_{year}.csv")

        for sdf in self.children.values():
            sdf._save_indicators(output_dir)

    def save_indicators(self, root_dir: Path) -> None:
        self._save_indicators(root_dir, is_root=True)

    def _save_targets(self, root_dir: Path, is_root: bool = False) -> None:
        output_dir = self._prepare_output_dir(root_dir, is_root)

        if self.targets is not None:
            self.targets.to_csv(output_dir / "targets.csv")
        else:
            for sdf in self.children.values():
                sdf._save_targets(output_dir)

    def save_targets(self, root_dir: Path) -> None:
        self._save_targets(root_dir, is_root=True)

    def save_metadata(self, root_dir: Path) -> None:
        with open(root_dir / SDF_METADATA_FILE_NAME, "w") as f:
            f.write(self.metadata.json())

    def save(self, root_dir: Path) -> None:
        self.save_intensities(root_dir)
        self.save_indicators(root_dir)
        self.save_targets(root_dir)
        self.save_metadata(root_dir)


def load(input_dir: Path) -> StandardDataFormat:
    assert input_dir.is_dir()
    targets_reader = TargetsReader()
    intensities_reader = IntensitiesReader()
    indicators_reader = IndicatorsReader()

    metadata_file = Path(input_dir / SDF_METADATA_FILE_NAME)

    def dfs(node: Path, is_root: bool) -> StandardDataFormat | None:
        sub_directories = list(filter(lambda p: p.is_dir(), node.iterdir()))

        base_intensities: pd.DataFrame | None = None
        intensities_yearly: dict[Year, pd.DataFrame] = {}
        base_indicators: pd.DataFrame | None = None
        indicators_yearly: dict[Year, pd.DataFrame] = {}
        targets: pd.DataFrame | None = None
        children: dict[str, StandardDataFormat] = {}

        files = filter(lambda f: f.is_file(), node.iterdir())
        for file in files:
            if match := intensities_reader.file_pattern.match(file.name):
                year = match.group(1)
                df = intensities_reader.read(file)
                if year is None:
                    base_intensities = df
                else:
                    year = Year(year)
                    intensities_yearly[year] = df
            elif match := indicators_reader.file_pattern.match(file.name):
                year = match.group(1)
                df = indicators_reader.read(file)
                if year is None:
                    base_indicators = df
                else:
                    year = Year(year)
                    indicators_yearly[year] = df
            elif targets_reader.file_pattern.match(file.name):
                targets = targets_reader.read(file)

        for sub_directory in sub_directories:
            leaf = dfs(sub_directory, False)
            if leaf is not None:
                children[sub_directory.name] = leaf

        # If not intensities or indicators were provided, use empty ones
        base_intensities = (
            pd.DataFrame() if base_intensities is None else base_intensities
        )
        base_indicators = pd.DataFrame() if base_indicators is None else base_indicators

        # Ignore leaves with no targets specified
        if (
            targets is None
            and base_intensities is None
            and base_indicators is None
            and not sub_directories
        ):
            logging.warning(f"No files found in {node.name}. Ignoring.")
            return None
        else:
            # *Move* metadata from all intensity frames into tech_metadata
            tech_metadata_cols = ["Description", "Material Unit", "Production Unit"]
            all_intensities = list(intensities_yearly.values()) + [base_intensities]
            all_metadata = [
                i.loc[:, tech_metadata_cols] for i in all_intensities if not i.empty
            ]
            if all_metadata:
                tech_metadata = pd.concat(all_metadata).groupby(level=(0, 1)).last()
            else:
                tech_metadata = pd.DataFrame()

            for intensities in filter(lambda df: not df.empty, all_intensities):
                intensities.drop(columns=tech_metadata_cols, inplace=True)

            if is_root and metadata_file.exists():
                metadata = SDFMetadata.parse_file(metadata_file)
            else:
                metadata = SDFMetadata()

            return StandardDataFormat(
                name="/" if is_root else node.name,
                base_intensities=base_intensities,
                intensities_yearly=intensities_yearly,
                base_indicators=base_indicators,
                indicators_yearly=indicators_yearly,
                targets=targets,
                children=children,
                tech_metadata=tech_metadata,
                metadata=metadata,
            )

    root_dfs = dfs(input_dir, True)
    assert root_dfs is not None
    return root_dfs
