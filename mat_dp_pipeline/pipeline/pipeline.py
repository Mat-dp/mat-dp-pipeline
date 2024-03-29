from collections import defaultdict
from dataclasses import dataclass
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Iterator, overload

import pandas as pd

from mat_dp_pipeline.pipeline.calculation import ProcessedOutput, calculate
from mat_dp_pipeline.pipeline.common import ProcessableInput, SparseYearsInput
from mat_dp_pipeline.pipeline.flatten_hierarchy import flatten_hierarchy
from mat_dp_pipeline.pipeline.sparse_to_processable_input import to_processable_input
from mat_dp_pipeline.sdf import SDFMetadata, StandardDataFormat, Year


@dataclass(frozen=True)
class LabelledOutput(ProcessedOutput):
    year: Year
    path: Path


class PipelineOutput:
    """The processed data from the pipeline in an easily accessible form."""

    _by_year: dict[Year, dict[Path, LabelledOutput]]
    _by_path: dict[Path, dict[Year, LabelledOutput]]
    _length: int
    _indicators: set[str]
    _tech_metadata: pd.DataFrame

    metadata: SDFMetadata

    def __init__(
        self,
        data: list[LabelledOutput],
        tech_metadata: pd.DataFrame,
        metadata: SDFMetadata,
    ):
        self._by_year = defaultdict(dict)
        self._by_path = defaultdict(dict)
        self._tech_metadata = tech_metadata
        self.metadata = metadata

        if data:
            # We know from the computation that each LabelledOutput has the same set of indicators
            self._indicators = data[0].indicators
        else:
            self._indicators = set()

        assert all(
            datum.indicators == self._indicators for datum in data
        ), "Every single output must have the same set of indicators!"
        assert all(
            len(group["Material Unit"].unique()) == 1
            and len(group["Production Unit"].unique()) == 1
            for _, group in self._tech_metadata.groupby("Category")
        ), "Every tech category must have unique matererial and production units!"

        for output in data:
            self._by_year[output.year][output.path] = output
            self._by_path[output.path][output.year] = output
        self._length = len(data)

    def emissions(self, key: Path | str, indicator: str) -> pd.DataFrame:
        data = self[Path(key)]
        return pd.concat(
            {k: v.emissions.loc[indicator, :] for k, v in data.items()}, names=["Year"]
        )

    def resources(self, key: Path | str) -> pd.DataFrame:
        data = self[Path(key)]
        return pd.concat(
            {k: v.required_resources for k, v in data.items()}, names=["Year"]
        )

    @property
    def by_year(self) -> dict[Year, dict[Path, LabelledOutput]]:
        return self._by_year

    @property
    def by_path(self) -> dict[Path, dict[Year, LabelledOutput]]:
        return self._by_path

    @property
    def indicators(self):
        return self._indicators

    @property
    def tech_metadata(self):
        return self._tech_metadata

    @overload
    def __getitem__(self, key: Year) -> dict[Path, LabelledOutput]:
        ...

    @overload
    def __getitem__(self, key: Path | str) -> dict[Year, LabelledOutput]:
        ...

    @overload
    def __getitem__(self, key: tuple[Year, Path | str]) -> LabelledOutput:
        ...

    @overload
    def __getitem__(self, key: tuple[Path | str, Year]) -> LabelledOutput:
        ...

    def __getitem__(
        self,
        key: Year | Path | str | tuple[Year, Path | str] | tuple[Path | str, Year],
    ) -> dict[Path, LabelledOutput] | dict[Year, LabelledOutput] | LabelledOutput:
        if isinstance(key, Year):
            return self.by_year[key]
        elif isinstance(key, (Path, str)):
            return self.by_path[Path(key)]
        elif isinstance(key, tuple):
            assert len(key) == 2
            year, path = key
            if isinstance(year, (Path, str)):
                path, year = year, path
            assert isinstance(year, Year) and isinstance(path, (Path, str))
            return self.by_year[year][Path(path)]

    def __iter__(self) -> Iterator[LabelledOutput]:
        for _, d in self.by_year.items():
            yield from d.values()

    def __len__(self) -> int:
        return self._length


def _to_labelled_output(
    full_inpt: tuple[Path, Year, ProcessableInput]
) -> LabelledOutput:
    path, year, inpt = full_inpt
    result = calculate(inpt)
    return LabelledOutput(
        required_resources=result.required_resources,
        emissions=result.emissions,
        year=year,
        path=path,
    )


def pipeline(sdf: StandardDataFormat) -> PipelineOutput:
    """Converts the input data to the PipelineOutput format.

    Returns:
        PipelineOutput: The fully converted output of the pipeline
    """

    def _make_iterator(
        flattened: list[tuple[Path, SparseYearsInput]]
    ) -> Iterator[tuple[Path, Year, ProcessableInput]]:
        for path, sparse_years in flattened:
            for path, year, inpt in to_processable_input(path, sparse_years):
                yield (path, year, inpt)

    flattened = flatten_hierarchy(sdf)
    with Pool(cpu_count()) as p:
        processed = p.map(_to_labelled_output, _make_iterator(flattened))

    tech_metadata = pd.DataFrame()
    for _, sparse_years in flattened:
        tech_metadata = (
            pd.concat([sparse_years.tech_metadata, tech_metadata])
            .groupby(level=(0, 1))
            .last()
        )

    return PipelineOutput(processed, tech_metadata=tech_metadata, metadata=sdf.metadata)
