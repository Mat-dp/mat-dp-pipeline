import logging
from pathlib import Path
from typing import ClassVar, Final, Literal, Optional

import pandas as pd

from mat_dp_pipeline.abstract_data_sources import TargetsSource
from mat_dp_pipeline.data_sources.country_sets import Identifier, SourceWithCountries

from .common import map_technologies
from .tech_map import TechMap, TechMapTypes, create_tech_map


class TMBATargetsSource(TargetsSource):
    _targets_csv: Path
    _targets_parameters: list[str]
    _country_to_path: dict[str, Path]
    _grouping: Final[list[str]] = ["country", "parameter"]
    _tech_map: ClassVar[TechMap] = create_tech_map(TechMapTypes.TMBA)
    # Dictionary for fixing country names like NM -> NA (Namibia)
    _country_patching: ClassVar[dict[str, str]] = {"NM": "NA"}

    tail_labels: ClassVar[list[str]] = ["Parameter"]

    def __init__(
        self,
        targets: pd.DataFrame,
        parameters: list[str],
        country_source: type[SourceWithCountries],
    ):
        if not parameters:
            raise ValueError("You must specify parameters.")

        self._targets = targets
        self._targets_parameters = parameters
        self._country_to_path = country_source.country_to_path(
            identifier=Identifier.alpha_2
        )

    @classmethod
    def from_excel(
        cls,
        spreadsheet: str | Path,
        parameters: list[str],
        country_source: type[SourceWithCountries],
        sheet_name: str = "DATA_TIAM",
        engine: Literal["xlrd", "openpyxl", "odf", "pyxlsb"] | None = None,
    ):
        source = pd.read_excel(Path(spreadsheet), sheet_name=sheet_name, engine=engine)
        return cls(source, country_source=country_source, parameters=parameters)

    @classmethod
    def from_csv(
        cls,
        csv: str | Path,
        parameters: list[str],
        country_source: type[SourceWithCountries],
        sep: Optional[str] = None,
    ):
        source = pd.read_csv(csv, sep=sep)
        return cls(
            source,
            country_source=country_source,
            parameters=parameters,
        )

    def __call__(self, output_dir: Path) -> None:
        targets = self._targets.copy()
        # Pick targets which parameter's value is in requested parameters (self._targets_parameters)
        targets = targets[targets["parameter"].isin(self._targets_parameters)]

        if targets.empty:
            logging.warning("Targets for selected parameters are empty!")
            return

        targets = targets.drop(columns=[targets.columns[0], "scenario"]).dropna()
        targets = map_technologies(targets, "variable", self._tech_map)

        for key, targets_frame in targets.groupby(self._grouping):
            country = self._country_patching.get(key[0]) or key[0]
            path = (self._country_to_path[country],) + key[1:]
            path = Path(*path)
            location_dir = output_dir / path.relative_to("/")
            location_dir.mkdir(exist_ok=True, parents=True)
            targets_frame.drop(columns=self._grouping).to_csv(
                location_dir / self.file_name, index=False
            )
