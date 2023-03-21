from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar, Optional


class BaseSource(ABC):
    @abstractmethod
    def __call__(self, output_dir: Path) -> None:
        """Prepare a Standard Data Format data and save it in the `output_dir`
        Args:
            output_dir (Path): Output SDF root directory
        """
        ...


class IntensitiesSource(BaseSource):
    base_file_name: str = "intensities.csv"
    main_label: ClassVar[Optional[str]] = None

    def year_file_name_maker(self, year: int):
        return f"intensities_{year}.csv"


class TargetsSource(BaseSource):
    file_name: str = "targets.csv"
    tail_labels: ClassVar[list[str]] = []


class IndicatorsSource(BaseSource):
    base_file_name: str = "indicators.csv"
    main_label: ClassVar[Optional[str]] = None

    def year_file_name_maker(self, year: int):
        return f"indicators_{year}.csv"
