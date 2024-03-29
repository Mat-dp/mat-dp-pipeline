import tempfile
from pathlib import Path
from typing import overload

import mat_dp_pipeline.abstract_data_sources as ds

from .standard_data_format import (
    SDF_METADATA_FILE_NAME,
    SDFMetadata,
    StandardDataFormat,
    load,
)

TailLabels = list[str] | type[ds.TargetsSource] | None
MainLabels = str | type[ds.IntensitiesSource] | type[ds.IndicatorsSource] | None


@overload
def create_sdf(
    *,
    intensities: ds.IntensitiesSource,
    indicators: ds.IndicatorsSource,
    targets: ds.TargetsSource | list[ds.TargetsSource],
    main_label: MainLabels = None,
    tail_labels: TailLabels = None,
) -> StandardDataFormat:
    ...


@overload
def create_sdf(
    source: Path | str,
    *,
    main_label: MainLabels = None,
    tail_labels: TailLabels = None,
) -> StandardDataFormat:
    ...


def create_sdf(
    source: Path | str | None = None,
    *,
    intensities: ds.IntensitiesSource | None = None,
    indicators: ds.IndicatorsSource | None = None,
    targets: ds.TargetsSource | list[ds.TargetsSource] | None = None,
    main_label: MainLabels = None,
    tail_labels: TailLabels = None,
) -> StandardDataFormat:
    if source:
        return load(Path(source))
    else:
        assert intensities and indicators and targets
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)

            targets_list = targets if isinstance(targets, list) else [targets]
            for t in targets_list:
                t(path)
            intensities(path)
            indicators(path)

            # We'll construct SDFMetadata and save it to the `path` location
            # so that it can be loaded as part of the sdf load
            metadata = SDFMetadata()
            if main_label is not None:
                if isinstance(main_label, str):
                    metadata.main_label = main_label
                elif main_label.main_label is not None:
                    metadata.main_label = main_label.main_label
            else:
                if intensities.main_label is not None:
                    metadata.main_label = intensities.main_label
                elif indicators.main_label is not None:
                    metadata.main_label = indicators.main_label

            if tail_labels is not None:
                metadata.tail_labels = (
                    tail_labels
                    if isinstance(tail_labels, list)
                    else tail_labels.tail_labels
                )
            elif isinstance(targets, ds.TargetsSource):
                metadata.tail_labels = targets.tail_labels
            elif isinstance(targets, list):
                # Use tail labels from targets only when all are the same. Otherwise - empty list
                tail_labels = targets[0].tail_labels
                if all(t.tail_labels == tail_labels for t in targets):
                    metadata.tail_labels = tail_labels
                else:
                    metadata.tail_labels = []
            else:
                assert False

            metadata_file = Path(path / SDF_METADATA_FILE_NAME)
            with open(metadata_file, "w") as f:
                f.write(metadata.json())

            return load(path)
