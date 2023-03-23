import argparse
from pathlib import Path

import mat_dp_pipeline.data_sources as ds
from mat_dp_pipeline import App, create_sdf, pipeline


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title="target types", description="available target types", dest="target_type"
    )
    ima_parser = subparsers.add_parser(
        "ima", description="IMA target type", help="IMA target type"
    )
    tmba_parser = subparsers.add_parser(
        "tmba", description="TMBA target type", help="TMBA target type"
    )
    sdf_parser = subparsers.add_parser(
        "sdf", description="TMBA target type", help="TMBA target type"
    )
    sdf_parser.add_argument("source", type=Path)

    ima_parser.add_argument("materials", type=Path)
    ima_parser.add_argument("targets", type=Path)
    ima_parser.add_argument("--sdf-output", type=Path)

    tmba_parser.add_argument("materials", type=Path)
    tmba_parser.add_argument("targets", type=Path)
    tmba_parser.add_argument("--sdf-output", type=Path)

    args = parser.parse_args()

    TMBA_TARGETS_PARAMETERS = [
        "Power Generation (Aggregate)",
        "Power Generation Capacity (Aggregate)",
    ]
    IAM_TARGETS_PARAMETERS = ["Primary Energy", "Secondary Energy|Electricity"]

    if args.target_type == 'sdf':
        sdf = create_sdf(args.sdf_source)
    elif args.target_type == 'tmba':
        assert args.materials and args.targets
        sdf = create_sdf(
            intensities=ds.MatDPDBIntensitiesSource(args.materials),
            indicators=ds.MatDPDBIndicatorsSource(args.materials),
            targets=ds.TMBATargetsSource(
                args.targets, TMBA_TARGETS_PARAMETERS, ds.MatDPDBIntensitiesSource
            ),
        )
    elif args.target_type == 'iam':
        sdf = create_sdf(
            intensities=ds.MatDPDBIntensitiesSource(args.materials),
            indicators=ds.MatDPDBIndicatorsSource(args.materials),
            targets=ds.IntegratedAssessmentModel(
                args.targets, IAM_TARGETS_PARAMETERS, ds.MatDPDBIntensitiesSource
            ),
        )
    else:
        assert False

    if args.sdf_output:
        sdf.save(args.sdf_output)

    output = pipeline(sdf)
    App(output).serve()


if __name__ == "__main__":
    main()
