from pathlib import Path

from mat_dp_pipeline import create_sdf, pipeline

sdf_dir = Path(__file__).parent.parent / "test_data/World"
output = pipeline(create_sdf(sdf_dir))
