# Mat DP Pipeline

The idea of mat-dp-pipeline is that it provides a multitude of input interfaces, that each convert to the same structure, otherwise known as the Standard Data Format (SDF). The SDF is then processed by the pipeline to produce graphs that show the use of materials or production of "indicator" (a quantity representating a measured output, like emissions or employment).

# Installation

## General Use

For testing see section below

`pip install git+ssh://git@github.com:dreamingspires/mat-dp-pipeline.git`

`poetry add git+ssh://git@github.com:dreamingspires/mat-dp-pipeline.git`

## Testing

You may clone the repo then run:

`poetry install`

# Basic Usage

## Running the pipeline on an existing sdf

If you already have a structure that conforms to the SDF structure, either by exporting after importing, or by generating manually, you may import the sdf like so:
```py
from mat_dp_pipeline import create_sdf, pipeline

sdf = create_sdf("./sdf_dir")
```

Then get to the pipeline output:
```py
output = pipeline(sdf)
```

Finally, to display the output in the app:

```py
App(output).serve()
```

## Using other data sources

The real differences in how mat dp pipeline can be used are all in SDF creation. After this point, all the steps are the same as above, but I've repeated them for full examples.

Note that in the below I use a mix of excel files with a sheet_name, and csv directly - either syntax is acceptable for all data sources. You can also simply enter a dataframe into the init.

### TMBA

```py
import mat_dp_pipeline.data_sources as ds
from mat_dp_pipeline import App, create_sdf, pipeline

TMBA_TARGETS_PARAMETERS = [
	"Power Generation (Aggregate)",
	"Power Generation Capacity (Aggregate)",
]

sdf = create_sdf(
	intensities=ds.MatDPDBIntensitiesSource.from_excel("./materials.xlsx", sheet_name = "Material intensities"),
	indicators=ds.MatDPDBIndicatorsSource.from_excel("./materials.xlsx", sheet_name = "Material emissions"),
	targets=ds.TMBATargetsSource.from_csv(
		"./results_1.5_deg.csv", TMBA_TARGETS_PARAMETERS, ds.MatDPDBIntensitiesSource
	),
)

output = pipeline(sdf)

App(output).serve()

```
### IAM

```py
import mat_dp_pipeline.data_sources as ds
from mat_dp_pipeline import App, create_sdf, pipeline

IAM_TARGETS_PARAMETERS = ["Primary Energy", "Secondary Energy|Electricity"]

sdf = create_sdf(
	intensities=ds.MatDPDBIntensitiesSource.from_excel("./materials.xlsx", sheet_name = "Material intensities"),
	indicators=ds.MatDPDBIndicatorsSource.from_excel("./materials.xlsx", sheet_name = "Material emissions"),
	targets=ds.IntegratedAssessmentModel.from_csv(
		"./ima_targets.csv", IAM_TARGETS_PARAMETERS, ds.MatDPDBIntensitiesSource
	),
)

output = pipeline(sdf)

App(output).serve()
```

# CLI Usage

There is a CLI that exposes some of the behaviour of Mat-DP-Pipeline. For details on how to use this run:

`poetry run app --help`

Then for each subcommand e.g.

`poetry run app tmba --help`



# Standard Data Format

The below shows a potential structure of a Standard Data Format:

```
intensities.csv (coal)
intensities_2016.csv (coal)
indicators.csv
-> Africa
	intensities.csv (nuclear, wood)
	intensities_2015.csv (nuclear, wood)
	intensities_2020.csv (nuclear, wood)
	-> Kenya
		intensities.csv (fish, fusion, nuclear, wood, coal)
		intensities_2011.csv (nuclear)
		intensities_2017.csv (wood, coal)
		targets.csv (coal, nuclear)
-> Europe
	-> England
		intensities.csv
		targets.csv
```

The SDF is a generalised format that other more specific formats can be converted to. The folders can be arranged in any structure, but are most often grouped by countries, continents and parameters. Each level takes its intensities and indicators hierarchically.

You can also specify year based files in the SDF. If these are specified, a linear interpolation is performed between the files, to allow changing intensities and indicators through the years. In the targets file you must already specify the years, so this condition does not apply.

The yearly files must also not specify technologies which aren't already present in the base file. Also there must always be a base file at the level where there are years.
