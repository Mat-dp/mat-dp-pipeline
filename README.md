# Mat DP Pipeline

The idea of mat-dp-pipeline is that provides a multitude of input interfaces, that each convert to the same structure, otherwise known as the Standard Data Format (SDF). The SDF is then processed by the pipeline to produce graphs that show the use of materials or production of "indicator" (a quantity representating a measured output, like emissions or employment).

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
	intensities=ds.MatDPDBIntensitiesSource.from_excel("./materials.xlsx", sheet_name = "Material intensities"),_
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
tech.csv (coal)
tech_2016.csv (coal)
indicators
-> Africa
	tech.csv (nuclear, wood)
	tech_2015.csv (nuclear, wood) B
	tech_2020.csv (nuclear, wood)
	-> Kenya
		tech.csv (fish, fusion)
		tech_2011.csv (nuclear) A
		tech_2017.csv (wood, coal)
		targets.csv (coal, nuclear)
	-> England
		tech.csv
		targets.csv <<- required file for every country!!!
```
