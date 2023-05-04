# Mat-dp pipeline

Mat-dp-pipeline, as the name suggests, provides a way to estimate material demand and visualise it in an automated manner. There are two key steps that are part of the pipeline: the conversion of input data into a standardised data structure and the use of such structure to calculate anfd visualise the resulting material demand and any other indicators provided. These two steps are described in detail below:

1. Data standardisation: Mat-dp-pipeline provides a multitude of input interfaces (which may include a material intensities database and projections of changing energy or transport systems) that convert to a standardised structure, which is called the "Standard Data Format" (SDF). Since the types of system projections may come from different sources, two options are offered to convert known modelling results into standardised data. More options could be developed over time, based on user modelling and visualisation needs.

2. Calculation and visualisation: The SDF is processed to produce a web-based visualisation that shows the material demand (based on the system projections), and additional material implications such as emissions or employment (provided as quantities representing a measured output in the Mat-dp-pipeline data inputs). 

# Installation

## General Use

For testing see section below

`pip install git+ssh://git@github.com:dreamingspires/mat-dp-pipeline.git`

`poetry add git+ssh://git@github.com:dreamingspires/mat-dp-pipeline.git`

## Testing

You may clone the repo then run:

`poetry install`

# Basic Usage

## Running the pipeline on an existing SDF

If you already have a structure that conforms to the SDF structure, either by exporting after importing, or by generating manually, you may import the SDF like this:
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

## Using other data sources to create the SDF and create the web visualisation

The real differences in how Mat-dp-pipeline can be used are all in SDF creation. After this point, all the steps are the same as above, but we have repeated them here to provide clear examples.

Note that in the following, we use a mix of excel files with a sheet_name, and csv directly - either syntax is acceptable for all data sources. The user can also simply enter a dataframe into the init.

### TMBA: An [OSeMOSYS](http://www.osemosys.org/)-type of results file
This is currently set up to take a csv of the [TEMBA](https://zenodo.org/record/4889373)-type of results. The results usually include files for every scenario used, which are the ones that this pipeline can take (e.g., TEMBA_1.5.csv).

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
### IAM: Integrated Assessment Models 
This is currently set up to work for [TIAM-UCL](https://www.ucl.ac.uk/energy-models/models/tiam-ucl) kind of results. If additional IAMs need to be used, please create a branch of the code implementing this and then create a pull request. 

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

# Command-line interface (CLI) Usage

There is a CLI that exposes some of the behaviour of Mat-dp-pipeline. For details on how to use this run:

`poetry run app --help`

Then for each subcommand e.g.

`poetry run app tmba --help`

`poetry run app iam --help`

As a starting point, you can run a command like the following to both create an SDF and the web visualisation:

'poetry run app iam Material_intensities_database.xlsx file_with_scenarios.xls'

# Standard Data Format (SDF)

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

## SDF modifications to include yearly intensities 

You can also specify year-based files in the SDF. If these are specified, a linear interpolation is performed between the files, to allow changing intensities and indicators through the years. In the targets file you must already specify the years, so this condition does not apply.

The yearly files must also not specify technologies which aren't already present in the base file. Also there must always be a base file at the level where there are years.
