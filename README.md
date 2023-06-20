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
### IAM: Integrated Assessment Models (IAM)

There are two types of IAM inputs that may be used: 1) files from the TIMES IAM [TIAM-UCL](https://www.ucl.ac.uk/energy-models/models/tiam-ucl) and 2) files that use the IAM community data standards.

#### TIAM_UCL
This is currently set up to work for TIAM-UCL kind of results. 

```py
import mat_dp_pipeline.data_sources as ds
from mat_dp_pipeline import App, create_sdf, pipeline

IAM_TARGETS_PARAMETERS = ["Capacity|Electricity"]

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
### IAM community standards

The files that may be used for this option can be downloaded from websites such as [IIASA's NGFS scenario explorer](https://data.ece.iiasa.ac.at/ngfs/#/login?redirect=%2Fworkspaces). The type of values that may be included so far are those associated to Capacity Additions for the different types of technologies. The file works very similarly to the TIAM-UCL option.


# Command-line interface (CLI) Usage

There is a CLI that exposes some of the behaviour of Mat-dp-pipeline. For details on how to use this run:

`poetry run app --help`

Then for each subcommand e.g.

TEMBA data
`poetry run app tmba --help`

TIAM-UCL data
`poetry run app iam --help`

IAM community data
`poetry run app iamc --help`

As a starting point, you can run a command like the following to both create an SDF and the web visualisation:

`poetry run app iam Material_intensities_database.xlsx file_with_scenarios.xls`

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

If you wish to save the SDF while running a specific model using CLI, you can use the following command (remember to change to the right names of your files):

`poetry run app iam Material_intensities_database.xlsx file_with_scenarios.xls --sdf-output sdf_folder_name`

In the cases when data within the SDF needs to be modified to reflect changes in intensities by year or technology, it is advisable to first run the model and use the option to save the SDF output. Then, such output can be modified and then the option to run the pipeline but starting from the SDF folder source can be used. Such option is:

`poetry run app sdf sdf_folder_source`

## SDF modifications to include yearly intensities 

You can also specify year-based values in the SDF. If these are specified, a linear interpolation is performed between two given years. For example, material intensities for solar PV between 2020 and 2025 can be assumed to change linearly as long as the values for both years are included in the SDF. In the case when the same value wants to be used after 2025, the intensities between 2025 and the final year in the case study must be specified to be the same. 

In the targets file you must already specify the years, so this condition does not apply.

Please note that the yearly files must only specify technologies which are already present in the base file. Also, there must always be a base file at the level where there are years.

# Contributing to Mat-dp-pipeline


Contributions are welcome! 

If you see something that needs to be improved, open an issue in the [respective section of the repository](https://github.com/Mat-dp/mat-dp-pipeline/issues).
If you have questions, need assistance or need better instructions for contributing, please 
[get in touch via e-mail](mailto:refficiency-enquiries@eng.cam.ac.uk) mentioning "Mat-dp-pipeline" in the subject.


We recommend that developers of mat-dp-pipeline make changes using poetry. Please work in a branch and make sure your contributions work before starting a pull request.
