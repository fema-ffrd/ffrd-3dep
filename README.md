# USGS 3DEP 1-Meter DEM Processing for FFRD Workflows
============================================================

This repository is designed for processing Digital Elevation Model (DEM) data from the USGS 3D Elevation Program (3DEP) for Future of Flood Risk Data (FFRD) workflows.

## Data Source

- **Source**: USGS 3D Elevation Program (3DEP)
- **Native Resolution**: 1-meter
- **CRS**: Various (typically UTM zones)
- **Original Units**: Meters

## Processing Details

### Target Specifications
- **File Format**: Cloud-Optimized GeoTIFF (COG)
- **Target CRS**: USA_Contiguous_Albers_Equal_Area_Conic_FFRD 
- **Vertical Units**: feet
- **Output Resolution**: 4-feet
- **Compression**: LZW
- **NoData Value**: -9999

## File Naming Convention

Processed tiles retain their original USGS naming convention:
```
USGS_1M_[LOC]_[GRID]_[YEAR]_[VERSION].tif

Example: USGS_1M_x63y499_PA_Northcentral_2019_B19.tif
```

Where:
- `[LOC]`: Grid coordinates (e.g., x63y499)
- `[GRID]`: Project area identifier
- `[YEAR]`: Acquisition year
- `[VERSION]`: USGS version/batch identifier

## Virtual Mosaic (VRT)

The file `terrain_{id}_4ft.vrt` is a virtual mosaic that seamlessly combines all processed tiles into a single dataset. This file:
- Provides efficient access to the full DEM coverage
- Requires no additional disk space (references individual tiles)
- Can be used directly in ArcPro, QGIS, GDAL, and Python (rasterio/rioxarray)

### Prerequisites

Before you can start developing, please make sure you have the following software installed on your machine:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Visual Studio Code (VSCode)](https://code.visualstudio.com/download)
- [Remote - Containers extension for VSCode](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### Setting up the Development Environment

- Make sure Docker Desktop is running.
- Clone the repository to your local machine.
- Open the project folder in VSCode.
- When prompted, click on "Reopen in Container" to open the project inside the devcontainer.
- If you don't see the prompt, you can manually open the command palette (`Ctrl+Shift+P` or `Cmd+Shift+P`) and select "Dev Containers: Rebuild and Reopen in Container".
- Wait for the devcontainer to build and start. This may take a few minutes if it is the first time you have opened the project in a container.

### Adding dependencies

Use the `env.yaml` file at the project root directory to keep pinned dependencies up-to-date and version controlled.

> Only include top level dependencies in this file (i.e. only packages you explicity want installed and use in your code) and Only inlcude the major.minor version (this allows all patches to automatically be applied when the project is rebuilt)

If your dependencies are more complex (i.e cannot be installed / managed with micromamba alone) you may need to update the `.devcontainer/Dockerfile` and apply similar modification to the production `Dockerfile`.

# Formatting and Linting

This project uses `ruff` for both Python formatting and linting. Before you merge your code into staging or main, make sure all your code passes Ruff's checks. Ruff is fast and enforces a consistent code style and linting rules.

## How to Format and Lint with Ruff

To check your code for formatting and linting issues, run the following commands from the root of the project:

```
ruff check .
ruff format --check .
```

To automatically fix formatting issues, run:

```
ruff format .
```

## Setting up Pre-commit Hooks

To ensure code is formatted and linted before every commit, set up pre-commit hooks using the `pre-commit` package:

1. Install pre-commit (once, in your environment):
   ```bash
   pip install pre-commit
   ```
2. Install the git hook scripts:
   ```bash
   pre-commit install
   ```

Now, every time you run `git commit`, Ruff will check and format your code. If there are issues, the commit will be blocked until you fix them.

### Saving Notebooks to HTML
You can save your Jupyter notebook as an HTML file from the terminal using the following command:
cd into the notebook directory
jupyter nbconvert --to html your_notebook.ipynb

Or, provide the full path
jupyter nbconvert --to html /workspaces/ffrd-3dep/notebooks/your_notebook.ipynb


## Project File Structure
------------

    ├── LICENSE
    ├── README.md                <- The top-level README for developers using this project.  
    ├── .devcontainer
    │   ├── devcontainer.json    <- Dev Container specifications  
    │   ├── Dockerfile           <- Container definition for dev environment  
    │   └── README.md            <- Documentation for devcontainer  
    │
    ├── configs             <- Configuration settings for running scripts
    │
    ├── data
    │   ├── 0_source       <- Source data, minimally altered from source
    │   ├── 1_interim      <- Intermediate data that has been transformed.
    │   └── 2_production   <- Fully transformed, clean datasets for next steps
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── env.yml    <- The requirements file for reproducing the analysis environment
    │
    ├── src                 
    │   ├── *repo_name                              <- Python source code for use in this project.
    │   │       ├── __init__.py                     <- Package indicator, various uses
    │   │       ├── sp00_python_template.py         <- Example of how to structure functional programming scripts
    │           └── sp01_python_runner_template.py  <- Example of how to store project specific parameters
    │            
    ├── tests
    │   ├── test_tnm_utils.py.py           <- Testing for processing utilities
    │   └── test_process_single_dem.py     <- Testing for how DEM is processed
    │  
    ├── .gitattributes      <- Handles consistent line endings in source control across multiple operating systems
    │
    ├── .gitignore          <- Handles which directories to keep out of version control
    │
    ├── .dockerignore       <- Which files to ignore when building docker image for production
    │
    ├── .gitconfig          <- Handles consistent file permissions in source control across multiple operating systems
    │
    ├── .pylintrc           <- Customizations to pylint default settings
    │
    ├── Dockerfile          <- The production Dockerfile
    │
    ├── README.md           <- Template information
    |
    ├── .env.template       <- Template env file (used to create a new .env file with your environment variables for authentication)
    │
    └── tox.ini             <- Alternative method to store project parameters (currently not used)

--------
