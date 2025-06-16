# UNFCCC ETF Command Line (CLI) Tool

## Background

The Enhanced Transparency Framework (ETF) is a set of reporting and review requirements under the Paris Agreement to enhance the transparency of climate action and support.

Parties can submit their ETF GHG inventory data in the electronic format of a CRT Data Exchange JSON file.

The structure and requirements of the CRT Data Exchange JSON format are described in detail at the [UNFCCC ETF reporting tools help page](https://unfccc.int/etf-reporting-tools-help#Technical).

## Project

This repository contains the source code of a command line tool for processing CRT Data Exchange JSON files before importing them into ETF GHG inventory.

The tool supports the following functionality:

- Splitting reports into subsets, filtered by GHG inventory sectors.
- Adding missing metadata required by the ETF Reporting Tools.

## Installation

To install and set up the CLI tool, follow these steps:

1. Prepare and activate a Python virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install build wheel
```

2. Clone the GitHub repository and build the package:

```bash
git clone https://github.com/unfccc/etf
cd etf
python -m build
```

3. Install the resulting package:

```bash
pip install dist/unfccc_etf_cli-1.0.0-py3-none-any.whl
```

## Usage examples

Locate the root node for LULUCF sector of medatata:
```
etf metadata find LULUCF
```

Locate lower level node in the Agriculture sector of medatata:
```
etf metadata find "3.F.1.b. Barley"
```

Filter out all country data, leaving only related to energy sector, print result to standard output:
```
etf data filter -s energy country_data.json
```

Insert missing template grids into the country data:
```
etf data fix -r GRIDS country_data.json
```

Apply all known fixes to satisfy import requirements:
```
etf data fix -r ALL country_data.json
```

Print statistic of country data:
```
etf data stats country_data.json
```

The tool contains built-in help on commands, available by calling with `--help` parameter.

## Credits

Special thanks to Pallets Projects, creators of excellent Click Python package.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
