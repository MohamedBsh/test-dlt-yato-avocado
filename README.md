# Avocats Analysis

This project analyzes the evolution of the number of lawyers (avocats) in France over time, using open data from the French Ministry of Justice. It includes a data pipeline for processing the data and a React application for visualizing the results.

## Project Overview

The project consists of two main parts:
1. A Python-based data pipeline for fetching, processing, and analyzing the data.
2. A React application for visualizing the results.

## Features

- Data extraction from the official French open data portal
- Data processing and analysis using Python
- Prediction of lawyer numbers for the next 5 years using linear regression
- Interactive visualization of historical data and predictions using React and D3.js

## Project objective

The main objective of this project is to explore and demonstrate the use of two emerging open-source tools in the field of data engineering:

1. **dlt (Data Load Tool)**: A flexible tool for extracting and loading data.
2. **yato (Yet Another Transformation Orchestrator)**: A lightweight orchestrator for SQL transformations, particularly suited to DuckDB.

By combining these tools with open data, we create a complete data pipeline (for free), from extraction to visualization.

## Why dlt?

- Makes it easy to extract data from a variety of sources.
- Provides a unified interface for loading data.
- Simplifies schema and increment management.

## Why yato?

- Enables simple orchestration of SQL transformations.
- Integrates seamlessly with DuckDB for optimum performance.
- Offers a lightweight, flexible approach to data transformation.

## Prerequisites

- Python 3.7+
- Node.js 12+
- npm 6+

## Installation

To set up the project, simply run the installation script:

```bash
bash install.sh
```

## Data Source
The data is sourced from the French government's open data portal:
Evolution du nombre d'avocats en France par barreau https://www.data.gouv.fr/fr/datasets/r/05f4ca76-0d72-4ecd-9f5c-20a12965e348

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.