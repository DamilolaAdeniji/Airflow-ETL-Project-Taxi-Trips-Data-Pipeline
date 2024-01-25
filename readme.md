# ETL Project: Taxi Trips Data Pipeline

## Overview

This ETL (Extract, Transform, Load) project aims to extract taxi trips data spanning the years 2014 to 2016 from a ClickHouse database, apply transformation techniques using Python, and finally load the cleaned data into an SQLite database. The entire data pipeline is orchestrated using Apache Airflow.

## Project Structure

The project is organized into the following components:

1. **Extraction**: The extraction process pulls raw data from the ClickHouse database containing taxi trips information from 2014 to 2016.

2. **Transformation**: Python scripts are employed to clean, transform, and preprocess the extracted data. Various techniques such as data type conversion, handling missing values, and feature engineering may be applied in this stage.

3. **Loading**: The cleaned data is loaded into an SQLite database, creating a structured and optimized database for analysis.

4. **Orchestration with Airflow**: Apache Airflow is used to orchestrate the entire ETL process. The workflow is defined as a Directed Acyclic Graph (DAG) with tasks for each step in the ETL pipeline. You can see a screenshot of the completed DAG [here](https://github.com/DamilolaAdeniji/Airflow-ETL-Project-Taxi-Trips-Data-Pipeline/blob/main/DAG_screensshot.png)


## Prerequisites

Before running the project, ensure the following dependencies are installed:

- Python 3.9
- ClickHouse client
- Apache Airflow

Install Python dependencies using the provided `requirements.txt`:

```bash
pip install -r requirements.txt
