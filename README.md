# Formula 1 End-to-End Data Project

## Overview

This project involves setting up an end-to-end data pipeline for Formula 1 data using various Azure services and Databricks for data transformation and analysis. Below is a detailed documentation of the project setup, transformations, workflows, and visualizations.

## Project Components

### Databricks
- Utilized for data processing and transformation.

### Storage Account
- **Hierarchical Namespace Enabled**
- **Redundancy:** Locally Redundant Storage (LRS)

### Key Vault
- **Vault Access Policy** enabled in access configuration

## Transformation

### Connection Configuration
- Configured connection from Azure Data Lake Storage (ADLS) to Key Vault to Databricks Secret Scopes.
- Refer to tutorials for reference.

### Databricks Notebooks for Transformation

#### CSV Transformations
- **Define datatypes/schema**
- **Rename Columns**
- **Concatenate Columns**
- **Drop Unnecessary Columns**
- **Add Ingestion Date**
- **Save as Parquet**

#### JSON Transformations
- **Define Schema**
- **Drop Unnecessary Columns**
- **Rename Columns**

#### Nested JSON Transformations
- **Schema for Nested JSON**:
    ```python
    {
      "driverId": 1,
      "driverRef": "hamilton",
      "number": 44,
      "code": "HAM",
      "name": {
        "forename": "Lewis",
        "surname": "Hamilton"
      },
      "dob": "1985-01-07",
      "nationality": "British",
      "url": "http://en.wikipedia.org/wiki/Lewis_Hamilton"
    }
    ```
    - Schema Definition:
        ```python
        name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                         StructField("surname", StringType(), True)])
        
        drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                            StructField("driverRef", StringType(), True),
                                            StructField("number", IntegerType(), True),
                                            StructField("code", StringType(), True),
                                            StructField("name", name_schema),
                                            StructField("dob", DateType(), True),
                                            StructField("nationality", StringType(), True),
                                            StructField("url", StringType(), True)])
        ```

    - Concatenate Nested Columns:
        ```python
        .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
        ```

#### Multi-Line JSON Transformations
- **Specify Multi-Line Parameter**:
    ```python
    pit_stops_df = spark.read.json(path = path, multiLine= True)
    ```

### Saving Transformed Files
- Saved all transformed files to the "processed" folder in the container.

## Databricks Workflows

### Including a Child Notebook
- Keep variables like file location in a separate folder and notebook.
    - Folder: `includes`
    - Notebook: `configuration`
    ```python
    raw_folder_path = 'abfss://raw@formula1adlsproj.dfs.core.windows.net'
    processed_folder_path = 'abfss://processed@formula1adlsproj.dfs.core.windows.net'
    presentation_folder_path = 'abfss://presentation@formula1adlsproj.dfs.core.windows.net'
    ```

- Run configuration in other notebooks:
    ```python
    %run ../includes/configuration
    ```

### Notebook Workflows
- Used to run notebooks manually from another notebook:
    ```python
    v_result = dbutils.notebook.run("<notebook_name>", 0, {"p_data_source": "Ergast API"})
    v_result
    ```

    ```python
    dbutils.notebook.exit("Success")
    ```

### Passing Parameters to Notebooks
- Example for creating interactive parameters:
    ```python
    dbutils.widgets.text("p_data_source", "")
    v_data_source = dbutils.widgets.get("p_data_source")
    ```

### Databricks Jobs
- Used to run the `0. ingest_all_files`, which runs all the other notebooks.

### Push to GitHub
- Configure connection and log in.
- Create a repo in GitHub.
- Clone notebooks and move to the repo.
    - Do not push secrets.

## Databases, Tables, Views

### Create External Table from the Raw Data (CSV)
- Created a database for the table of raw files.

## Aggregations and Visualization
- Aggregated the tables created.
- Visualization for:
    - Most dominant drivers throughout the years.
    - Most dominant teams throughout the years.
