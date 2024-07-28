# Formula 1 End-to-End Data Project

## Overview

This project involves the extraction, transformation, and analysis of Formula 1 data using various Azure services. The main components include Databricks, Azure Data Lake Storage (ADLS), and Azure Key Vault. The data is transformed and stored for further analysis and visualization.

## Components

### 1. Databricks
- Central platform for data processing and transformation.

### 2. Storage Account
- **Hierarchical Namespace Enabled**: Organizes data with a file system hierarchy.
- **Redundancy**: Locally-redundant storage (LRS) for data durability.

### 3. Key Vault
- **Vault Access Policy**: Secures sensitive information, allowing controlled access.

![System Architecture](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/473c6755-2ddb-4064-9145-82c0be54cceb/Untitled.png)

## Data Transformation

### Connection Configuration
- Configured connections from ADLS to Key Vault using Databricks Secret Scopes.
- Refer to tutorials for setting up these connections.

### Transformations
Data transformations were performed in Databricks notebooks, including:

#### CSV Files
- Defined datatypes/schema.
- Renamed columns, concatenated, dropped unnecessary fields.
- Added ingestion date and saved as Parquet.

#### JSON Files
- Defined schema, dropped unnecessary fields, renamed columns.

#### Nested JSON
- Sample structure:
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
- Created schema for nested fields:
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
- Concatenated nested fields into one column:
    ```python
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
    ```

#### Multi-line JSON
- Specified that JSON is multi-line:
    ```python
    pit_stops_df = spark.read.json(path=path, multiLine=True)
    ```

### Storage
- All transformed files were saved in the "processed" folder within the container.

![Processed Data](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/a8cffd2c-b6f3-4290-a7bc-d874c6160710/Untitled.png)

## Databricks Workflows

### Including a Child Notebook
- Variables like file locations are stored in a separate folder and notebook:
    - Folder: `includes`
    - Notebook: `configuration`

    ```python
    raw_folder_path = 'abfss://raw@formula1adlsproj.dfs.core.windows.net'
    processed_folder_path = 'abfss://processed@formula1adlsproj.dfs.core.windows.net'
    presentation_folder_path = 'abfss://presentation@formula1adlsproj.dfs.core.windows.net'
    ```
- To use these variables in other notebooks:
    ```python
    %run ../includes/configuration
    ```

### Notebook Workflows
- Used to manually run notebooks from another notebook:
    ```python
    v_result = dbutils.notebook.run("<notebook_name>", 0, {"p_data_source": "Ergast API"})
    dbutils.notebook.exit("Success")
    ```

### Passing Parameters to Notebooks
- Example of parameterization in Databricks:
    ```python
    dbutils.widgets.text("p_data_source", "")
    v_data_source = dbutils.widgets.get("p_data_source")
    ```

## Databricks Jobs
- Used to run the `ingest_all_files` notebook, which orchestrates the execution of all other notebooks.

![Databricks Jobs](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/2a7eaa28-a34c-4220-a4b3-fdd98b41caf2/Untitled.png)

## GitHub Integration
- Configured connection with GitHub, created a repository, and cloned notebooks into the repo.
- Note: Never push secrets to GitHub.

## Databases, Tables, Views

### Creating External Tables from Raw Data
- Created a database for raw file tables.

## Aggregations and Visualization
- Aggregated data for analysis.
- Visualizations include:
  - Most dominant drivers throughout the years.
  - Most dominant teams throughout the years.

---

This documentation provides a comprehensive overview of the Formula 1 data project, detailing the architecture, data transformation processes, Databricks workflows, and integration with GitHub.
