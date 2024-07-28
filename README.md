
# Formula 1 End-to-End Data Project

## Overview

This project aims to build a comprehensive data pipeline for Formula 1 data using Azure Databricks, Azure Storage Account, and Azure Key Vault. The project involves extracting, transforming, and loading data, followed by performing aggregations and visualizations to gain insights.

## Architecture

### Databricks

- Utilized Databricks for data transformation and processing.

### Storage Account

- **Hierarchical Namespace**: Enabled
- **Redundancy**: Locally Redundant Storage (LRS)

### Key Vault

- **Vault Access Policy**: Enabled in access configuration

![Key Vault Configuration](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/473c6755-2ddb-4064-9145-82c0be54cceb/Untitled.png)

## Transformation

- Configured connection from ADLS to Key Vault to Databricks Secret Scopes (refer to TUTORIALS for reference).
- Refer to the Databricks notebooks for the transformation steps.

### Transformations Done:

#### CSV

- Define data types/schema
- Rename columns
- Concatenate columns
- Drop unnecessary columns
- Add ingestion date
- Save as Parquet

#### JSON

- Define schema
- Drop unnecessary fields
- Rename fields

#### Nested JSON

- Sample from nested JSON:
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
    - Notice that `name` is nested.
    - Create a schema for the outer and inner objects:
    ```python
    name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                     StructField("surname", StringType(), True)
    ])

    drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                        StructField("driverRef", StringType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("code", StringType(), True),
                                        StructField("name", name_schema),
                                        StructField("dob", DateType(), True),
                                        StructField("nationality", StringType(), True),
                                        StructField("url", StringType(), True)
    ])
    ```

    - Concatenate the nested fields into one column:
    ```python
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
    ```

#### Multi-line JSON

- Specify that JSON is multi-line:
    ```python
    pit_stops_df = spark.read.json(path=path, multiLine=True)
    ```

- Saved all transformed files to the “processed” folder in the container.

![Processed Folder](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/a8cffd2c-b6f3-4290-a7bc-d874c6160710/Untitled.png)

## Databricks Workflows

### Including a Child Notebook

![Child Notebook](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/b135f052-eddf-488a-aa6d-e65403867f64/Untitled.png)

- Keep variables like file location in a separate folder and a new notebook.
    - Folder: `includes`
    - Notebook: `configuration`
    ```python
    raw_folder_path = 'abfss://raw@formula1adlsproj.dfs.core.windows.net'
    processed_folder_path = 'abfss://processed@formula1adlsproj.dfs.core.windows.net'
    presentation_folder_path = 'abfss://presentation@formula1adlsproj.dfs.core.windows.net'
    ```

- To pass these variables to another notebook, run:
    ```python
    %run ../includes/configuration
    ```

### Notebook Workflows

Used to run notebooks manually from another notebook.

```python
v_result = dbutils.notebook.run("<notebook_name>", 0, {"p_data_source": "Ergast API"})
v_result
```

```python
# `v_result` above will display "Success" when the notebook runs successfully.

# Paste at the end of the notebook you want to run:
dbutils.notebook.exit("Success")
```

### Passing Parameters to Notebooks

- Create a text widget:
    ```python
    dbutils.widgets.text("p_data_source", "")
    v_data_source = dbutils.widgets.get("p_data_source")
    ```

**Usage:**

1. **Parameterization:** Replace hardcoded values with dynamic parameters.
2. **Interactive Workflows:** Prompt users for input to tailor the notebook's behavior.
3. **Dynamic Logic:** Use input to control the flow of the code.

**Use Cases:**

- Data source selection
- Configuration settings
- Date range selection
- User preferences

### Databricks Jobs

![Databricks Jobs](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/2a7eaa28-a34c-4220-a4b3-fdd98b41caf2/Untitled.png)

- Used to run the `0. ingest_all_files` notebook, which runs all other notebooks.
    - Databricks jobs run `ingest_all_files`, which runs all other notebooks.

### Push to GitHub

- Configure connection and log in.
- Create a repository in GitHub.
- Clone notebooks and move to the repository.
    - **Important**: Do not push secrets.

## Databases, Tables, Views

![Databases, Tables, Views](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/62acec40-5129-49f5-8efc-c687c63bf619/Untitled.png)

### Create External Table from Raw Data - CSV

### Created External Table

- Created a database for the table of raw files.

## Aggregations and Visualization

- Aggregated the tables created.
- Visualization includes:
    - Most dominant drivers throughout the years.
    - Most dominant teams throughout the years.
```

This Markdown file is designed to be comprehensive and presentable, providing a clear overview of your Formula 1 end-to-end data project. It includes detailed descriptions, code snippets, and visual aids to guide readers through the project's various components and steps.
