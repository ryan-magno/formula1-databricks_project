```markdown

# Formula 1 End-to-End Data Project



This project involves the ingestion, transformation, and analysis of Formula 1 data using Azure Databricks, ADLS Gen2, and other Azure services.



## Architecture Overview



### Databricks

- Main computational engine for data processing and transformation.



### Storage Account

- **Hierarchical Namespace**: Enabled

- **Redundancy**: Locally Redundant Storage (LRS)



### Key Vault

- **Vault Access Policy**: Enabled



![Architecture Diagram](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/473c6755-2ddb-4064-9145-82c0be54cceb/Untitled.png)



## Data Transformation



### Configuration and Connection

- Established connections from ADLS to Key Vault and Databricks Secret Scopes.

- Refer to tutorials for detailed configuration steps.



### Databricks Notebooks

- Transformation tasks performed include:

 - **CSV Files**:

  - Defined datatypes/schema.

  - Renamed columns.

  - Concatenated fields.

  - Dropped unnecessary columns.

  - Added ingestion date.

  - Saved transformed data as Parquet files.

 - **JSON Files**:

  - Defined schema.

  - Dropped and renamed fields as necessary.

 - **Nested JSON**:

  - Example of nested structure:



  ```json

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



  - Handled nested structures with appropriate schemas:



  ```python

  name_schema = StructType([StructField("forename", StringType(), True),

               StructField("surname", StringType(), True)])



  drivers_schema = StructType([StructField("driverId", IntegerType(), False),

                 StructField("driverRef", StringType(), True),

                 StructField("number", IntegerType(), True),

                 StructField("code", StringType(), True),

                 StructField("name", name_schema),

                 StructField("dob", DateType(), True),

                 StructField("nationality", StringType(), True),

                 StructField("url", StringType(), True)])

  ```



  - Concatenated nested fields:



  ```python

  .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

  ```



 - **Multi-line JSON**:

  - Specified as multi-line:



  ```python

  pit_stops_df = spark.read.json(path=path, multiLine=True)

  ```



- Saved all transformed files to the "processed" folder in the container.



![Processed Data](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/a8cffd2c-b6f3-4290-a7bc-d874c6160710/Untitled.png)



## Databricks Workflows



### Including a Child Notebook

- Separated variables like file locations in a new notebook within an `includes` folder named `configuration`.



```python

raw_folder_path = 'abfss://raw@formula1adlsproj.dfs.core.windows.net'

processed_folder_path = 'abfss://processed@formula1adlsproj.dfs.core.windows.net'

presentation_folder_path = 'abfss://presentation@formula1adlsproj.dfs.core.windows.net'

```



- To include these variables in other notebooks, use:



```python

%run ../includes/configuration

```



### Notebook Workflows

- Run notebooks manually from another notebook:



```python

v_result = dbutils.notebook.run("<notebook_name>", 0, {"p_data_source": "Ergast API"})

v_result

```



- Ensure successful execution by checking:



```python

dbutils.notebook.exit("Success")

```



### Passing Parameters to Notebooks

- Create interactive parameters with widgets:



```python

dbutils.widgets.text("p_data_source", "")

v_data_source = dbutils.widgets.get("p_data_source")

```



This allows for dynamic and interactive parameterization, enhancing flexibility and reusability.



### Databricks Jobs

![Databricks Jobs](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/2a7eaa28-a34c-4220-a4b3-fdd98b41caf2/Untitled.png)



- Used to run the `ingest_all_files` notebook, which in turn runs all other necessary notebooks.



### Push to GitHub

- Configure GitHub connection:

 - Log in and create a repository.

 - Clone notebooks and push to the repository.

 - **Note**: Do not push secrets.



## Databases, Tables, Views



![Database Schema](https://prod-files-secure.s3.us-west-2.amazonaws.com/581864de-d26b-4d58-8208-4a0f112c12b2/62acec40-5129-49f5-8efc-c687c63bf619/Untitled.png)



### Creating External Tables

- Created a database for the raw data files.



## Aggregations and Visualization



- Aggregated data from the tables and performed visualizations for:

 - Most dominant drivers over the years.

 - Most dominant teams over the years.



This project demonstrates the complete data engineering lifecycle, from data ingestion and transformation to analysis and visualization, using Azure Databricks and related technologies.

```
