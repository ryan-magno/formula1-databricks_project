# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create circuits table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country STRING,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://raw@formula1adlsproj.dfs.core.windows.net/circuits.csv", header true);

# COMMAND ----------

from pyspark.sql.functions import current_user
spark.conf.set("spark.databricks.userInfoFunctions.enabled", "true")
current_user = current_user().alias("current_user")
df = spark.sql("SELECT current_user()") 
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a storage credential
# MAGIC CREATE CREDENTIAL `storage_name`
# MAGIC WITH (
# MAGIC     AZURE_SAS_TOKEN = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-07-03T21:44:24Z&st=2024-07-03T13:44:24Z&spr=https&sig=4HEu8YyePaUGUXPD3%2Fzq5%2F3E7RAnP7puHB6aQiGR5Bs%3D'
# MAGIC );
# MAGIC
# MAGIC -- Create an external location
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `f1_raw_location`
# MAGIC URL 'abfss://raw@formula1adlsproj.dfs.core.windows.net'
# MAGIC WITH (CREDENTIAL `storage_name`);
# MAGIC
# MAGIC -- Grant necessary permissions
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `f1_raw_location` TO `gabrielmagno.ryan@gmail.com`;
# MAGIC
# MAGIC -- Create the table using the external location
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits(
# MAGIC     circuitId INT,
# MAGIC     circuitRef STRING,
# MAGIC     name STRING,
# MAGIC     location STRING,
# MAGIC     country STRING,
# MAGIC     lat DOUBLE,
# MAGIC     lng DOUBLE,
# MAGIC     alt INT,
# MAGIC     url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC     path 'abfss://raw@formula1adlsproj.dfs.core.windows.net/circuits.csv',
# MAGIC     header true
# MAGIC );

# COMMAND ----------


