# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #Display the file paths

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType([StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the file with the defined schema

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True, schema= circuits_schema)
circuits_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Display the DataFrame

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Select necessary columns only

# COMMAND ----------

circuits_selected_df = circuits_df.drop(circuits_df["url"])
circuits_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns

# COMMAND ----------

from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC # Add ingestion date to the dataframe

# COMMAND ----------

#append new column with the ingestion date
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save as parquet to the processed folder in ADLS

# COMMAND ----------

#save tarnsformed df to processed folder
#circuits_final_df.write.parquet(f"{processed_folder_path}/circuits", mode = "overwrite")

#save as table to f1_processed database
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the saved parquet

# COMMAND ----------

#circuits_parquet = spark.read.parquet(f"{processed_folder_path}/circuits")

#display(circuits_parquet)

# COMMAND ----------

dbutils.notebook.exit("Success")
