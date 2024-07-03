# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructtors_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema=constructors_schema)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit


constructors_final_df = constructtors_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .drop("url")

# COMMAND ----------

constructors_final_df.write.parquet(f"{processed_folder_path}/constructors", mode= "overwrite") 

# COMMAND ----------

dbutils.notebook.exit("Success")