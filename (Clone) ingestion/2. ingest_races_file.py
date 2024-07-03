# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/races.csv", header = True, schema = races_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

#concat date and time
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                .drop(races_df["url"])

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), 
                                                   col('year').alias('race_year'), 
                                                   col('round'), 
                                                   col('circuitId').alias('circuit_id'),
                                                   col('name'), 
                                                   col('ingestion_date'), 
                                                   col('race_timestamp')) \
                                                   .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

races_selected_df.write.parquet(f"{processed_folder_path}/races", partitionBy = "race_year", mode = "overwrite")

# COMMAND ----------

dbutils.notebook.exit("Success")
