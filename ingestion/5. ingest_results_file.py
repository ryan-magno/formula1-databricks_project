# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

file_path = f"{raw_folder_path}/results.json"

results_df = spark.read.json(path = file_path, schema = results_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit 

results_renamed = results_df.withColumnRenamed("resultId", "result_id") \
                            .withColumnRenamed("raceId", "race_id") \
                            .withColumnRenamed("driverId", "driver_id") \
                            .withColumnRenamed("constructorId", "constructor_id") \
                            .withColumnRenamed("positionText", "position_text") \
                            .withColumnRenamed("positionOrder", "position_order") \
                            .withColumnRenamed("fastestLap", "fastest_lap") \
                            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                            .withColumn("ingestion_date", current_timestamp()) \
                            .withColumn("data_source", lit(v_data_source)) \
                            .drop("statusId")

# COMMAND ----------

#results_renamed.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

results_renamed.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")


# COMMAND ----------

dbutils.notebook.exit("Success")
