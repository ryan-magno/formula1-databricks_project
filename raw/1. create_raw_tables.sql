-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CREATE TABLES FOR CSV
-- MAGIC #### Create circuits table

-- COMMAND ----------

-- Create the table using the external location
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
    circuitId INT,
    circuitRef STRING,
    name STRING,
    location STRING,
    country STRING,
    lat DOUBLE,
    lng DOUBLE,
    alt INT,
    url STRING
)
USING csv
OPTIONS (
    path 'abfss://raw@formula1adlsws.dfs.core.windows.net/circuits.csv', header true);

--- SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "abfss://raw@formula1adlsws.dfs.core.windows.net/races.csv", header true);

---SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### CREATE TABLE FOR JSON
-- MAGIC #### create constructors table
-- MAGIC - single line json
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "abfss://raw@formula1adlsws.dfs.core.windows.net/constructors.json");

---SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure
-- MAGIC * `name` is nested

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "abfss://raw@formula1adlsws.dfs.core.windows.net/drivers.json");

---SELECT * FROM hive_metastore.f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results table
-- MAGIC  * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "abfss://raw@formula1adlsws.dfs.core.windows.net/results.json");

---SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "abfss://raw@formula1adlsws.dfs.core.windows.net/pit_stops.json", multiLine true);

---SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Folder
-- MAGIC   * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@formula1adlsws.dfs.core.windows.net/lap_times");

---SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC  * JSON file
-- MAGIC  * MultiLine JSON
-- MAGIC  * Folder
-- MAGIC     * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "abfss://raw@formula1adlsws.dfs.core.windows.net/qualifying", multiLine true);

---SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------


DESC TABLE f1_raw.lap_times;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED f1_raw.lap_times;

-- COMMAND ----------


