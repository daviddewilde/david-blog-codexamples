# Databricks notebook source
# MAGIC %md
# MAGIC # Interpolate big data time series in native pySpark
# MAGIC Databricks pyspark notebook showing how big data time series can be interpolated in spark without using UDF's

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
import timeit

# COMMAND ----------

nsensors = 10
ntimestamps = 1000
resample_interval = 60*60  # Resample interval size in seconds
interval_from = '2021-01-01 00:00:00'
interval_to = '2021-01-07 00:00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC # Create test data

# COMMAND ----------

# Per sensor create ntimestamps random time values between interval_from and interval_to
# Take a sinus based function to calculate a value (take different magnitude and period per sensor)
df_sensors = spark.range(nsensors).selectExpr('id+1 as SensorId')
df_time = spark.range(ntimestamps).selectExpr('id as Range')
df_test = (df_sensors.join(df_time, how='full')
           .withColumn('UnixTimestamp', F.expr(f"round(rand()*(unix_timestamp('{interval_to}')-unix_timestamp('{interval_from}'))+unix_timestamp('{interval_from}'))"))
           .withColumn('Timestamp', F.expr("to_timestamp(UnixTimestamp)"))
           .withColumn('Value', F.expr(f"SensorId * sin(2*3.14/SensorId*(UnixTimestamp - unix_timestamp('{interval_from}'))/(unix_timestamp('{interval_to}')-unix_timestamp('{interval_from}')))"))
           .drop('UnixTimestamp')
           .drop('Range')
           # Make sure no duplicate rows
           .dropDuplicates(['SensorId', 'Timestamp'])
          )

# COMMAND ----------

df_test.persist()

# COMMAND ----------

df_test.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Resample + interpolate pyspark

# COMMAND ----------

df_pyspark = (
      df_test
      # Get timestamp and value of previous measurement
      .selectExpr(
        "SensorId",
        "LAG(Timestamp) OVER (PARTITION BY SensorId ORDER BY Timestamp ASC) as PreviousTimestamp",
        "Timestamp as NextTimestamp",
        "LAG(Value) OVER (PARTITION BY SensorId ORDER BY Timestamp ASC) as PreviousValue",
        "Value as NextValue",
      )
      # To determine resample interval round up start and round down end timeinterval to nearest interval boundary
      .withColumn("PreviousTimestampRoundUp", F.expr(f"to_timestamp(ceil(unix_timestamp(PreviousTimestamp)/{resample_interval})*{resample_interval})"))
      .withColumn("NextTimestampRoundDown", F.expr(f"to_timestamp(floor(unix_timestamp(NextTimestamp)/{resample_interval})*{resample_interval})"))
      # Make sure we don't get any negative intervals (whole interval is within resample interval)
      .filter("PreviousTimestampRoundUp<=NextTimestampRoundDown")
      # Create resampled time axis by creating all "interval" timestamps between previous and next timestamp
      .withColumn("Timestamp", F.expr(f"explode(sequence(PreviousTimestampRoundUp, NextTimestampRoundDown, interval {resample_interval} second)) as Timestamp"))
      # Interpolate value between previous and next
      .selectExpr(
        "SensorId",
        "Timestamp", 
        """(unix_timestamp(Timestamp)-unix_timestamp(PreviousTimestamp))
            /(unix_timestamp(NextTimestamp)-unix_timestamp(PreviousTimestamp))
            *(NextValue-PreviousValue) 
            +PreviousValue
            as Value"""
      )
)

# COMMAND ----------

# MAGIC %%timeit  -n 1 -r 1
# MAGIC df_pyspark.count()

# COMMAND ----------

display(df_test.filter("SensorId = 1"))

# COMMAND ----------

display(df_pyspark.filter("sensorId=1"))

# COMMAND ----------


