# Databricks notebook source
# MAGIC %pip install dbldatagen python-dateutil

# COMMAND ----------

table_partition = "demo.david.iot_partition"
table_partition_pruning = "demo.david.iot_partition_pruning"
table_clustering = "demo.david.iot_liquid_clustering"
table_clustering_pruning = "demo.david.iot_liquid_clustering_pruning"
table_increment = "demo.david.iot_increment"
table_increment_overlap = "demo.david.iot_increment_overlap"

# COMMAND ----------

from pyspark.sql.types import LongType, IntegerType, StringType
import pyspark.sql.functions as F
import dbldatagen as dg
import datetime
from dateutil import parser

def create_test_data(begin,
                     end,
                     data_rows_per_day=2000000, 
                     device_population=100000, 
                     partitions_requested=40):
    nr_of_days = round((parser.parse(end)-parser.parse(begin)).total_seconds()/24/60/60)
    data_rows = round(nr_of_days*data_rows_per_day)
    country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                    'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
    country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                    17]

    manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

    lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

    testDataSpec = (
        dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                        partitions=partitions_requested,
                        randomSeedMethod='hash_fieldname')
        .withIdOutput()
        # we'll use hash of the base field to generate the ids to
        # avoid a simple incrementing sequence
        .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                    uniqueValues=device_population, omit=True, baseColumnType="hash")

        # note for format strings, we must use "%lx" not "%x" as the
        # underlying value is a long
        .withColumn("device_id", StringType(), format="0x%013x",
                    baseColumn="internal_device_id")

        # the device / user attributes will be the same for the same device id
        # so lets use the internal device id as the base column for these attribute
        .withColumn("country", StringType(), values=country_codes,
                    weights=country_weights,
                    baseColumn="internal_device_id")
        .withColumn("manufacturer", StringType(), values=manufacturers,
                    baseColumn="internal_device_id")

        # use omit = True if you don't want a column to appear in the final output
        # but just want to use it as part of generation of another column
        .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                    baseColumnType="hash", omit=True)
        .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                    baseColumn="device_id",
                    baseColumnType="hash", omit=True)

        .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                    baseColumn=["line", "model_ser"])
        .withColumn("event_type", StringType(),
                    values=["activation", "deactivation", "plan change",
                            "telecoms activity", "internet activity", "device error"],
                    random=True)
        .withColumn("event_ts", "timestamp",
                    begin=begin, end=end,
                    interval="1 minute",
                    random=True)

        )

    dfTestData = testDataSpec.build().withColumn('date',F.expr('to_date(event_ts)'))
    return dfTestData

# COMMAND ----------

# MAGIC %md # Create target tables

# COMMAND ----------

dfTestData = create_test_data("2020-01-01 00:00:00", "2022-12-31 23:59:59", partitions_requested=256)
dfTestData.write.partitionBy('date').mode('overwrite').saveAsTable(table_partition)

# COMMAND ----------

spark.sql(f'OPTIMIZE {table_partition}')

# COMMAND ----------

spark.sql(f'CREATE TABLE {table_clustering} CLUSTER BY (`date`) AS SELECT * FROM {table_partition}')
spark.sql(f'CREATE TABLE {table_clustering_pruning} CLUSTER BY (`date`) AS SELECT * FROM {table_partition}')
spark.sql(f'CREATE TABLE {table_clustering}_photon CLUSTER BY (`date`) AS SELECT * FROM {table_partition}')
spark.sql(f'CREATE TABLE {table_clustering_pruning}_photon CLUSTER BY (`date`) AS SELECT * FROM {table_partition}')

# COMMAND ----------

spark.sql(f"OPTIMIZE {table_clustering}")
spark.sql(f"OPTIMIZE {table_clustering_pruning}")
spark.sql(f"OPTIMIZE {table_clustering}_photon")
spark.sql(f"OPTIMIZE {table_clustering_pruning}_photon")

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_partition_pruning} DEEP CLONE {table_partition}""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_partition+'_photon'} DEEP CLONE {table_partition}""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_partition_pruning+'_photon'} DEEP CLONE {table_partition}""")


# COMMAND ----------

# MAGIC %md # Create Increment Table

# COMMAND ----------

df_increment = create_test_data("2023-01-01 00:00:00", "2023-01-01 23:59:59", partitions_requested=32)
df_increment.write.mode('overwrite').saveAsTable(table_increment)

# COMMAND ----------

spark.sql(f'OPTIMIZE {table_increment}')

# COMMAND ----------

# MAGIC %md # Create Overlapping Increment Table

# COMMAND ----------

# 4 days overlap 1 new day
df_increment_overlap = create_test_data("2022-12-29 00:00:00", "2023-01-02 23:59:59", partitions_requested=32)
df_increment_overlap.write.mode('overwrite').saveAsTable(table_increment_overlap)

# COMMAND ----------

spark.sql(f'OPTIMIZE {table_increment_overlap}')

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS demo.david.iot_test DEEP CLONE {table_partition}""")
