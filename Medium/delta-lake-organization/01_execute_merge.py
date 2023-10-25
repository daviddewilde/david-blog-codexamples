# Databricks notebook source
import time
import functools

# COMMAND ----------

photon = spark.sparkContext.getConf().get("spark.databricks.clusterUsageTags.runtimeEngine")=='PHOTON'

# COMMAND ----------

table_partition = f"demo.david.iot_partition{'_photon' if photon else ''}"
table_partition_pruning = f"demo.david.iot_partition_pruning{'_photon' if photon else ''}"
table_clustering = f"demo.david.iot_liquid_clustering{'_photon' if photon else ''}"
table_clustering_pruning = f"demo.david.iot_liquid_clustering_pruning{'_photon' if photon else ''}"

table_increment = "demo.david.iot_increment"
table_increment_overlap = "demo.david.iot_increment_overlap"

# COMMAND ----------

# MAGIC %md # Get Increment Tables

# COMMAND ----------

spark.table(table_increment).createOrReplaceTempView('table_increment')
spark.table(table_increment_overlap).createOrReplaceTempView('table_increment_overlap')
spark.sql("CACHE TABLE table_increment")
spark.sql("CACHE TABLE table_increment_overlap")

# COMMAND ----------

spark.table('table_increment').count()
spark.table('table_increment_overlap').count()

# COMMAND ----------

# MAGIC %md # Partitions

# COMMAND ----------

def timer_func(func): 
    # This function shows the execution time of  
    # the function object passed 
    def wrap_func(*args, **kwargs): 
        t1 = time.time() 
        result = func(*args, **kwargs) 
        t2 = time.time() 
        duration = t2-t1
        print(f'{duration=}') 
        return duration, result 
    return wrap_func 
  
@timer_func 
def merge_increment(source, target, pruning_col):
  statement = f"""
  MERGE INTO {target} AS t
  USING {source} AS s
  ON s.id=t.id
  AND s.{pruning_col}=t.{pruning_col}
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT * 
  """
  return spark.sql(statement)

@timer_func
def merge_increment_pruning(source, target, pruning_col):
  # Determine dynamic file pruning filters by getting unique values of pruning_col in the increment
  partition_values = spark.sql(f"""SELECT DISTINCT {pruning_col} FROM {source}""").collect()
  partition_statement = f"t.{pruning_col} IN ("+ (', '.join([f"""'{d["date"].strftime("%Y-%m-%d")}'""" for d in partition_values]))+ ")"
  # Create the dynamic MERGE statement
  statement = f"""
  MERGE INTO {target} AS t
  USING {source} AS s
  ON s.id=t.id
  AND s.{pruning_col}=t.{pruning_col}
  AND {partition_statement}
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT * 
  """
  # Execute merge statement
  return spark.sql(statement)


# COMMAND ----------

# MAGIC %md # Execute increment

# COMMAND ----------

partition_duration, partition_result = merge_increment('table_increment', table_partition, 'date')
partition_pruning_duration, partition_pruning_result = merge_increment_pruning('table_increment', table_partition_pruning, 'date')
clustering_duration, clustering_result = merge_increment('table_increment', table_clustering, 'date')
clustering_pruning_duration, clustering_pruning_result = merge_increment_pruning('table_increment', table_clustering_pruning, 'date')
print(', '.join([str(x) for x in [partition_duration, partition_pruning_duration, clustering_duration, clustering_pruning_duration]]))

# COMMAND ----------

# MAGIC %md # Execute increment overlap

# COMMAND ----------

partition_duration, partition_result = merge_increment('table_increment_overlap', table_partition, 'date')
partition_pruning_duration, partition_pruning_result = merge_increment_pruning('table_increment_overlap', table_partition_pruning, 'date')
clustering_duration, clustering_result = merge_increment('table_increment_overlap', table_clustering, 'date')
clustering_pruning_duration, clustering_pruning_result = merge_increment_pruning('table_increment_overlap', table_clustering_pruning, 'date')
print(', '.join([str(x) for x in [partition_duration, partition_pruning_duration, clustering_duration, clustering_pruning_duration]]))

# COMMAND ----------


