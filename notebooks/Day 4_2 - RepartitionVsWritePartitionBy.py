# Databricks notebook source
file_path = '/databricks-datasets/iot/iot_devices.json'
df = spark.read.json(file_path)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ## Repartition
# MAGIC

# COMMAND ----------

df.repartition(3, "cca3")\
  .write\
  .mode("overwrite")\
  .parquet("./tmp/repartition.parquet/")

# COMMAND ----------

# MAGIC %md ### Notice the output
# MAGIC `.repartition` creates the specified number (3) `.snappy.parquet` files.

# COMMAND ----------

# MAGIC %fs ls ./tmp/repartition.parquet/

# COMMAND ----------

# MAGIC %md ## PartitionBy

# COMMAND ----------

df.write\
  .partitionBy("cca3")\
  .parquet("./tmp/partition-by.parquet/")

# COMMAND ----------

# MAGIC %md ### Notice the File Structure
# MAGIC Compared to a simple `repartition`, `partitionBy` writes your partitions into separate directories

# COMMAND ----------

# MAGIC %fs ls ./tmp/partition-by.parquet/

# COMMAND ----------

# MAGIC %md ### Notice the number of files in each partitioned dir
# MAGIC ... it's still 8 (same as the `repartition` output)

# COMMAND ----------

# MAGIC %fs ls ./tmp/partition-by.parquet/cca3=ABW/

# COMMAND ----------

# MAGIC %md ## Why not both?
# MAGIC You might want to somehow guarantee that there is only one .snappy.parquet file per partition. You'll have to use both the `repartition` and `partitionBy` methods.

# COMMAND ----------

df.repartition(3, "cca3")\
  .write\
  .partitionBy("cca3")\
  .parquet("./tmp/why-not-both.parquet/")


# COMMAND ----------

# MAGIC %fs ls ./tmp/why-not-both.parquet/

# COMMAND ----------

# MAGIC %fs ls ./tmp/why-not-both.parquet/cca3=ABW/

# COMMAND ----------

# MAGIC %md ## Reading in a Partition
# MAGIC Notice that the `cca3` column (the column that you partitioned by) is now gone. This is a feature.

# COMMAND ----------

df = spark.read.parquet("dbfs:/tmp/why-not-both.parquet/cca3=ABW")
df.show()

# COMMAND ----------

# MAGIC %md ### with Wildcards

# COMMAND ----------

df = spark.read.parquet("dbfs:/tmp/why-not-both.parquet/cca3=A*")
df.show()

# COMMAND ----------

# MAGIC %md ## Resources
# MAGIC * [Data Partitioning in PySpark](https://kontext.tech/column/spark/296/data-partitioning-in-spark-pyspark-in-depth-walkthrough)