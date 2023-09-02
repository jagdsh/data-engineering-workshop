# Databricks notebook source
# MAGIC %md Partion and Repartition 
# MAGIC

# COMMAND ----------

simpleNumbers = spark.range(1, 1000000)
times5 = simpleNumbers.selectExpr("id * 5 as id")
times5.show()

# COMMAND ----------

moreNumbers = spark.range(1, 10000000, 2)
split7 = moreNumbers.repartition(7)
split7.take(2)

# COMMAND ----------

ds1 = spark.range(1, 10000000)
ds2 = spark.range(1, 10000000, 2)
ds3 = ds1.repartition(7)
ds4 = ds2.repartition(9)
ds5 = ds3.selectExpr("id * 5 as id")
joined = ds5.join(ds4, "id")
sum = joined.selectExpr("sum(id)")
sum.show()

# COMMAND ----------

extract -h


# COMMAND ----------

