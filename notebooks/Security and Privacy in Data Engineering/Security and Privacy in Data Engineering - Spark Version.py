# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Security & Privacy in Workflows: Spark Solutions
# MAGIC
# MAGIC In this notebook, you'll find solutions as to one way to solve the translation between Python processing and Spark. Remember: there are usually multiple ways to solve a problem in Spark! So long as yours worked, please take these as simply suggestions!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Our Goal: Build a Privacy-First Sensor Map
# MAGIC
# MAGIC - We want to ingest air quality sensor data from users, buildings and institutions who are willing to send us data to build an air quality map (similar to the [IQAir map](https://www.iqair.com/air-quality-map).
# MAGIC - Users only want to share the data if they can remain anonymous and their location is fuzzy, so that they are protected against stalkers, prying eyes and state surveillance.
# MAGIC - Since the data is sensitive (from people and their homes!), we want to sure that it is secured either at collection, as well as at any intermediary hops.
# MAGIC
# MAGIC Let's first take a look at our data and determine what can and should be done...

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import when, split, udf, round as pys_round,\
    regexp_replace, rand, to_timestamp, expr, unix_timestamp

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("data/air_quality.csv", header=True, inferSchema=True)

# COMMAND ----------

df.head()

# COMMAND ----------

df.select('location').show(1, truncate=False)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("location", regexp_replace("location", "[()']", ""))

# COMMAND ----------

df.select('location').show(1, truncate=False)

# COMMAND ----------

df = df.withColumn("location_arr", split("location", ", "))

# COMMAND ----------

df.head()

# COMMAND ----------

df = df.withColumn('lat', df.location_arr.getItem(0).cast('float'))
df = df.withColumn('long', df.location_arr.getItem(1).cast('float'))
df = df.withColumn('city', df.location_arr.getItem(2))
df = df.withColumn('country', df.location_arr.getItem(3))
df = df.withColumn('timezone', df.location_arr.getItem(4))

# COMMAND ----------

cleaned_df = df.drop('location', 'location_arr')

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC What else is missing for our map? It seems like on IQAir's map they have categories of pollutants. We likely want to do something similar to break our map down into colors and ranges. 
# MAGIC
# MAGIC Based on the IQAir map, the ranges look to be about:
# MAGIC
# MAGIC - Great: less than or equal to 50
# MAGIC - Good: 51-100
# MAGIC - Okay: 101-150
# MAGIC - Poor: 151-200
# MAGIC - Bad: 201-300
# MAGIC - Extremely Bad: 301+
# MAGIC
# MAGIC Let's make these into integer values 1-6

# COMMAND ----------

cleaned_df = cleaned_df.withColumn('air_quality_category', when(
    cleaned_df.air_quality_index <= 50, 1).when(
    cleaned_df.air_quality_index <= 100, 2).when(
    cleaned_df.air_quality_index <= 150, 3).when(
    cleaned_df.air_quality_index <= 200, 4).when(
    cleaned_df.air_quality_index <= 300, 5).otherwise(6))

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is sensitive here?

# COMMAND ----------

cleaned_df.sample(0.01).show(3, truncate=False, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### How might we...?
# MAGIC
# MAGIC - Protect user_id while still allowing it to be linkable?
# MAGIC - Remove potentially identifying precision in location?
# MAGIC - Remove potentially identifying information in the timestamp?
# MAGIC - Make these into scalable and repeatable actions for our workflow?
# MAGIC
# MAGIC Let's work on these step by step!

# COMMAND ----------

from ff3 import FF3Cipher
key = "2DE79D232DF5585D68CE47882AE256D6"
tweak = "CBD09280979564"

c6 = FF3Cipher.withCustomAlphabet(key, tweak, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")

plaintext = "michael______"
ciphertext = c6.encrypt(plaintext)

ciphertext

# COMMAND ----------

decrypted = c6.decrypt(ciphertext)
decrypted

# COMMAND ----------

def encrypt_username(username):
    c6 = FF3Cipher.withCustomAlphabet(key, tweak, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
    return c6.encrypt(username)

# COMMAND ----------

encrypt_username_udf = udf(lambda z: encrypt_username(z), StringType())

# COMMAND ----------

cleaned_df.select(encrypt_username_udf("user_id").alias("user_id")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like it's working, but with UDFs you never know. Remember, Spark function evaluation is LAZY, so it will sample a bit and test. To see if it will work on the entire dataframe, we need to call collect. Let's test it out!

# COMMAND ----------

cleaned_df.select(encrypt_username_udf("user_id")).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Oh no! What happened here???

# COMMAND ----------

def add_padding_and_encrypt(username):
    c6 = FF3Cipher.withCustomAlphabet(key, tweak, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
    if len(username) < 4:
        username += "X" * (4-len(username))
    return c6.encrypt(username)

pad_and_encrypt_username_udf = udf(lambda y: add_padding_and_encrypt(y), StringType())

cleaned_df.select(pad_and_encrypt_username_udf("user_id")).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC This looks like it works now! Let's add it as a column.

# COMMAND ----------

cleaned_df = cleaned_df.withColumn('user_id', pad_and_encrypt_username_udf("user_id"))

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC We are now technically leaking length information... which we could determine is okay, so long as access to this data and the real data is fairly controlled. We could also say that we want to by default add padding to every username to make them consistent. This would be a good homework exercise (and also to write a function to decrypt and remove padding!!). One challenge, what happens if my username ends in X??? :) 
# MAGIC
# MAGIC
# MAGIC Now we can move onto our GPS data!
# MAGIC
# MAGIC How precise is GPS data anyways? 🤔 (from [wikipedia](https://en.wikipedia.org/wiki/Decimal_degrees))
# MAGIC
# MAGIC
# MAGIC decimal places  | degrees  |distance
# MAGIC ------- | -------          |--------
# MAGIC 0        |1                |111  km
# MAGIC 1        |0.1              |11.1 km
# MAGIC 2        |0.01             |1.11 km
# MAGIC 3        |0.001            |111  m
# MAGIC 4        |0.0001           |11.1 m
# MAGIC 5        |0.00001          |1.11 m
# MAGIC 6        |0.000001         |11.1 cm
# MAGIC 7        |0.0000001        |1.11 cm
# MAGIC 8        |0.00000001       |1.11 mm

# COMMAND ----------

cleaned_df.show(2, vertical=True)

# COMMAND ----------

cleaned_df = cleaned_df.withColumn('lat', pys_round('lat', 3))
cleaned_df = cleaned_df.withColumn('long', pys_round('long', 3))

# COMMAND ----------

cleaned_df.show(2, vertical=True, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC What type of risk should we be aware of with regard to timestamp precision? When and how do we need to de-risk this  type of information?

# COMMAND ----------

cleaned_df = cleaned_df.withColumn('timestamp', to_timestamp('timestamp'))

# COMMAND ----------

cleaned_df.withColumn('new_timestamp', (unix_timestamp('timestamp') + 
                                    (rand() * 60) + (rand() * 60 * 20)).cast('timestamp')).select('timestamp', 'new_timestamp').show(truncate=False)

# COMMAND ----------

cleaned_df = cleaned_df.withColumn('timestamp', (unix_timestamp('timestamp') + 
                        (rand() * 60) + (rand() * 60 * 20)).cast('timestamp'))

# COMMAND ----------

cleaned_df.head()

# COMMAND ----------

cleaned_df = cleaned_df.orderBy(cleaned_df.timestamp.asc())

# COMMAND ----------

!rm -rf data/data_for_marketing

# COMMAND ----------

cleaned_df.write.format('csv').save('data/data_for_marketing')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations!! 
# MAGIC
# MAGIC You've walked through potential privacy snags and helped increase the protection for the individuals sending you their air quality details! Now developers can use this dataset and we have ensured that there are some base protections. As you may have noticed, it wasn't always obvious what we should do -- but by thinking through each data type and determining what worked to balance the utility of the data and the privacy we want to offer, we were able to find some ways to protect individuals. 
# MAGIC
# MAGIC A good set of questions to ask for guidance is:
# MAGIC
# MAGIC - Where will this data be accessed and used? How safe is this environment?
# MAGIC - What person-related data do we actually need to use to deliver this service or product? (data minimization!)
# MAGIC - What other protections will be added to this data before it is seen or used? (i.e. encryption at rest, access control systems, or other protections when it reaches another processing point or sink!)
# MAGIC - What privacy and security expectations do we want to set for the individuals in this dataset?
# MAGIC - Where can we opportunistically add more protection while not hindering the work of data scientists, data analysts, software engineers and other colleagues?
# MAGIC
# MAGIC
# MAGIC As you continue on in your data engineering journey, you'll likely encounter many more situations where you'll need to make privacy and security decisions. If you'd like to learn more and even work as a privacy or security champion -- feel free to join in your organizations' programs to support topics like this!

# COMMAND ----------



# COMMAND ----------

