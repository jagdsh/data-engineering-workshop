# Databricks notebook source
# MAGIC %pip install wget ff3 pandas datafuzz

# COMMAND ----------

# Clear out existing working directory

current_user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]
working_directory=f"/FileStore/{current_user}/dataSecurity"
dbutils.fs.rm(working_directory, True)

# COMMAND ----------

# Function to download files to DBFS

import os
import wget
import sys
import shutil

sys.stdout.fileno = lambda: False # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'   

def clean_remake_dir(dir):
    if os.path.isdir(local_tmp_dir): shutil.rmtree(local_tmp_dir)
    os.makedirs(local_tmp_dir)
    

def download_to_local_dir(local_dir, target_dir, url, filename_parsing_lambda):
    filename = (filename_parsing_lambda)(url)
    tmp_path = f"{local_dir}/{filename}"
    target_path = f"{target_dir}/{filename}"
    if os.path.exists(tmp_path):
        os.remove(tmp_path) 
    
    saved_filename = wget.download(url, out = tmp_path)
    
    if target_path.endswith(".zip"):
        with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
            zip_ref.extractall(local_dir)

    dbutils.fs.cp(f"file:{local_dir}/", target_dir, True)
    
    return target_path

# COMMAND ----------

local_tmp_dir = f"{os.getcwd()}/{current_user}/dataSecurity/tmp"
clean_remake_dir(local_tmp_dir)

urls = [
    "https://raw.githubusercontent.com/data-derp/exercise-data-security/master/data/air_quality.csv"
]
    
# target_directory = f"/FileStore/{current_user}/dataSecurity"
    
for url in urls:    
    download_to_local_dir(local_tmp_dir, f"{working_directory}/data", url, lambda y: y.split("/")[-1])
    

# COMMAND ----------

dbutils.fs.ls(f"{working_directory}/data")

# COMMAND ----------

import pandas as pd 
from IPython.display import Image
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import when, split, udf, round as pys_round,\
    regexp_replace, rand, to_timestamp, expr, unix_timestamp

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv(f"{working_directory}/data", header=True, inferSchema=True)

# COMMAND ----------

df.head()

# COMMAND ----------

display(df)

# COMMAND ----------

cleaned_df = df.withColumn('air_quality_category', when(
    df.air_quality_index <= 50, 1).when(
    df.air_quality_index <= 100, 2).when(
    df.air_quality_index <= 150, 3).when(
    df.air_quality_index <= 200, 4).when(
    df.air_quality_index <= 300, 5).otherwise(6))

# COMMAND ----------

def parse_location(location_string):
     location_keys = ['lat','long','city','country','timezone']
     location_vals = [substring for substring in location_string.split["'"]
                      if len(substring.replace(' ','').replace(',','')) > 1]
     return dict(zip(location_keys, location_vals))

# COMMAND ----------

cleaned_df.select('location').show(1, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col



silver_df = cleaned_df.withColumn("Lat", regexp_replace(split(cleaned_df["location"], "',")[0], "\(.", "").cast("double")) \
    .withColumn("Long", regexp_replace(split(cleaned_df["location"], ",")[1], "'", "").cast("double")) \
    .withColumn("City", regexp_replace(split(cleaned_df["location"], ",")[2], "'", "")) \
    .withColumn("Country", regexp_replace(split(cleaned_df["location"], ",")[3], "'", "")) \
    .withColumn("Timezone", regexp_replace(split(cleaned_df["location"], ",")[4], "'", "")
).drop("location")


display(silver_df)

# COMMAND ----------

from ff3 import FF3Cipher
key = "2DE79D232DF5585D68CE47882AE256D6"
tweak = "CBD09280979564"


# COMMAND ----------

def encrypt(text):
    display(text)
    c6 = FF3Cipher.withCustomAlphabet(key, tweak, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
    return c6.encrypt(plaintext)

# COMMAND ----------

plaintext = "micheal_"
# display(encrypt(plaintext))
# display(c6.decrypt(encrypt(plaintext)))

# COMMAND ----------

encrypt_username_udf = udf(lambda z: encrypt(z), StringType())

# COMMAND ----------

# encrypted_df = silver_df.withColumn("userid_encrypted", encrypt_username_udf(silver_df['user_id'])).show()
silver_df.select(encrypt_username_udf("user_id").alias("user_id")).show(truncate=False)

# COMMAND ----------

