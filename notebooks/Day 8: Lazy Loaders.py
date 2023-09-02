# Databricks notebook source
#%pip install opendatasets
%pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers 

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, "LazyLoaders_project_suicidal_ingestion")

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")


# COMMAND ----------

"""import opendatasets as od

data_dir = f"{working_directory}/data/"
print(f"Data Directory: {data_dir}")
    
od.download("https://www.kaggle.com/datasets/kumaranand05/who-suicide-data-1950-2021/download?datasetVersionNumber=2",data_dir)"""

# COMMAND ----------



# COMMAND ----------

helpers.clean_working_directory()
url = "https://raw.githubusercontent.com/jagdsh/who_Suicide_Data_1950-2021/main/combined_processed_data.csv"
filepath = helpers.download_to_local_dir(url)
print(filepath)

# COMMAND ----------

display(dbutils.fs.ls(f"{working_directory}"))

# COMMAND ----------

# data = spark.read.format("csv") \
#       .option("header", True) \
#       .option("inferSchema", True) \
#       .option("delimiter", ",") \
#       .load("/FileStore/jagdshlk/LazyLoaders_project_suicidal_ingestion/combined_processed_data.csv")

# display(data.printSchema())

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

#filepath = "/FileStore/jagdshlk/LazyLoaders_project_suicidal_ingestion/combined_processed_data.csv"
def create_dataframe(filepath: str) -> DataFrame:
    custom_schema = StructType([
    StructField("Region Name", StringType(), True),
    StructField("Country Name", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Sex", StringType(), True),
    StructField("0_Years", DoubleType(), True),
    StructField("1-4_Years", DoubleType(), True),
    StructField("5-9_Years", DoubleType(), True),
    StructField("10-14 Years", DoubleType(), True),
    StructField("15-19_Years", DoubleType(), True),
    StructField("20-24_Years", DoubleType(), True),
    StructField("25_29_Years", DoubleType(), True),
    StructField("30-34_Years", DoubleType(), True),
    StructField("35-39_Years", DoubleType(), True),
    StructField("40-44_Years", DoubleType(), True),
    StructField("45-49_Years", DoubleType(), True),
    StructField("50-54_Years", DoubleType(), True),
    StructField("55-59_Years", DoubleType(), True),
    StructField("60-64_Years", DoubleType(), True),
    StructField("65-69_Years", DoubleType(), True),
    StructField("70-74_Years", DoubleType(), True),
    StructField("75-79_Years", DoubleType(), True),
    StructField("80-84_Years", DoubleType(), True),
    StructField("85+_Years", DoubleType(), True),
    StructField("Unknown_Age", DoubleType(), True),
    StructField("No_of_Suicides", DoubleType(), True),
    StructField("Percentage_of_cause_specific_deaths_out_of_total_deaths", DoubleType(), True),
    StructField("Death_rate_per_100000_population", DoubleType(), True),
    ])
      
    df = spark.read.format("csv") \
    .option("header", True) \
    .option("delimiter", ",") \
    .schema(custom_schema) \
    .load(filepath)

    return df
    
input_df = create_dataframe(filepath)
display(input_df)


# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count

input_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in input_df.columns]).show()

# COMMAND ----------

display(input_df.select("*").where(input_df.Death_rate_per_100000_population < input_df.Percentage_of_cause_specific_deaths_out_of_total_deaths))

input_df = input_df.select("*").where(input_df.Death_rate_per_100000_population > input_df.Percentage_of_cause_specific_deaths_out_of_total_deaths)

display(input_df)

# COMMAND ----------

out_dir = f"{working_directory}/validated_output_silver/"
print(f"Output Directory: {out_dir}")
input_df. \
        write. \
        mode("overwrite"). \
        parquet(out_dir)

display(dbutils.fs.ls(f"{out_dir}"))

# COMMAND ----------



# COMMAND ----------

