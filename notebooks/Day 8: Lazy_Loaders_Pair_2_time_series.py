# Databricks notebook source


# COMMAND ----------



# COMMAND ----------

# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers 
# MAGIC %pip install mlforecast
# MAGIC %pip install xgboost

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, "LazyLoaders_project_time_series")

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")


# COMMAND ----------

helpers.clean_working_directory()
url = "https://raw.githubusercontent.com/jagdsh/who_Suicide_Data_1950-2021/main/combined_processed_data.csv"
filepath = helpers.download_to_local_dir(url)
print(filepath)
display(dbutils.fs.ls(f"{working_directory}"))

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

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

out_dir = f"{working_directory}/validated_output_silver/"

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(out_dir)

display(df)
df = df.toPandas()

# COMMAND ----------

from sklearn import preprocessing

# Data PreProcessing
def process_categorical(df, col):
    le = preprocessing.LabelEncoder()
    le.fit(df[col])
    le.classes_
    df[col] = le.transform(df[col])
    return le, df

col_transform = ["Region Name", "Country Name", "Sex"]
label_encoders = dict()
processed_cat_df = df.copy()
for col in col_transform:
    label_encoders[col], processed_cat_df = process_categorical(processed_cat_df, col)


display(processed_cat_df)
print("*"*10, "col: ", {col: label_encoders[col].classes_ for col in col_transform})

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import make_pipeline
from xgboost import XGBRegressor
from mlforecast import MLForecast
import pandas as pd

from window_ops.rolling import rolling_mean, rolling_max, rolling_min

models = [make_pipeline(MinMaxScaler(), 
                        RandomForestRegressor(random_state=0, n_estimators=20)),
           XGBRegressor(random_state=0, n_estimators=20)]


model = MLForecast(models=models,
                   freq=1,
                   lags=[1,2,4],
                   lag_transforms={
                       1: [(rolling_mean, 4), (rolling_min, 4), (rolling_max, 4)], # aplicado a uma janela W a partir do registro Lag
                   },
                   num_threads=6)

# COMMAND ----------

#processed_cat_df['Year'] = pd.to_datetime(processed_cat_df['Year'])

# COMMAND ----------

dynamic_features = ['Country Name', 'Sex', '0_Years', '1-4_Years',
       '5-9_Years', '10-14 Years', '15-19_Years', '20-24_Years', '25_29_Years',
       '30-34_Years', '35-39_Years', '40-44_Years', '45-49_Years',
       '50-54_Years', '55-59_Years', '60-64_Years', '65-69_Years',
       '70-74_Years', '75-79_Years', '80-84_Years', '85+_Years', 'Unknown_Age']
static_features = []


# COMMAND ----------

processed_cat_df.head()

# COMMAND ----------

h = 1
#cols_drop = ["Death_rate_per_100000_population", "No_of_Suicides"]
#processed_cat_df = processed_cat_df.drop(cols_drop, 1)
train = processed_cat_df.loc[processed_cat_df['Year'] < 2017]
valid = processed_cat_df.loc[processed_cat_df['Year'] >= 2017]
model.fit(train, id_col='Region Name', time_col='Year', target_col='Percentage_of_cause_specific_deaths_out_of_total_deaths')
predictions = model.predict(horizon=h, dynamic_dfs=[train[['Region Name','Year']+dynamic_features]])
predictions_with_label = predictions.merge(valid[['Region Name', 'Year', 'Percentage_of_cause_specific_deaths_out_of_total_deaths']], on=['Region Name', 'Year'], how='left')
display(predictions_with_label)

# COMMAND ----------

h = 5
predictions = model.predict(horizon=h, dynamic_dfs=[valid[['Region Name','Year']+dynamic_features]])
predictions_with_label = predictions.merge(valid[['Region Name', 'Year', 'Percentage_of_cause_specific_deaths_out_of_total_deaths']], on=['Region Name', 'Year'], how='left')
display(predictions_with_label)

# COMMAND ----------

import numpy as np

def smape(y_true, y_pred):
    return 100 * np.mean(np.abs(y_pred - y_true) / (np.abs(y_true) + np.abs(y_pred)))
   
print(f"SMAPE Random Forest Pipeline: {smape(predictions_with_label['Percentage_of_cause_specific_deaths_out_of_total_deaths'], predictions_with_label['Pipeline'])}\nSMAPE XGBRegressor: {smape(predictions_with_label['Percentage_of_cause_specific_deaths_out_of_total_deaths'], predictions_with_label['XGBRegressor'])}")




# COMMAND ----------

features_importance_df = pd.Series(model.models_['XGBRegressor'].feature_importances_, index=model.ts.features_order_).sort_values(ascending=False)
features_importance_df = features_importance_df.drop(["lag1", "lag2", "lag4", "rolling_mean_lag1_window_size4", "rolling_min_lag1_window_size4", "rolling_max_lag1_window_size4"])
features_importance_df.plot.bar(title='Feature Importance XGBRegressor')

# COMMAND ----------

def process_categorical_decode(df, col, label_encoder):
    le = label_encoder[col]
    df[col] = le.inverse_transform(df[col])
    return df

col_transform_val = ["Region Name"]
for col in col_transform_val:
     predictions_with_label = process_categorical_decode(predictions_with_label, col, label_encoders)

# COMMAND ----------

display(predictions_with_label)

# COMMAND ----------

# Fig show for Region Name

import plotly.express as px


df = px.data.tips()
fig = px.histogram(predictions_with_label, x="Year", y="Percentage_of_cause_specific_deaths_out_of_total_deaths", color="Region Name", marginal="rug", hover_data=predictions_with_label.columns)
fig.show()


df = px.data.tips()
fig = px.histogram(predictions_with_label, x="Year", y="Pipeline", color="Region Name", marginal="rug", hover_data=predictions_with_label.columns)
fig.show()

# COMMAND ----------

# import graph_objects from plotly package
import plotly.graph_objects as go
 
# import make_subplots function from plotly.subplots
# to make grid of plots
from plotly.subplots import make_subplots
 
# use specs parameter in make_subplots function
# to create secondary y-axis

for reg in list(label_encoders["Region Name"].classes_):
    fig = make_subplots()
    
    # plot a scatter chart by specifying the x and y values
    # Use add_trace function to specify secondary_y axes.
    fig.add_trace(
        go.Scatter(x=predictions_with_label["Year"][predictions_with_label["Region Name"] == reg],
                    y=predictions_with_label["Pipeline"][predictions_with_label["Region Name"] == reg],
                    name="Predicted with RandomForestRegressor"),)

    fig.add_trace(
        go.Scatter(x=predictions_with_label["Year"][predictions_with_label["Region Name"] == reg],
                    y=predictions_with_label["XGBRegressor"][predictions_with_label["Region Name"] == reg],
                    name="Predicted with XGB regressor"))
    
    # Use add_trace function and specify secondary_y axes = True.
    fig.add_trace(
        go.Scatter(x=predictions_with_label["Year"][predictions_with_label["Region Name"] == reg],
                    y=predictions_with_label["Percentage_of_cause_specific_deaths_out_of_total_deaths"][predictions_with_label["Region Name"] == reg], name="True Value"))
    
    # Adding title text to the figure
    fig.update_layout(
        title_text= reg + " Predicted vs true"
    )
    
    # Naming x-axis
    fig.update_xaxes(title_text="<b>Years</b>")
    
    # Naming y-axes
    fig.update_yaxes(title_text="<b>Deaths by Suicide (%)</b>", secondary_y=False)
    fig.show()