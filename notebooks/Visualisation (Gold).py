# Databricks notebook source
# MAGIC %md
# MAGIC # Visualisations
# MAGIC In this exercise, we'll take our data from the Gold layer and create a visualisation. We want to show a distribution of charge dispensed along with the mean, median and range.
# MAGIC
# MAGIC There are many tools that we can use to generate Visualisations. This exercise will explore visualisation creation using:
# MAGIC * Databricks Graphs
# MAGIC * Plotly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers
# MAGIC %pip install dash

# COMMAND ----------

exercise_name = "visualisation"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Gold Layer
# MAGIC Let's read the parquet files that we created in the Gold layer!

# COMMAND ----------

input_dir = working_directory.replace(exercise_name, "batch_processing_gold")
print(input_dir)


url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-gold-2/output/cdr/part-00000-tid-8188744955325376871-3934a431-560c-4ff0-b57c-88baab52939b-566-1-c000.snappy.parquet"

filepath = helpers.download_to_local_dir(url)


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(filepath)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Visualisations with Databricks
# MAGIC Let's start with the easy one. Databricks comes with Visualisation and Dashboarding capabilities. When we `display` our DataFrame in Databricks, we have the possibility to create a Visualisation from it.
# MAGIC
# MAGIC 1. Click the `+` button next to the display `Table`.
# MAGIC
# MAGIC ![databricks-click-visualisation.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-click-visualisation.png?raw=true)
# MAGIC
# MAGIC
# MAGIC 2. Then, use the Visualisation Editor to create a Histogram:
# MAGIC ![databricks-visualisation-editor-histogram.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-visualisation-editor-histogram.png?raw=true)
# MAGIC
# MAGIC 3. The resulting Histogram should look as follows:
# MAGIC ![databricks-histogram-total-parking-time.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-histogram-total-parking-time.png?raw=true)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Try playing with the bin size. How does the graph change? How can this graph be interpreted?

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks also has Dashboarding capabilities! Click the drop down arrow on the Histogram you just created and click **Add to Dashboard**.
# MAGIC
# MAGIC ![databricks-histogram-add-to-dashboard.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-histogram-add-to-dashboard.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC Feel free to try this with other fields in the DataFrame!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisations with Plotly
# MAGIC Outside of Databricks, there are many other visualisation tools. One of the more customisable ones is called Plotly. Before we get started on the exercise, we'll quickly review [Plotly](https://plotly.com/).
# MAGIC
# MAGIC Examples from [Plotly](https://plotly.com/python/getting-started/)

# COMMAND ----------

import plotly.express as px

# We see that set labels on the x-axis and the values on the y-axis.
fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
fig.write_html('first_figure.html', auto_open=True)
fig

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### EXERCISE: Histograms
# MAGIC Plot the `total_energy` in a [Histogram](https://plotly.com/python/histograms/) to show the distribution of the energy dispensed across all of our data. Note: Plotly can't use Spark DataFrames but rather Pandas DataFrames (convert the Spark DataFrame to a PandasDataFrame using the [toPandas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html) function).

# COMMAND ----------

import plotly.express as px

### YOUR CODE HERE
pandasDF = df.toPandas()
fig = px.histogram(pandasDF, x="total_time")
###

fig.show()

# COMMAND ----------

import plotly.express as px

### YOUR CODE HERE
pandasDF = df.toPandas()
fig = px.histogram(pandasDF, x="charge_point_id")
###

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflect
# MAGIC * What kind of information can you extract from this plot? 
# MAGIC * Is this the most effective way to tell a story about this data?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### EXERCISE: Box Plots
# MAGIC Plot the `total_energy` in a [Box Plot](https://plotly.com/python/box-plots/) to show the distribution of the energy dispensed across all of our data (similar to our Histogram). Note: Plotly can't use Spark DataFrames but rather Pandas DataFrames (convert the Spark DataFrame to a PandasDataFrame using the [toPandas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html) function).

# COMMAND ----------

import plotly.express as px

### YOUR CODE HERE
pandasDF = df.toPandas()
fig = px.box(pandasDF, y="total_energy")
###

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflect
# MAGIC * What kind of information can you extract from this plot? 
# MAGIC * Is this the most effective way to tell a story about this data? 
# MAGIC * Why are Box plots more informative than Histograms?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Topic: Dashboards (Bonus)
# MAGIC Everyone talks about Dashbaords. It's possible to go beyond our singular visualisations and compile them into a dashboard for someone to read. There are a variety of tools that exist, such as Dash, which works great with Plotly, and [StreamLit](https://streamlit.io/).
# MAGIC
# MAGIC When working with Dash, independent applications can be built and deployed to production environments. Dash applications can also run in-line in notebooks and also [within Databricks](https://medium.com/plotly/building-plotly-dash-apps-on-a-lakehouse-with-databricks-sql-advanced-edition-4e1015593633).

# COMMAND ----------

from dash import Dash, dcc, html, Input, Output, callback
import dash_mantine_components as dmc
import datetime as dt

from utils import dbx_utils, figures
import utils.components as comp
from constants import app_description

app = Dash(__name__, title="dash-dbx", update_title=None)
server = app.server

app.layout = dmc.MantineProvider(
    withGlobalStyles=True,
    theme={"colorScheme": "dark"},
    children=dmc.NotificationsProvider(
        [
            ## Header and app description
            comp.header(
                app,
                "#FFFFFF",
                "Dash with Databricks",
                header_background_color="#111014",
            ),
            comp.create_text_columns(app_description, "description"),
            ## Tab Selection and their Content
            dmc.Tabs(
                grow=True,
                variant="outline",
                children=[
                    dmc.Tab(
                        label="Population level visualizations", children=comp.LEFT_TAB
                    ),
                    dmc.Tab(label="Specific User Metrics", children=comp.RIGHT_TAB),
                ],
            ),
            ## Notification containers and affixes
            html.Div(id="notifications-user"),
            html.Div(id="notifications-scatter"),
            html.Div(id="notifications-line"),
            html.Div(id="notifications-heatmap"),
            dcc.Interval(id="interval", interval=1_000),
            dmc.Affix(
                html.A(
                    "See code",
                    href="https://github.com/plotly/dash-dbx-sql",
                    target="_blank",
                    className="demo-button",
                ),
                position={"bottom": 40, "left": 20},
            ),
            dmc.Affix(dmc.Text(id="time"), position={"bottom": 5, "left": 5}),
        ]
    ),
)


@callback(Output("time", "children"), Input("interval", "n_intervals"))
def refresh_data_at_interval(interval_trigger):
    """
    This simple callback demonstrates how to use the Interval component to update data at a regular interval.
    This particular example updates time every second, however, you can subsitute this data query with any acquisition method your product requires.
    """
    return dt.datetime.now().strftime("%M:%S")


@callback(
    Output("user-demo", "children"),
    Output("user-comp", "children"),
    Output("user-header", "children"),
    Output("user-metrics-fig", "figure"),
    Output("notifications-user", "children"),
    Input("user-id", "value"),
    Input("user-fit", "value"),
)
def make_userpage(userid, fitness):
    df_userdemo, df_userfit = dbx_utils.get_user_data(int(userid), fitness)
    fig_user = figures.generate_userbar(df_userfit, fitness, userid)
    df_usercomp = dbx_utils.get_user_comp(fitness)

    header = f"Patient {userid}'s fitness data"
    user_comparison = comp.generate_usercomp(df_usercomp, userid, fitness)
    blood_pressure = dmc.Text(
        f"Blood Pressure Level: {df_userdemo['bloodpressure'][0]}"
    )
    chorestelor = dmc.Text(f"Cholesterol Level: {df_userdemo['cholesterol'][0]}")
    patient_info = dmc.Text(
        f"Patient is a {df_userdemo['age'][0]} old {df_userdemo['sex'][0].lower()}, weights {df_userdemo['weight'][0]} lbs, and is a {df_userdemo['Smoker'][0].lower()}"
    )

    notification = f"User data loaded. \n\n3 queries executed with number of rows retrieved: {len(df_userdemo) + len(df_userfit) + len(df_usercomp)}"
    return (
        [patient_info, chorestelor],
        [user_comparison, blood_pressure],
        header,
        fig_user,
        comp.notification_user(notification),
    )


@callback(
    Output("demographics", "figure"),
    Output("notifications-scatter", "children"),
    Input("scatter-x", "value"),
    Input("comparison", "value"),
)
def make_scatter(xaxis, comparison):
    df_scatter = dbx_utils.get_scatter_data(xaxis, comparison)
    fig_scatter = figures.generate_scatter(df_scatter, xaxis, comparison)
    notification = f"Scatter data loaded. \n1 query executed with number of rows retrieved: {len(df_scatter)}"
    return fig_scatter, comp.notification_scatter(notification)


@callback(
    Output("fitness-line", "figure"),
    Output("notifications-line", "children"),
    Input("line-y", "value"),
    Input("comparison", "value"),
)
def make_line(yaxis, comparison):
    df_line = dbx_utils.get_line_data(yaxis, comparison)
    fig_line = figures.generate_line(df_line, yaxis, comparison)
    notification = f"Scatter data loaded. \n1 query executed with number of rows retrieved: {len(df_line)}"
    return fig_line, comp.notification_line(notification)


@callback(
    Output("heat-fig", "figure"),
    Output("notifications-heatmap", "children"),
    Input("heat-axes", "value"),
    Input("heat-fitness", "value"),
    Input("comparison", "value"),
    Input("slider-val", "value"),
)
def make_heatmap(axes, fitness, comparison, slider):
    if len(axes) == 2:
        df_heat = dbx_utils.get_heat_data(axes[0], axes[1], fitness, comparison, slider)
        fig_heat = figures.generate_heat(df_heat, axes[0], axes[1], fitness, comparison)
        notification, action = (
            f"Scatter data loaded. \n1 query executed with number of rows retrieved: {len(df_heat)}",
            "show",
        )
    else:
        text = "You must select exactly 2 axes for this plot to display!"
        fig_heat = figures.create_empty(text)
        notification, action = "", "hide"
    return fig_heat, comp.notification_heatmap(notification, action)


if __name__ == "__main__":
    app.run_server(debug=True)

# COMMAND ----------

