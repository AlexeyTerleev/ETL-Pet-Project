import plotly.express as px
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import pandas as pd

spark = SparkSession.builder.config("spark.jars", "/usr/local/postgresql-42.5.1.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()


def price_distance(dataframe):
    fig = px.line(dataframe.toPandas().sort_values(by='distance_to_center', ascending=False), x="distance_to_center",
                  y="last_price", title="graph")
    fig.show()


def num_place(dataframe):
    data = spark.sql("select district, count(*) as num from test group by district")
    fig = px.bar(data.toPandas(), x='district', y='num')
    fig.show()


if __name__ == '__main__':

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pet_project_db") \
        .option("dbtable", "realtby_data_table") \
        .option("user", "postgres") \
        .option("password", "changeme") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df.createOrReplaceTempView("test")

    num_place(df)

