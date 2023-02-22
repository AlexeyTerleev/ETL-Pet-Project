import sys

sys.path.append('/opt/airflow/src')

import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

import common.geo_info

import json

import common.utilities

spark = SparkSession.builder.config("spark.jars", "/usr/local/postgresql-42.5.1.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()

schema = StructType([
        StructField('id', IntegerType()),
        StructField('last_price', IntegerType()),
        StructField('ceiling_height', FloatType()),
        StructField('year_of_construction', IntegerType()),
        StructField('number_of_rooms', IntegerType()),
        StructField('floor', IntegerType()),
        StructField('living_area', FloatType()),
        StructField('total_area', FloatType()),
        StructField('kitchen_area', FloatType()),
        StructField('repair', StringType()),
        StructField('district', StringType()),
        StructField('neighborhood', StringType()),
        StructField('street', StringType()),
        StructField('house_num', StringType()),

        StructField('lat', FloatType()),
        StructField('lon', FloatType()),
        StructField('distance_to_center', FloatType())
    ])


def json2df():

    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    with open('../../data/realtby.json', 'r') as json_file:
        data = json.load(json_file)

        # for usage
        for line in list(data):

            if line['last_price'] == 'None':
                continue

            tmp_dct = {
                'id': int(line['id']),
                'last_price': int(''.join(re.findall(r'(\d+)', line['last_price']))),
                'ceiling_height': float(re.findall(r'([\d\.]+)', line.get('Высота потолков', '0'))[0]),
                'year_of_construction': int(line.get('Год постройки', '0')),
                'number_of_rooms': int(line['Количество комнат']),
                'floor': int(re.findall(r'(\d+)', line['Этаж / этажность'])[0]),
                'total_area': float(re.findall(r'([\d\.]+)', line['Площадь общая'])[0]),
                'living_area': float(re.findall(r'([\d\.]+)', line['Площадь жилая'])[0]),
                'kitchen_area': float(re.findall(r'([\d\.]+)', line.get('Площадь кухни', '0'))[0]),
                'repair': line.get('Ремонт', None),
                'district': line.get('Район города', None),
                'neighborhood': line.get('Микрорайон', None),
                'street': line.get('Улица', None),
                'house_num': line.get('Номер дома', None)
            }
            if not tmp_dct['ceiling_height']:
                tmp_dct['ceiling_height'] = None
            if not tmp_dct['kitchen_area']:
                tmp_dct['kitchen_area'] = None
            if not tmp_dct['year_of_construction']:
                tmp_dct['year_of_construction'] = None

            coordinates = geo_info.Client.coordinates('Минск, ' + ', '.join(
                [tmp_dct[x] for x in ('district', 'street', 'neighborhood', 'house_num') if tmp_dct[x] is not None]))

            tmp_dct['lat'] = coordinates[0]
            tmp_dct['lon'] = coordinates[1]
            tmp_dct['distance_to_center'] = geo_info.Client.distance_to_center(coordinates)

            tmp_df = spark.createDataFrame(data=[tuple(tmp_dct.values())], schema=schema)
            df = df.union(tmp_df)

    return df

def merge_to_exist_df(df_new):
    df_main = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pet_project_db") \
        .option("dbtable", "realtby_data_table") \
        .option("user", "postgres") \
        .option("password", "changeme") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df_main.show()
    df_new.show()

    df_main.createOrReplaceTempView("main")
    df_new.createOrReplaceTempView("new")

    query = "with exist as (\
	            select * from new\
	            where id in (select id from main)\
            ), n as (\
                select * from new\
                where id not in (select id from main)\
            ), changed as (select exist.* from exist\
                left join main\
                on main.id = exist.id\
            ), o as (\
                select * from main\
                where id not in (select id from new)\
            )\
            select * from changed union select * from o union select * from n"

    df = spark.sql(query).toDF('id', 'last_price', 'ceiling_height', 'year_of_construction', 'number_of_rooms', 'floor',
                               'living_area', 'total_area', 'kitchen_area', 'repair', 'district', 'neighborhood',
                               'street', 'house_num', 'lat', 'lon', 'distance_to_center')

    return df

def upload_to_db():

    utilities.delete_table()

    json2df().select("*").write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pet_project_db") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "realtby_data_table") \
        .option("user", "postgres").option("password", "changeme").save()

def update_db():
    df = merge_to_exist_df(json2df())

    utilities.delete_table()

    df.select("*").write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pet_project_db") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "realtby_data_table") \
        .option("user", "postgres").option("password", "changeme").save()

