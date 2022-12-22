import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

import geo_info

import json

spark = SparkSession.builder.config("spark.jars", "/usr/local/postgresql-42.5.1.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()

'''df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/pet_project_db") \
    .option("dbtable", "test_table") \
    .option("user", "postgres") \
    .option("password", "changeme") \
    .option("driver", "org.postgresql.Driver") \
    .load()
df.printSchema()'''


def json2df():

    schema = StructType([
        StructField('id', IntegerType()),
        StructField('fingerprint', StringType()),
        StructField('last_price', IntegerType()),
        StructField('ceiling_height', FloatType()),
        StructField('year_of_construction', IntegerType()),
        StructField('number_of_rooms', IntegerType()),
        StructField('floor', IntegerType()),
        StructField('living_area', FloatType()),
        StructField('total_area', FloatType()),
        StructField('kitchen_area', FloatType()),
        StructField('repair', StringType()),
        StructField('address', StringType()),
        StructField('distance_to_center', FloatType())
    ])

    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    with open('../../data/realtby.json', 'r') as json_file:
        data = json.load(json_file)

        # for usage
        #for line in list(data):

        # --- for test ---
        for i in range(20):
            line = list(data)[i]
        # ----------------
            if line['last_price'] == 'None':
                continue

            tmp_dct = {
                'id': int(line['id']),
                'fingerprint': str(line['fingerprint']),
                'last_price': int(''.join(re.findall(r'(\d+)', line['last_price']))),
                'ceiling_height': float(re.findall(r'([\d\.]+)', line.get('Высота потолков', '0'))[0]),
                'year_of_construction': int(line.get('Год постройки', '0')),
                'number_of_rooms': int(line['Количество комнат']),
                'floor': int(re.findall(r'(\d+)', line['Этаж / этажность'])[0]),
                'total_area': float(re.findall(r'([\d\.]+)', line['Площадь общая'])[0]),
                'living_area': float(re.findall(r'([\d\.]+)', line['Площадь жилая'])[0]),
                'kitchen_area': float(re.findall(r'([\d\.]+)', line.get('Площадь кухни', '0'))[0]),
                'repair': line.get('Ремонт', None),
                'address': ' '.join([line.get(x, '') for x in ['Район города', 'Улица', 'Номер дома']])

            }
            if not tmp_dct['ceiling_height']:
                tmp_dct['ceiling_height'] = None
            if not tmp_dct['kitchen_area']:
                tmp_dct['kitchen_area'] = None
            if not tmp_dct['year_of_construction']:
                tmp_dct['year_of_construction'] = None

            tmp_dct['distance_to_center'] = geo_info.Client.distance_to_center('Минск ' + tmp_dct['address'])

            tmp_df = spark.createDataFrame(data=[tuple(tmp_dct.values())], schema=schema)
            df = df.union(tmp_df)

    return df


if __name__ == '__main__':

    json2df().select("*").write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pet_project_db") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "realtby_data_table") \
        .option("user", "postgres").option("password", "changeme").save()

