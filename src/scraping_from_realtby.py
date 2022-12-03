import os
import sys
import requests
import re
from bs4 import BeautifulSoup
import pyspark.pandas as pd
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql import SparkSession, functions


def extract_item(item_url: str):

    cols = ['id', 'Цена', 'Населенный пункт', 'Адрес', 'Комнат всего/разд', 'Этаж / этажность',
            'Площадь общая/жилая/кухня', 'Планировка', 'Высота потолков', 'Год постройки', 'Ремонт']

    item_page = requests.get(item_url)

    tables = pd.read_html(item_page.content)
    df = tables[1].to_spark().union(tables[2].to_spark())

    df = df.groupBy().pivot("0").agg(functions.first("1"))

    item_soup = BeautifulSoup(item_page.content, 'html.parser')
    try:
        item_cost = item_soup.find('div', class_='d-flex align-items-center fs-giant').find(
            'strong').text.strip()
    except AttributeError:
        item_cost = None
    id = re.findall(r"https://realt.by/sale/flats/object/(\d+)/", item_url)[0]

    df = df.withColumn("id", functions.lit(id))
    df = df.withColumn("Цена", functions.lit(item_cost))

    df = df.withColumnRenamed('Комнат всего/разд.', 'Комнат всего/разд')
    df = df.select([c for c in df.columns if c in cols])
    for c in cols:
        if c not in df.columns:
            df = df.withColumn(c, functions.lit(None))
    return df


def extract(spark: SparkSession):

    schema = StructType([
        StructField('id', StringType(), False),
        StructField('Цена', StringType(), True),
        StructField('Населенный пункт', StringType(), True),
        StructField('Адрес', StringType(), True),
        StructField('Комнат всего/разд', StringType(), True),
        StructField('Этаж / этажность', StringType(), True),
        StructField('Площадь общая/жилая/кухня', StringType(), True),
        StructField('Планировка', StringType(), True),
        StructField('Высота потолков', StringType(), True),
        StructField('Год постройки', StringType(), True),
        StructField('Ремонт', StringType(), True)
    ])
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    url = 'https://realt.by/sale/flats/minskij-rajon/'

    while url:
        page = requests.get(url)
        url = ''
        soup = BeautifulSoup(page.content, 'html.parser')
        res = soup.find_all('a', class_='teaser-title')

        for el in res:
            if el.get('title') is not None and re.fullmatch(r"https://realt.by/sale/flats/object/\d+/", el.get('href')):
                df = df.unionByName(extract_item(el.get('href')))

        next_pages_links = soup.find('div', class_='paging-list')
        curr = int(next_pages_links.find('a', class_='active').text)

        if curr == 1: break

        for el in next_pages_links.find_all('a'):
            try:
                if int(el.text) == curr + 1:
                    print(f'page number {curr} is completed')
                    url = el.get('href')
                    break
            except ValueError:
                pass
    return df


def upload_df(df, output_path) -> None:
    df.write.parquet(output_path)


def run(output_s3_path: str, spark: SparkSession) -> None:

    input_df = extract(spark)
    input_df.show()
    upload_df(input_df, output_s3_path)


if __name__ == '__main__':

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('realtby_scraping') \
        .getOrCreate()

    run(sys.argv[1], spark)

#python3 src/scraping_from_realtby.py s3://test/resutl.parquet
