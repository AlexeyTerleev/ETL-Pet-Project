from pyspark.sql import SparkSession

S3 = 'http://localhost:4566'

if __name__ == '__main__':

        spark = SparkSession \
            .builder \
            .master('local') \
            .getOrCreate()

        df = spark.read.parquet('s3://test/result.parquet').show()
