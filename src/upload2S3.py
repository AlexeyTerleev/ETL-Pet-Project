import os
import sys
from pyspark.sql import SparkSession
from scraping_from_realtby import run

S3 = 'http://localhost:4566'

if __name__ == '__main__':

        spark = SparkSession \
            .builder \
            .master('local') \
            .getOrCreate()

        # Setup aws buckets
        os.system('aws --endpoint-url={} s3 mb s3://{}'.format(S3, sys.argv[1]))

        run('s3://{}/realtby.parquet'.format(sys.argv[1]), spark)

