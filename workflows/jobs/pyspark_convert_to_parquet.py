import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.functions import md5, concat, col, coalesce, lit, regexp_replace
import logging

import os 
import sys

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import *
from common.schema import *

logger = logging.getLogger(__name__)

logger.info('go into this file')

spark = SparkSession.builder \
                .master('local') \
                .appName('convert_csv_to_parquet') \
                .getOrCreate()

trsction = spark.read \
                .option("header", "true") \
                .csv(f'{trsction_local_ings}', schema = trnsction_schema)


trsction = trsction.fillna({'errors': 'unknown',
                            'merchant_state': 'unknown',
                            'merchant_city': 'unknown'})

trsction = trsction.withColumn("amount", regexp_replace("amount", "\\$", ""))

trsction.write.parquet(f'{trsction_local_trgt}', mode='overwrite') 
logger.info(f'It is stored into {trsction_local_trgt}')
