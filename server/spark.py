from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_utc_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType, MapType
from datetime import datetime
import pytz
import os

spark = SparkSession \
    .builder \
    .appName("Reading CSV") \
    .master("local[3]") \
    .getOrCreate()

# file_path = ['../data/AAPL.csv',
#              '../data/GOOG.csv',
#              '../data/IBM.csv',
#              '../data/META.csv',
#              '../data/NFLX.csv']


df = spark.read.options(header=True).csv('../data')
# df = spark.read.options(header=True).csv(file_path)
df.show()
# Query No - 2
df.createOrReplaceTempView('data')
most_traded_stock_each_day = spark.sql(
    "select company,max(Volume) from data group by company")
most_traded_stock_each_day.show()

