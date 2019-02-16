import pyverdict
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os.path import expanduser, join, abspath

#disable logs
sc = SparkContext.getOrCreate()
sc.setLogLevel("off")

#prepare spark session
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

#load data
spark.sql("DROP TABLE IF EXISTS example")
spark.sql("CREATE TABLE IF NOT EXISTS example (key INT, value STRING) USING hive")
spark.sql("LOAD DATA LOCAL INPATH '/pyverdict_on_spark/kv1.txt' INTO TABLE example")

#exectue queries
verdict = pyverdict.spark(spark)

query = "SELECT count(*) FROM default.example"
res = verdict.sql_raw_result(query)
print(res.to_df())

query = "SELECT * FROM default.example WHERE value LIKE '%00' "
res = verdict.sql_raw_result(query)
print(res.to_df())

query = "INSERT INTO default.example VALUES (999, 'val_999') "
spark.sql(query)
query = "SELECT * FROM default.example WHERE value LIKE '%999' "
res = verdict.sql_raw_result(query)
print(res.to_df())

spark.sql("DROP TABLE IF EXISTS default.example")