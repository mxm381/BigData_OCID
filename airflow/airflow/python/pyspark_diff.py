#pyspark_diff
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='Job to partition diff_data and parse into mysql')
    parser.add_argument('--year', required=True, type=str)
    parser.add_argument('--month', required=True, type=str)
    parser.add_argument('--day', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', required=True, type=str)
    parser.add_argument('--hdfs_target_format', required=True, type=str)
    
    return parser.parse_args()
# Parse Command Line Args
args = get_args()
# Initialize Spark Context
sc = pyspark.SparkContext()
spark = SparkSession(sc)
print("Hallo")
database_schema = StructType(
        [
            StructField("radio", StringType(), True),
            StructField("mcc", IntegerType(), True),
            StructField("net", IntegerType(), True),
            StructField("area", IntegerType(), True),
            StructField("cell", IntegerType(), True),
            StructField("unit", IntegerType(), True),
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("range", IntegerType(), True),
            StructField("samples", IntegerType(), True),
            StructField("changeable", IntegerType(), True),
            StructField("created", IntegerType(), True),
            StructField("updated", IntegerType(), True),
            StructField("averageSignal", IntegerType(), True)
        ])
data_diff = spark.read.format("csv")\
.options(header="true", delimiter=",", nullValue="null", inferSchema="false"
).schema(database_schema).load(args.hdfs_source_dir + "/cell_towers_diff.csv")
print("herrlich")
#select relevant cols
data_diff = data_diff.select("radio", "lat", "lon", "range")
data_diff.repartition('radio').write.format("parquet").mode("append").option(
        "path", args.hdfs_target_dir).partitionBy("radio").saveAsTable("default")
data_diff.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/towers',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='towers', #take same as for full towers to append diffs to full_towers
        user="root",
        password="bigD").mode('append').save()