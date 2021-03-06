import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("Question3")\
    .master("local[2]")\
    .getOrCreate()


file1=spark.read.csv("/home/sunbeam/Downloads/nonConfidential.csv",inferSchema=True,header=True)


file2=spark.read.parquet("/home/sunbeam/Downloads/confidential.snappy.parquet",inferSchema=True,header=True)

file3=file1.unionAll(file2)


#QUestion No 3

print("Answer of third Question: ")
from pyspark.sql.types import IntegerType

df4=file3.withColumn('GrossSqFoot',file3['GrossSqFoot'].cast(IntegerType()))

df4.createOrReplaceTempView('df4_table')

spark.sql("select sum(GrossSqFoot) from df4_table where State=='VA' and IsCertified=='Yes' ").show()












