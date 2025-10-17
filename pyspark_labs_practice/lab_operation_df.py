from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

spark=SparkSession.builder.appName("Create_df").master('local[*]').getOrCreate()

