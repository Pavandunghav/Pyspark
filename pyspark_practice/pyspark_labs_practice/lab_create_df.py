from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

spark=SparkSession.builder.appName("Create_df").master('local[*]').getOrCreate()

data = [
   (101, "Alice", "HR", 5000),
   (102, "Bob", "IT", 6000),
   (103, "Charlie", "IT", 5500),
   (104, "David", "Finance", 6500)
]

columns=['id','name','department','salary']
data_df=spark.createDataFrame(data,schema=columns)
data_df.show()
data_df.limit(3).show()
data_df.select(['salary']).distinct().show()

#data_df.printSchema()


path='hdfs://localhost:9000/user/hadoop/HFS/Input/car_data.csv'
data_csv_df=spark.read.csv(path,header=True,inferSchema=True)
data_csv_df.limit(3).show()



