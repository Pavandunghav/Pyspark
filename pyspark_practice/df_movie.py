
## question 3 :    From the movies file display 1st & 2nd column & split 2nd column value by ('|') and 
# repeat on every row.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , IntegerType , StringType ,StructField
from pyspark.sql.functions import * 


spark=SparkSession.builder.appName('df_movie').master('local[*]').getOrCreate()


movie_schema=StructType([
             StructField('movieID',IntegerType()),
             StructField('title',StringType()),
             StructField('genres',StringType()),
])

movie_df=spark.read.options(header=True)\
              .schema(movie_schema)\
              .csv('hdfs://localhost:9000/user/hadoop/HFS/Input/movies.csv')

# movie_df.show(truncate=False)


filter_df=movie_df.withColumn('genres',explode(split('genres','\|')))
                              
#                             

# filter_df=filter_df.drop('title')
filter_df=filter_df.select(filter_df['movieID'],filter_df['genres'])


filter_df.show(truncate=False)



