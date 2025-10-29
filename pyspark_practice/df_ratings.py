from pyspark.sql import SparkSession 
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('df_ratings').master('local[*]').getOrCreate()

ratings_df=spark.read.options(header=True,delimiter=',',inferSchema=True,)\
                 .csv('hdfs://localhost:9000/user/hadoop/HFS/Input/ratings.csv')
                 
# ratings_df.show(5,truncate=False)

# rate_count_df=ratings_df.groupBy('rating').agg(count('rating').alias('TotalRating')).orderBy(ratings_df['rating'].desc())

# rate_count_df.show(5,truncate=False)






##Question 2 :Sum of ratings and no. of ratings for each movie

sum_count_rate_df=ratings_df.groupBy('movieID')\
                            .agg(count('rating').alias('count_Rating'),sum('rating').alias('sum_rating'))\
                            .orderBy(ratings_df['movieID'].desc())

#val_sum_count_rate_df=sum_count_rate_df.filter(sum_count_rate_df[''])


# sum_count_rate_df.show(5,truncate=False)

