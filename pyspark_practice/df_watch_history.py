from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
 
def create_watch_history_df(spark,path):
    watch_history_df=spark.read.format('csv')\
        .option('header','true')\
        .option('delimiter','\t')\
        .option('inferSchema','true')\
        .load(path)
    return watch_history_df

def create_user_df(spark,path):
    user_df=spark.read.format('csv')\
        .option('header','true')\
        .option('delimiter','\t')\
        .option('inferSchema','true')\
        .load(path)
    return user_df

def join_user_watch_df(watch_df, user_df):
    joined_df= watch_df.join(user_df, on='user_id', how='inner')
    return joined_df

def compute_avg_watch_duration(watch_df):
    computed_df=watch_df.groupBy('user_id').agg(mean('watch_duration').alias('Avg_watch_duration'))
    return computed_df

def categorize_watchers(watched_df):
    categorize_df=watched_df.withColumn('watch_category',\
        when(col('watch_duration')<=30, 'Light')\
        .when((col('watch_duration')>30) & (col('watch_duration')<60),'Moderate')\
        .otherwise('Heavy'))
    return categorize_df

def filter_frequent_users(watched_df, min_watches=2):
    watched_df=watched_df.groupBy('user_id').count().alias('counts')
    filtered_df=watched_df.filter(col('count')>=min_watches)
    return filtered_df

def most_watched_genre(watch_df):
    watch_df=watch_df.groupBy('genre').count()
    watch_df=watch_df.orderBy(col('count').desc(),col('genre').asc())
    # return watch_df
    return watch_df.first().asDict()['genre']

if __name__ == '__main__':
    spark=SparkSession.builder.appName('user').master('local[*]').getOrCreate()
    watch_path='/home/user/LFS/pyspark_practice/data/watch_history.csv'
    user_path='/home/user/LFS/pyspark_practice/data/users.csv'
    watch_history_data=create_watch_history_df(spark,watch_path)
    # watch_history_data.show()
    user_data=create_user_df(spark,user_path)
    # user_data.show()
    joined_data=join_user_watch_df(watch_history_data, user_data)
    # joined_data.show()
    computed_data=compute_avg_watch_duration(watch_history_data)
    # computed_data.show()
    categorize_data=categorize_watchers(watch_history_data)
    # categorize_data.show()
    frequent_user_date=filter_frequent_users(watch_history_data, min_watches=2)
    # frequent_user_date.show()
    most_watched_genre_data=most_watched_genre(watch_history_data)
    # most_watched_genre_data.show()
    print(most_watched_genre_data)