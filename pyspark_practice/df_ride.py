from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

def load_data(spark, trips_path, drivers_path):
    
    drivers_schema = StructType([
    StructField("driver_id", IntegerType(), nullable=False),
    StructField("driver_name", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True)
    ])
    
    trips_schema = StructType([
    StructField("trip_id", IntegerType(), nullable=False),
    StructField("driver_id", IntegerType(), nullable=False),
    StructField("distance_km", FloatType(), nullable=False),
    StructField("fare_amount", FloatType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False)
    ])
    
    trip_df=spark.read.options(header=True,delimiter=',').schema(trips_schema).csv(trip_path)
    driver_df=spark.read.options(header=True,delimiter=',').schema(drivers_schema).csv(drivers_path)
    
    return (trip_df,driver_df)
    
def clean_trips(trips_df):
    
    trips_df=trips_df.dropna()
    trips_df=trips_df.dropDuplicates()
    
    return trips_df 

def convert_unix_to_date(trips_df):
    
    data_df=trips_df.withColumn('trip_datetime',to_date(from_unixtime(trips_df['timestamp'])))
    return data_df 

def calculate_avg_fare_per_km(trips_df):

    data_df=trips_df.withColumn('fare_per_km',round(trips_df['fare_amount']/trips_df['distance_km'],2))
    
    return data_df 

def join_with_driver(trips_df, drivers_df):
    
    join_df=trips_df.join(drivers_df,trips_df['driver_id']==drivers_df['driver_id'],'left')
    
    return join_df 
    
    
def top_n_earning_drivers(joined_df,n):
    
    data_df=joined_df.groupBy('driver_name').agg(sum(joined_df['fare_amount']).alias('total_earning'))
    data_df=data_df.orderBy(data_df['total_earning'].desc())
    return data_df.limit(n)


    
if __name__=="__main__":
    
    spark=SparkSession.builder.appName('ride_df').master('local[*]').getOrCreate()
    trip_path='data/trips.csv'
    drivers_path='data/drivers.csv'
    
    
    data_df=load_data(spark,trip_path,drivers_path)
    
    trip_df=data_df[0]
    trip_df.show()
    
    
    driver_df=data_df[1]
    driver_df.show()
    
    clean_trip_df=clean_trips(trip_df)
    clean_trip_df.show()
    
    date_df=convert_unix_to_date(trip_df)
    date_df.show()
    
    trip_df.printSchema()
    driver_df.printSchema()
    
    calcu_df=calculate_avg_fare_per_km(trip_df)
    calcu_df.show()
    
    join_df=join_with_driver(trip_df, driver_df)
    join_df.show()
    
    top_df=top_n_earning_drivers(join_df,3)
    top_df.show()
        