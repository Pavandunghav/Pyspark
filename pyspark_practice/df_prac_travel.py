from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

def define_booking_schema() -> StructType:
    
    data_schema=StructType([
        StructField("booking_id",StringType()),
        StructField("customer_id",StringType()),
        StructField("destination",StringType()),
        StructField("travel_date",DateType()),
        StructField("booking_amount",FloatType()),
        StructField("status",StringType())]
    )

    return data_schema

def load_travel_bookings(spark: SparkSession, path: str, dt_schema: StructType) -> DataFrame:
    
    data_df=spark.read.options(header=True,delimeter=',').schema(dt_schema).csv(path)
    #data_df=spark.read.options(delimeter=',').csv(path)
    return data_df 

def top_n_destinations_by_revenue(df: DataFrame, n: int) -> DataFrame:
    
    data_df=df.groupBy('destination').agg(sum(df['booking_amount']).alias('revenue'))
    data_df=data_df.orderBy(data_df['revenue'].desc())
    return data_df.limit(n)

def list_active_destinations(df: DataFrame) -> List[str]:
    
    data_df=df.filter(df['status']=='active')
    
    data_list=[row['destination'] for row in data_df.collect()]
    data_list=list(set(data_list))
    
    return data_list

if __name__ == '__main__':
    
    path="/home/user/LFS/pyspark_practice/data/booking.csv"
    spark=SparkSession.builder.appName('travel df').master('local[*]').getOrCreate()
    
    schema=define_booking_schema()
    
    data_df=load_travel_bookings(spark,path,schema)
    data_df.show()
    
    top_df=top_n_destinations_by_revenue(data_df,3)
    top_df.show()
    
    data_list=list_active_destinations(data_df)
    print(data_list)
    
    
    