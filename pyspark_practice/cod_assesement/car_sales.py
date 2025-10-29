
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


def define_sales_schema():
    
    sales_schema=StructType([
        StructField('sale_id',IntegerType()),
        StructField('brand',StringType()),
        StructField('model',StringType()),
        StructField('price',DoubleType()),
        StructField('is_test_drive',BooleanType())
    ])
    
    return sales_schema 

def load_sales_data(spark,path):
    
    sale_schema=define_sales_schema()
    
    sales_df=spark.read.options(header=True).schema(sale_schema).csv(path)
    
    return sales_df

def filter_succesful_data(df):
    
    filter_df=df.filter(df['is_test_drive']==True)
    return filter_df 

def compute_brand_performance(df):
    
    compute_df=df.groupBy('brand').agg(count(df['sale_id']).alias('total_cars_sold'),avg(df['price'].alias('average_price')))
    
    return compute_df 

if __name__ =="__main__":
    
    
    spark=SparkSession.builder.appName('cust_loyalty').master('local[*]').getOrCreate()
    
    path='hdfs://localhost:9000/user/hadoop/HFS/Input/car_data.csv'
    
    data_df=load_sales_data(spark,path)
    data_df.show(truncate=False)
    
    filter_df=filter_succesful_data(data_df)
    filter_df.show(truncate=False)
    
    compute_df=compute_brand_performance(data_df)
    compute_df.show(truncate=False)
    
    
    
    
