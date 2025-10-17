"""

Question 1:
You are given a nested JSON dataset of customer orders from an e-commerce platform. 
Each record contains customer details and an array of orders.

Write a PySpark Code for below:
1) Load the JSON into a PySpark DataFrame.
2) Explode the orders array so each order is a separate row.
3) Flatten the nested structure to get a flat table with the following columns:
customer_id
customer_name
order_id
product
price
quantity
4) Calculate the total revenue per customer (price * quantity) and display the top 3 customers by revenue.
5) Return First Row as dictionary:

"""


from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import * 

def load_df(spark,path):
    
    data_schema=StructType([
        
        StructField('customer_id',StringType()),
        StructField('customer_name',StringType()),
        StructField('order_id',IntegerType()),
        StructField('product',StringType()),
        StructField('price',IntegerType()),
        StructField('quantity',IntegerType()),
        
    ])
    
    
    data_df=spark.read.options(header=True ).schema(data_schema).csv(path)
    
    return data_df 


def explode_df(df):
    
    explode_df=df.withColumn('order_id',explode(split('order_id',"")))   
    return explode_df 



def total_revenue(df):
    
    revenue_df=df.withColumn('revenue',df['quantity']*df['price'])
    
    revenue_df_groupby=revenue_df.groupby('customer_name').agg(sum(revenue_df['revenue']).alias('revenue_per_customer')).limit(3)
    
    return revenue_df_groupby.orderBy(revenue_df_groupby['revenue_per_customer'].desc()).limit(3)

def  dict_row(df):
    
    # row_df=df.first()
    # row_df=row_df.asDict()
    df1=df.collect()[0].asDict()
    #print(df1)
    return df1

if __name__=='__main__':
    
    path ='/user/hadoop/HFS/Input/df_prac.csv'
    
    spark=SparkSession.builder.appName("prac_df_v1").master('local[*]').getOrCreate()
    
    cust_df=load_df(spark,path)
    cust_df.show(truncate=False)
    
    # exp_df=explode_df(cust_df)
    # exp_df.show(truncate=False)
    
    revenue_df=total_revenue(cust_df)
    revenue_df.show(truncate=False)
    
    
    row_df=dict_row(cust_df)
    print(row_df)
    
    
    