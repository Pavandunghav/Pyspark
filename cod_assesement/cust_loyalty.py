'''
customer_id, transaction_date, amount_spent, product_category
CUST1001, 2023-11-05, 250, Electronics
CUST1002, 2023-11-06, 75, Grocery

'''

from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

def load_transaction_data(spark,path):
    cust_schema=StructType([
        StructField(),
        StructField(),
        StructField(),
        
    ])
    data_df=spark.read.options(header=True).schema(cust_schema).csv(path)
    return data_df 

def create_dummy_customers(spark):
    pass 

if  __name__=='__main__':
    
    path="hdfs/localhost:9000/user/hadoop/HFS/Input/cust_loyalty_v2.csv"
    
    

