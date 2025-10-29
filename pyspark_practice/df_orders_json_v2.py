from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 



print('Hello')

if __name__ == "__main__":
    
    spark=SparkSession.builder.appName('orders_data_df').master('[*]').getOrCreate()
    path=''
    data_df=load_data(spark,path)