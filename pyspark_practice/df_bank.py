
from pyspark.sql import SparkSession 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

def load_data(spark,path):
    
    data_df=spark.read.options(header=True).csv(path)
    return data_df 

def find_high_value_customers(df: DataFrame, n: int) -> DataFrame:
    
    data_df=df.groupBy('CustomerID').agg(count(df['TransactionID']).alias('trans_count'))
    data_df=data_df.filter(data_df['trans_count']>=n)
    data_df=data_df.select('CustomerID')
    
    return data_df 

def apply_service_charges(df: DataFrame, high_value_customers: DataFrame) -> DataFrame:
    
    high_list = [row['CustomerID'] for row in high_value_customers.collect()]
    #print(high_list)
    data_df=df.withColumn('service_charge',when(df['CustomerID'].isin(high_list) ,0.005).otherwise(0.01))
    data_df=data_df.select(['CustomerId','Amount','TransactionType','Date','service_charge'])
    
    return data_df 

def top_three_deposits(df: DataFrame) -> DataFrame:
    
    df=df.withColumn('Amount',df['Amount'].cast(IntegerType()))
    data_df=df.filter(df['TransactionType'] == 'Deposit')
    data_df=data_df.orderBy(data_df['Amount'].desc()).limit(3)
    data_list=[row for row in data_df.collect()]
    print(data_list)
    print("The dictionary ")
    data_dict=[row.asDict() for row in data_df.collect()]
    print(data_dict)
    
          
    return data_df 

if __name__=='__main__':
    
    spark=SparkSession.builder.appName('bank data').master('local[*]').getOrCreate()
    path='/home/user/LFS/pyspark_practice/data/bank_data.csv'
    
    data_df=load_data(spark,path)
    data_df.show()
    
    high_df=find_high_value_customers(data_df,4)
    high_df.show()
    
    service_df=apply_service_charges(data_df, high_df) 
    service_df.show()
    
    top_df=top_three_deposits(data_df)
    top_df.show()
    
    # data_df.printSchema()
    
    