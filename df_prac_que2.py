
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import * 

def load_account_data(spark,path)->DataFrame:
    
    data_df=spark.read.options(header=True,inferSchema=True ).csv(path)
    return data_df 

def clean_invalid_accounts(df):
    
    data_df=df.filter((df['customer_id'].isNotNull()) & (df['balance'].isNotNull()) & (df['balance']!=0))
    return data_df
    
def compute_balance_stats(df):
    
    data_df=df.filter(df['balance'].isNotNull())
    data_df=df.agg(min(df['balance']).alias('Minimum_value'),max(df['balance']).alias('maximum_value'),avg(df['balance']).alias('average_value'))
    
    
    data_dict=data_df.collect()[0].asDict()
    print(data_dict)
    
    return data_df



def count_valid_accounts(df):
    data_df=df.filter((df['balance'].isNotNull()) & (df['balance']>=0))
    data_df=data_df.agg(count(df['balance']).alias('Balance_count'))
    
    data_dict=data_df.collect()[0].asDict()
    print(data_dict['Balance_count'])
    return data_df 

def most_common_account_type(df):
    
    data_df=df.filter(df['balance'].isNotNull()) 
    data_df=data_df.groupBy('account_type').agg(count(df['account_type']).alias('count_type'))
    data_df=data_df.orderBy(data_df['count_type'].desc(),data_df['account_type'].asc()).limit(1)
    
    print(data_df.collect()[0]['account_type'])
    return data_df 
    

if __name__=='__main__':
    
    #user/hadoop/HFS/Input/accounts.csv
    path='hdfs://localhost:9000/user/hadoop/HFS/Input/accounts2.csv'
    spark=SparkSession.builder.appName('bank_df').master('local[*]').getOrCreate()
    
    data_df=load_account_data(spark,path)
    data_df.show(truncate=False)
    
    filter_df=clean_invalid_accounts(data_df)
    filter_df.show(truncate=False)
    
    comp_df=compute_balance_stats(data_df)
    comp_df.show(truncate=False)
    # print(comp_df)
    
    count_df=count_valid_accounts(data_df)
    count_df.show(truncate=False)
    
    type_df=most_common_account_type(data_df)
    type_df.show(truncate=False)
    
    
    
    