from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def project_df(spark:SparkSession,path:str)->DataFrame:
    
    data_df=spark.read.options(delimiter='\t',header=True,inferSchema=True).csv(path)
    #date_df=data_df.withColumn('StartDateStr',data_df['StartDateStr'].cast(DateType()))
    date_df=data_df.withColumn('StartDateStr',to_date(data_df['StartDateStr']))
    #date_df_2=date_df.withColumn('date_from_timestamp',date_format(date_df['EndDateStr'],'yy-MM-dd'))
    date_df_2=date_df.withColumn('date_from_timestamp',date_df['EndDateStr'].cast(DateType()))
    day_diff_df=date_df_2.withColumn('DateDifference',date_diff(date_df['EndDateStr'],date_df['StartDateStr']))
    month_diff_df=day_diff_df.withColumn('Months_Difference',months_between(day_diff_df['EndDateStr'],day_diff_df['StartDateStr']))
    curr_date_df=month_diff_df.withColumn('Current_Date',current_date())
    
    day_diff_df_2=curr_date_df.withColumn('Current day Difference',\
                                         date_diff(curr_date_df['Current_Date'],curr_date_df['EndDateStr']))
    
    return day_diff_df_2
    

if __name__== '__main__' :
    
    spark=SparkSession.builder.appName("emp_project_df").master('local[*]').getOrCreate()
    path='hdfs://localhost:9000/user/hadoop/HFS/Input/emp_proj.csv'
       
    
    data_df=project_df(spark,path)
    data_df.show()
    data_df.printSchema()
    
    
    
    
    

