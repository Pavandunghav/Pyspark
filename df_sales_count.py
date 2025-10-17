
#Find the employee count & cost to company for each group consisting of dept, cadre, and state?


##Libraries 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *


#Object Creation 

spark=SparkSession.builder.appName('df_sales_count').master('local[*]').getOrCreate()


sale_df=spark.read.options(header=True)\
             .csv('hdfs://localhost:9000/user/hadoop/HFS/Input/sales.txt')
             
# sale_df.show(truncate=False)

emp_count_sum_df=sale_df.groupBy(sale_df['dept'],sale_df['cadre'],sale_df['state'])\
                        .agg(count('dept').alias('emp_count'),sum('costToCompany').alias('Cost_To_Company'))
                        
     
emp_count_sum_df=emp_count_sum_df.orderBy(emp_count_sum_df['cost_To_Company'].asc(),emp_count_sum_df['dept'].desc())
     
emp_count_sum_df.show(5,truncate=False)                   
