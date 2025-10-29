from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 



def load_data(spark:SparkSession)->DataFrame:
    
    emp_data=data = [
        (1, "Alice", "North", "2025-01-10", 2500.50),
        (2, "Bob", "South", "2025-02-11", 1800.75),
        (3, "Charlie", "East", "2025-03-15", 3400.00),
        (4, "David", "West", "2025-04-10", 1250.00),
        (5, "Eve", "North", "2025-05-12", 2750.25),
        (6, "Frank", "East", "2025-06-18", 4100.00),
        (7, "Grace", "South", "2025-07-22", 3900.75),
        (8, "Hank", "West", "2025-08-05", 1900.00),
        (9, "Ivy", "North", "2025-09-10", 3100.40),
        (10, "Jack", "South", "2025-10-14", 2950.80),
        (11, "Kelly", "East", "2025-01-25", 2300.60),
        (12, "Leo", "West", "2025-02-08", 3200.20),
        (13, "Mona", "North", "2025-03-03", 4150.75),
        (14, "Nina", "South", "2025-04-20", 1700.50),
        (15, "Oscar", "East", "2025-05-17", 2800.40),
        (16, "Paul", "West", "2025-06-09", 3550.30),
        (17, "Quinn", "North", "2025-07-01", 4500.00),
        (18, "Rita", "South", "2025-08-23", 2650.80),
        (19, "Steve", "East", "2025-09-13", 3800.10),
        (20, "Tina", "West", "2025-10-03", 1950.60),
        (21, "Uma", "North", "2025-01-22", 2850.90),
        (22, "Vik", "South", "2025-02-28", 3250.75),
        (23, "Wendy", "East", "2025-03-27", 2900.40),
        (24, "Xavier", "West", "2025-04-15", 2400.10),
        (25, "Yara", "North", "2025-05-05", 3650.00),
        (26, "Zack", "South", "2025-06-10", 1750.80),
        (27, "Aman", "East", "2025-07-19", 4100.50),
        (28, "Bela", "West", "2025-08-21", 3400.00),
        (29, "Cory", "North", "2025-09-11", 2950.20),
        (30, "Dina", "South", "2025-10-16", 3150.75)
    ]
    
    emp_schema=StructType([
        
        StructField('emp_id',IntegerType()),
        StructField("emp_name",StringType()),
        StructField('region',StringType()),
        StructField('joining_date',StringType()),
        StructField('salary',DoubleType()),
       
    ])
    
    data_df=spark.createDataFrame(emp_data,emp_schema)
    return data_df 

def transform_customer_data(df)->DataFrame:
    
    date_df=df.withColumn('joining_date',df['joining_date'].cast(DateType()))
    # date_df.show()
    # date_df.printSchema()
    
    month_df=date_df.withColumn('month',month(date_df['joining_date']))
    # month_df.show()
    
    year_df=month_df.withColumn('year',year(month_df['joining_date']))
    # year_df.show()
    
    
    quarter_df=year_df.withColumn('qaurter',quarter(year_df['joining_date']))
    # quarter_df.show()
    
    return quarter_df 

def transform_salary(df)->DataFrame:
    
    bonus_df=df.withColumn('salary',when(df['region']=='North',df['salary']*1.1).otherwise(df['salary']))
    
    #Add Discount flag for transactions > 3500
    discount_df=bonus_df.withColumn('discount',when(bonus_df['salary']>3500,True).otherwise(False))
    
    
    #Round Amount to 2 decimals
    round_df=discount_df.withColumn('salary',round(discount_df['salary'],2))
    
    
    #Normalize Region column (title case)
    normal_df=round_df.withColumn('region',initcap(round_df['region']))
    
    
    #Add a new column Category (based on amount range)  
    new_df=normal_df.withColumn('salary_category',when(normal_df['salary']<1500,'low').when((normal_df['salary']> 1500) & (normal_df['salary']<3000),'medium').when(normal_df['salary']>3000,'high'))
    print(list(new_df.collect()[0]))
    print(new_df.collect()[0].asDict())
    
    return new_df 

def write_in_hadoop(df)->DataFrame:
    
    df.write.mode('overwrite').partitionBy('region').csv('hdfs://localhost:9000/user/hadoop/HFS/Output/emp_sal')
    
    
    
    
    
    
    
if __name__=='__main__':
    
    
    spark=SparkSession.builder.appName('emp_message').master('local[*]').getOrCreate()
    data_df=load_data(spark)
    data_df.show(truncate=False)
    
    date_df=transform_customer_data(data_df)
    
    salary_df=transform_salary(date_df)
    salary_df.show()
    
    #write_in_hadoop(salary_df)
    
    