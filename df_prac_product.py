from pyspark.sql import SparkSession 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 


def load_warranty_data(spark:SparkSession)-> DataFrame:
    
    data = [
	("CL001", "Laptop", "2024-01-10", "2024-02-15", "Hardware Failure"),
	("CL002", "Mobile", "2024-02-01", "2024-02-05", "Battery Issue"),
	("CL003", "Tablet", "2024-03-05", "2024-04-10", "Screen Damage"),
	("CL004", "Laptop", "2024-03-15", "2024-03-16", "Keyboard Issue"),
	("CL005", "Mobile", "2024-04-01", "2024-05-20", "Software Glitch")
    ]
    
    columns=['claim_id','product_type','claim_date','resolved_date','issue_type']
    data_df=spark.createDataFrame(data,columns)
    
    return data_df

def add_resolution_days(df:DataFrame)->DataFrame:
    
    data_df=df.withColumn('resolution_days',date_diff(to_date(df['resolved_date']),to_date('claim_date')))
    return data_df 

def filter_long_resolutions(df:DataFrame)->DataFrame:
    data_df=df.filter(df['resolution_days']>15)
    return data_df 



# 4. Product type with highest average resolution time
def product_with_highest_avg_resolution(df: DataFrame) -> DataFrame:
    data_df=df.groupBy('product_type').agg(avg(df['resolution_days']).alias('avg_days'))
    data_df=data_df.orderBy(data_df['avg_days'].desc())
    print('printing')
    data_df.show()
    return data_df.limit(1)


# 5. Most frequent issue for delayed claims
def top_issue_in_delays(df: DataFrame) -> DataFrame:
    
    data_df=df.groupBy('issue_type').agg(count(df['issue_type']).alias('issue_count'))
    data_df=data_df.orderBy(data_df['issue_count'].desc()).limit(1)
    print("printing")
    data_df.show()
    
    return data_df 


# 6. Compute delayed claim ratio
def delayed_claim_ratio(df: DataFrame) -> float:
    
    delay_df=df.filter(df['resolution_days']>15).count()
    total_df=df.select(df['claim_id']).count()
    
    return float(delay_df/total_df)
    

# 7. Add claim_month column
def add_claim_month(df: DataFrame) -> DataFrame:
    
    data_df=df.withColumn('Month',month(df['claim_date']))
    return data_df 

# 8. Count of claims per product
def count_claims_per_product(df: DataFrame) -> DataFrame:
    
    data_df=df.groupBy('product_type').agg(count(df['claim_id']).alias('claim_count'))
    print(f'The results as a dictionary')
    
    for i in data_df.collect():
        print(i.asDict())
        
    print(f"Following results are in list format")
    print(data_df.collect())
    
    for i in data_df.collect():
        print(tuple(i))
        
    return data_df 

# 9. Average resolution days by issue type
def avg_resolution_per_issue(df: DataFrame) -> DataFrame:
    
    data_df=df.groupBy(df['issue_type']).agg(avg(df['resolution_days']))
    
    return data_df 

# 10. Flag fast resolutions (<=10 days)

def flag_fast_resolution(df: DataFrame) -> DataFrame:
    data_df=df.withColumn('Flag',when(df['resolution_days']>10,True).otherwise(False))
    
    return data_df 

if __name__=="__main__":
    
    spark=SparkSession.builder.appName('product_df').master('local[*]').getOrCreate()
    load_df=load_warranty_data(spark)
    load_df.show()
    
    resol_df=add_resolution_days(load_df)
    resol_df.show()
    
    filter_resol_df=filter_long_resolutions(resol_df)
    filter_resol_df.show()
    
    avg_resol_df=product_with_highest_avg_resolution(resol_df)
    avg_resol_df.show()
    
    issue_df=top_issue_in_delays(resol_df)
    issue_df.show()
    
    delay_ratio_df=delayed_claim_ratio(filter_resol_df)
    print(f"delayed ratio is :{delay_ratio_df}")
    
    month_df=add_claim_month(resol_df)
    month_df.show()
    
    claim_count_df=count_claims_per_product(month_df)
    claim_count_df.show()
    
    # avg_resol_df=avg_resolution_per_issue(resol_df)
    # avg_resol_df.show()
     
    # flag_df=flag_fast_resolution(resol_df)
    # flag_df.show()
    
    
    
    
    