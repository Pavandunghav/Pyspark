from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

def load_data(spark):
        
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

def add_resolution_days(df: DataFrame) -> DataFrame:
    
    data_df=df.withColumn('resolution_days',date_diff(to_date(df['resolved_date']),to_date(df['claim_date'])))
    return data_df

def filter_long_resolution(df: DataFrame) -> DataFrame:
    
    filter_df=df.filter(df['resolution_days']>15)
    return filter_df 

def product_with_highest_avg_resolution(df):
   
    avg_df = df.groupBy("product_type") .agg(avg(df["resolution_days"]).alias("avg_resolution_days"))
    
    data_df = avg_df.orderBy(avg_df["avg_resolution_days"].desc()).limit(1)
    
    return data_df 

def top_issue_in_delays(df):
    
    delayed_claims = df.filter(col("resolution_days") > 15)
    top_issue = delayed_claims.groupBy("issue_type").count() \
                              .orderBy(col("count").desc()) \
                              .limit(1)
                              
    return top_issue


def delayed_claim_ratio(df):
  
    total_claims = df.count()
    delayed_claims = df.filter(col("resolution_days") > 15).count()
    # print(delayed_claims)
    
    if total_claims == 0:
        return 0.0
    
    ratio = delayed_claims / total_claims
    return ratio

def add_claim_month(df):
   
    df = df.withColumn("claim_month",month("claim_date"))
    return df

def count_claims_per_product(df):
   
    claim_counts = df.groupBy("product_type") \
                     .count() \
                     .alias("claims_count")
    return claim_counts

def avg_resolution_per_issue(df):
  
    avg_resolution = df.groupBy("issue_type") \
                       .agg(avg("resolution_days").alias("avg_resolution_days"))
                       
    return avg_resolution

def flag_fast_resolution(df):
   
    df = df.withColumn("fast_resolution_flag", 
                       when(col("resolution_days") <= 10, True).otherwise(False))
    return df


if __name__=="__main__":
    
    spark=SparkSession.builder.appName('warranty_df').master('local[*]').getOrCreate()
    
    data_df=load_data(spark)
    data_df.show()
    
    days_df=add_resolution_days(data_df)
    days_df.show()
    
    filter_df=filter_long_resolution(days_df)
    filter_df.show()
    
    high_df=product_with_highest_avg_resolution(days_df)
    high_df.show()
    
    top_issue_df=top_issue_in_delays(days_df)
    top_issue_df.show()
    
    claim_ratio=delayed_claim_ratio(days_df)
    print(claim_ratio)
    
    month_df=add_claim_month(days_df)
    month_df.show()
    
    claim_count_df=count_claims_per_product(days_df)
    claim_count_df.show()
    
    avg_issue_df=avg_resolution_per_issue(days_df)
    avg_issue_df.show()
    
    flag_df=flag_fast_resolution(days_df)
    flag_df.show()
    