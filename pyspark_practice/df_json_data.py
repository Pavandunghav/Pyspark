from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


    
def load_data(spark: SparkSession, path: str):

    df = spark.read.option("multiline", True).json(path)
    user_df = df.select(explode(col("results")).alias("result")).select("result.user.*")
    return user_df

# # 1. Load Data from JSON File
# def load_data(file_path):
    
#     data_df = spark.read.options(multiline=True).json(file_path)
#     user_df=data_df.select(explode(col('results')).alias('result')).select('result.user.*')
    
#     return data_df

# 2. Create Additional Columns
def create_additional_columns(df):

    df = df.withColumn("dob", to_date((df["dob"] / 1000).cast("timestamp")))
    df = df.withColumn("is_gmail_user", when(col("email").endswith("@gmail.com"), True).otherwise(False))
    
    df = df.withColumn("full_name", concat(col("name.first"), col("name.last")))
    
    df = df.withColumn("region_code", substring(col("location.zip").cast("string"), 1, 2))
    
    df = df.withColumn("account_age_years", ((current_date().cast("long") - col("registered")) / (60 * 60 * 24 * 365)).cast(IntegerType()))
    
    df = df.withColumn("dob_day_of_week", date_format(col("dob"), "EEEE"))
    
    return df

# 3. Apply Filters
def apply_filters(df):
    df = df.filter(col("location.state") == "Cambridgeshire")
    
   
    df = df.filter((current_date().cast("long") - df["dob"].cast("long")) / (60 * 60 * 24 * 365) >= 18)
    df = df.filter((current_date().cast("long") - df["dob"].cast("long")) / (60 * 60 * 24 * 365) <= 60)
    
    df = df.filter(col("username").rlike("^[A-Za-z]"))
    
    return df

# 4. Group and Aggregate
def group_and_aggregate(df):
   
    return df.groupBy("location.zip",'gender').count()

# 5. Sort the Final Result
def sort_result(df):
   
    return df.orderBy("location.zip", col("count").desc())

# 6. Mask Phone Number
def mask_phone_number(df):
    
    return df.withColumn("masked_phone", concat(lit("****-****-"), substring(col("phone"), -4, 4)))

if __name__=="__main__":
    
    file_path='/home/user/LFS/pyspark_practice/data/json_data.json'
    
    spark = SparkSession.builder.appName("UserDataProcessing").master('local[*]')\
    .getOrCreate()
    
    
    df = load_data(spark,file_path)
    df.show(2,truncate=False)
    
    # Step 2: Create additional columns
    col_df = create_additional_columns(df)
    col_df.show()
    
    # Step 3: Apply filters
    filter_df = apply_filters(df)
    filter_df.show()
    
    # Step 4: Group and aggregate
    df_grouped = group_and_aggregate(df)
    df_grouped.show()
    
    # Step 5: Sort the result
    df_sorted = sort_result(df_grouped)
    df_sorted.show()
    
    # Step 6: Mask phone number
    df_masked = mask_phone_number(df)
    df_masked.show()
    
    
    df.printSchema()
    