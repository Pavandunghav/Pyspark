from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

def load(spark,path):
    
    data_schema=schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
    StructField("session_time_sec", IntegerType(), True),
    StructField("purchase_amount", DoubleType(), True),
    StructField("SalesTime", LongType(), True)
])
    
    data_df=spark.read.schema(data_schema).csv(path,header=True)
    return data_df 

def cleanse_data(df):
    
    initial_row_count = df.count()
    df = df.dropDuplicates()
    duplicate_removed_count = initial_row_count - df.count()
    df = df.filter(df.user_id.isNotNull() & df.email.isNotNull() & df.SalesTime.isNotNull())
    df = df.filter((df.age >= 0) & (df.age <= 120))
    df = df.filter(df.email.contains('@'))
    final_row_count = df.count()

    invalid_age_count = initial_row_count - df.filter((df.age >= 0) & (df.age <= 120)).count()
    invalid_email_count = initial_row_count - df.filter(df.email.contains('@')).count()

    print(f"Initial rows: {initial_row_count}")
    print(f"Duplicate rows removed: {duplicate_removed_count}")
    print(f"Invalid ages: {invalid_age_count}")
    print(f"Invalid emails: {invalid_email_count}")
    print(f"Final row count: {final_row_count}")
    
    return df    
    
# def mask_sensitive_info(df):
#     df = df.withColumn(
#         "email", 
#         concat(substring(df.email, 1, 2), lit("****"), substring(df.email,instr(df.email, '@'), 1000))
#     )
#     df = df.withColumn(
#         "phone", 
#         concat(lit("***-***-"), substring(df.phone, -4, 4))
#     )
#     return df


def detect_outliers(df):
    df = df.withColumn(
        "is_outlier", 
        ( (df.clicks > 1000) | 
          (df.session_time_sec > 10000) | 
          (df.purchase_amount > 5000) )
    )
    
    return df

def detect_anomalies(df):
    df = df.withColumn(
        "is_anomaly", 
        ( (df.session_time_sec == 0) & (df.purchase_amount > 0) ) | 
        ( (df.email.isNull()) & (df.clicks > 0) )
    )
    
    return df

def convert_timestamps(df):
    
    return df.withColumn("event_time", from_unixtime(df.SalesTime).cast("timestamp")) \
             .withColumn("year", year("event_time")) \
             .withColumn("month", month("event_time"))

# Question 7: Validate Data Quality
def validate_data_quality(df):
    df = df.withColumn(
        "is_valid", 
        (df.email.rlike("^[^@]+@[^@]+\.[^@]+$")) & 
        (df.age >= 0) & (df.age <= 120) &
        (df.clicks >= 0) & (df.purchase_amount >= 0)
    )
    return df

# Question 8: Generate Quality Report
def generate_quality_report(df):
    total_records = df.count()
    null_email_count = df.filter(df.email.isNull()).count()
    duplicate_users = df.select("user_id").distinct().count() != df.count()
    invalid_age_count = df.filter((df.age < 0) | (df.age > 120)).count()
    # outliers_count = df.filter(df.is_outlier == True).count()
    # anomalies_count = df.filter(df.is_anomaly == True).count()
    
    report = {
        'total_records': total_records,
        'null_email': null_email_count,
        'duplicate_users': duplicate_users,
        'invalid_age': invalid_age_count,
        # 'outliers': outliers_count,
        # 'anomalies': anomalies_count
    }
    
    return report

if __name__=='__main__':
    
    spark=SparkSession.builder.appName('customer_data_quality').master('local[*]').getOrCreate()
    
    path='data/customer_logs.csv'
    data_df=load(spark,path)
    data_df.show()
    
    clean_df=cleanse_data(data_df)
    clean_df.show()
    
    # mask_df=mask_sensitive_info(data_df)
    # mask_df.show()
    
    outlier_df=detect_outliers(data_df)
    outlier_df.show()
    
    anomaly_df=detect_anomalies(data_df)
    anomaly_df.show()
    
    time_df=convert_timestamps(data_df)
    time_df.show()
    
    validate_df=validate_data_quality(data_df)
    validate_df.show()
    
    quality_df=generate_quality_report(data_df)
    print(quality_df)
    
    