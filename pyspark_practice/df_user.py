from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def load_data(spark: SparkSession, path: str) -> DataFrame:
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("clicks", IntegerType(), False),
        StructField("session_time_sec", IntegerType(), False),
        StructField("purchase_amount", DoubleType(), False),
        StructField("SalesTime", LongType(), False)
    ])
    user_data_df = spark.read.schema(schema).csv(path)
    return user_data_df

def cleanse_data(df: DataFrame) -> DataFrame:
    df_cleaned = df.dropDuplicates().filter(
        (col('user_id').isNotNull()) &
        (col('email').isNotNull()) &
        (col('SalesTime').isNotNull()) &
        (col('age').between(0, 120)) &
        (col('email').rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
    )
    return df_cleaned

def mask_sensitive_fields(df: DataFrame) -> DataFrame:
    df_masked = df.withColumn(
        'email', regexp_replace(col('email'), r"([a-zA-Z0-9._%+-])([a-zA-Z0-9._%+-]+)(@)", r"\1****\3")
    ).withColumn(
        'phone', concat(lit("***-***-"), substring(col('phone'), 8, 4))
    )
    return df_masked

def detect_outliers(df: DataFrame) -> DataFrame:
    df_outliers = df.withColumn(
        'is_outlier', 
        (col('clicks') > 1000) | (col('session_time_sec') > 10000) | (col('purchase_amount') > 5000)
    )
    return df_outliers

def detect_anomalies(df: DataFrame) -> DataFrame:
    df_anomalies = df.withColumn(
        'is_anomaly', 
        ((col('session_time_sec') == 0) & (col('purchase_amount') > 0)) |
        (col('email').isNull() & (col('clicks') > 0))
    )
    return df_anomalies

def convert_unix_to_timestamp(df: DataFrame) -> DataFrame:
    df_timestamp = df.withColumn(
        'event_time', from_unixtime(col('SalesTime')).cast('timestamp')
    ).withColumn(
        'year', year(col('event_time'))
    ).withColumn(
        'month', month(col('event_time'))
    )
    return df_timestamp

def validate_data(df: DataFrame) -> DataFrame:
    df_validated = df.withColumn(
        'is_valid',
        (col('email').rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")) &
        (col('age').between(0, 120)) &
        (col('clicks') >= 0) &
        (col('purchase_amount') >= 0)
    )
    return df_validated

def generate_quality_report(df: DataFrame) -> dict:
    total_records = df.count()
    null_email = df.filter(col('email').isNull()).count()
    duplicate_users = df.count() != df.dropDuplicates(['user_id']).count()
    invalid_age = df.filter(~col('age').between(0, 120)).count()
    outliers = df.filter(col('is_outlier') == True).count()
    anomalies = df.filter(col('is_anomaly') == True).count()

    report = {
        'total_records': total_records,
        'null_email_count': null_email,
        'duplicate_users': duplicate_users,
        'invalid_age_count': invalid_age,
        'outliers_count': outliers,
        'anomalies_count': anomalies
    }
    
    return report

if __name__ == "__main__":
    spark = SparkSession.builder.appName('User_data').master('local[*]').getOrCreate()
    path = "hdfs://localhost:9000/user/hadoop/HFS/Input/user.csv"

    df_load = load_data(spark, path)
    df_load.show()
    df_cleaned = cleanse_data(df_load)
    df_cleaned.show()
    df_masked = mask_sensitive_fields(df_cleaned)
    df_masked.show()
    df_outliers = detect_outliers(df_masked)
    df_outliers.show()
    df_anomalies = detect_anomalies(df_outliers)
    df_anomalies.show()
    df_timestamp = convert_unix_to_timestamp(df_anomalies)
    df_timestamp.show()
    df_validated = validate_data(df_timestamp)
    df_validated.show()
    quality_report = generate_quality_report(df_validated)
    print(quality_report)
