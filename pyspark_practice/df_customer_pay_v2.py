from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Load both datasets using user-defined schema
def load_data_with_schema(spark: SparkSession, order_path: str, customer_path: str) -> Tuple[DataFrame, DataFrame]:
    cust_data = spark.read.options(header=True).csv(customer_path)
    order_data = spark.read.options(header=True).csv(order_path)
    return (cust_data, order_data)

# 2. Clean orders by removing nulls and filtering valid statuses
def clean_orders(df: DataFrame) -> DataFrame:
    data_df = df.dropna()
    return data_df 

# 3. Clean customers with null or invalid age
def clean_customers(df: DataFrame) -> DataFrame:
    data_df = df.dropna()
    return data_df 

# 4. Mask email and phone fields
def mask_customer_info(df: DataFrame) -> DataFrame:
    masked_df = df.withColumn("email", lit("masked_email@domain.com")) \
                  .withColumn("phone", lit("XXXXXXXXXX"))
    return masked_df

# 5. Drop duplicate orders based on order_id
def drop_duplicate_orders(df: DataFrame) -> DataFrame:
    data_df = df.dropDuplicates(["order_id"])
    return data_df

# 6. Flag zero amount orders
def flag_zero_amount_orders(df: DataFrame) -> DataFrame:
    data_df = df.withColumn('amount_flag', when(df['total_amount'] == 0, True).otherwise(False))
    return data_df

# 7. Join orders with customers on customer_id
def join_orders_customers(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    data_df = orders_df.join(customers_df, orders_df['customer_id'] == customers_df['customer_id'], 'left')
    return data_df 

# 8. Filter orders based on recent year threshold
def filter_recent_orders(df: DataFrame, year_threshold: int) -> DataFrame:
    df = df.withColumn("order_date", from_unixtime(col("order_date")).cast("timestamp"))
    data_df = df.withColumn('order_date', df['order_date'].cast(DateType()))
    data_df = data_df.filter(year(data_df['order_date']) > year_threshold)
    return data_df 

# 9. Convert order_date (UNIX) to timestamp
def convert_unix_to_timestamp(df: DataFrame) -> DataFrame:
    df = df.withColumn("order_date", from_unixtime(col("order_date")).cast("timestamp"))
    return df

# 10. Extract year and month from order_date
def extract_order_year_month(df: DataFrame) -> DataFrame:
    df = df.withColumn("order_year", year(df["order_date"])) \
           .withColumn("order_month", month(df["order_date"]))
    return df

# 11. Monthly order summary (count and revenue)
def monthly_order_summary(df: DataFrame) -> DataFrame:
    monthly_summary_df = df.groupBy("order_year", "order_month") \
                           .agg(count("order_id").alias("order_count"), 
                                sum("total_amount").alias("total_revenue"))
    return monthly_summary_df

# 12. Customer age group distribution
def age_group_distribution(df: DataFrame) -> DataFrame:
    # Cast 'age' column to IntegerType
    df = df.withColumn("age", df["age"].cast(IntegerType()))
    
    # Define age ranges
    age_group_udf = udf(lambda age: '18-24' if age >= 18 and age <= 24 else
                        '25-34' if age >= 25 and age <= 34 else
                        '35-44' if age >= 35 and age <= 44 else
                        '45+' if age >= 45 else 'Unknown', StringType())
    
    df = df.withColumn("age_group", age_group_udf(df['age']))
    age_distribution = df.groupBy("age_group").count()
    
    return age_distribution

# 13. Top N customers by total spend
def top_customers_by_spend(df: DataFrame, n: int = 5) -> DataFrame:
    df = df.groupBy("customer_id").agg(sum("total_amount").alias("total_spend"))
    top_customers_df = df.orderBy(col("total_spend").desc()).limit(n)
    return top_customers_df

# 14. Payment mode revenue and counts
def payment_mode_distribution(df: DataFrame) -> DataFrame:
    payment_mode_df = df.groupBy("payment_mode") \
                        .agg(count("order_id").alias("order_count"), 
                             sum("total_amount").alias("total_revenue"))
    return payment_mode_df

# 15. Average order value
def average_order_value(df: DataFrame) -> DataFrame:
    avg_order_value = df.agg(avg("total_amount").alias("average_order_value"))
    return avg_order_value

# 16. Customer loyalty flag (if more than 3 orders)
def analyze_customer_loyalty(df: DataFrame) -> DataFrame:
    order_count_df = df.groupBy("customer_id").agg(count("order_id").alias("order_count"))
    loyal_customers_df = order_count_df.withColumn("loyalty_flag", when(col("order_count") > 3, True).otherwise(False))
    return loyal_customers_df

# 17. Detect high-value anomalous orders (>10000)
def detect_anomalous_orders(df: DataFrame) -> DataFrame:
    anomalous_df = df.withColumn("anomalous_order", when(df['total_amount'] > 10000, True).otherwise(False))
    return anomalous_df

# 18. Validate schema structure of customers DataFrame
def validate_customer_schema(df: DataFrame) -> DataFrame:
    expected_columns = ['customer_id', 'name', 'email', 'phone', 'age']
    actual_columns = df.columns
    missing_columns = set(expected_columns) - set(actual_columns)
    
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
    else:
        print("Schema is valid!")
    
    return df

# 19. Generate data quality report (nulls, duplicates, etc.)
def generate_data_quality_report(df: DataFrame) -> dict:
    report = {}
    report['nulls'] = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas().to_dict('records')[0]
    report['duplicates'] = df.count() - df.distinct().count()
    return report

# 20. Count invalid customer age or missing emails
def count_invalid_customers(df: DataFrame) -> int:
    invalid_age_condition = (df['age'] < 18) | (df['age'] > 100)
    invalid_email_condition = df['email'].isNull()
    
    invalid_customers_df = df.filter(invalid_age_condition | invalid_email_condition)
    return invalid_customers_df.count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName('order_customer_pay').master('local[*]').getOrCreate()
    
    order_path = 'hdfs://localhost:9000/user/hadoop/HFS/Input/order_pay.csv'
    customer_path = 'hdfs://localhost:9000/user/hadoop/HFS/Input/customer_pay.csv'
    
    # Load Data
    data_df = load_data_with_schema(spark, order_path, customer_path)
    
    # Clean Data
    clean_cust_df = clean_customers(data_df[0])
    clean_order_df = clean_orders(data_df[1])
    
    # Mask Customer Info
    masked_cust_df = mask_customer_info(clean_cust_df)
    
    # Drop Duplicates
    dup_order_df = drop_duplicate_orders(clean_order_df)
    
    # Flag Zero Amount Orders
    zero_df = flag_zero_amount_orders(clean_order_df)
    
    # Join Orders and Customers
    join_df = join_orders_customers(clean_order_df, clean_cust_df)
    
    # Filter Recent Orders
    year_filter_df = filter_recent_orders(clean_order_df, 2000)
    
    # Convert UNIX to Timestamp
    timestamp_df = convert_unix_to_timestamp(join_df)
    
    # Extract Year and Month from Order Date
    year_month_df = extract_order_year_month(timestamp_df)
    
    # Generate Reports
    print("Age Group Distribution:")
    age_group_dist = age_group_distribution(masked_cust_df)
    age_group_dist.show()
    
    print("Top 5 Customers by Spend:")
    top_customers = top_customers_by_spend(join_df)
    top_customers.show()
    
    print("Payment Mode Distribution:")
    payment_mode_dist = payment_mode_distribution(join_df)
    payment_mode_dist.show()
    
    print("Average Order Value:")
    avg_order_value = average_order_value(join_df)
    avg_order_value.show()
    
    print("Customer Loyalty:")
    loyalty_flag_df = analyze_customer_loyalty(join_df)
    loyalty_flag_df.show()
    
    print("Anomalous Orders:")
    anomalous_orders_df = detect_anomalous_orders(join_df)
    anomalous_orders_df.show()
    
    print("Data Quality Report:")
    data_quality_report = generate_data_quality_report(join_df)
    print(data_quality_report)
    
    print("Invalid Customers Count:")
    invalid_customer_count = count_invalid_customers(masked_cust_df)
    print(f"Invalid customer count: {invalid_customer_count}")
