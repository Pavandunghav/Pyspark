from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# 1. Load both datasets using user-defined schema
def load_data_with_schema(spark: SparkSession, order_path: str, customer_path: str) -> Tuple[DataFrame, DataFrame]:
	
 
    cust_data=spark.read.options(header=True).csv(customer_path)
    order_data=spark.read.options(header=True).csv(order_path)

    return (cust_data,order_data)
    
# 2. Clean orders by removing nulls and filtering valid statuses
def clean_orders( df: DataFrame) -> DataFrame:
	
    data_df=df.dropna()
    return data_df 

# 3. Clean customers with null or invalid age
def clean_customers( df: DataFrame) -> DataFrame:
	
    data_df=df.dropna()
    return data_df 

# 4. Mask email and phone fields
def mask_customer_info(df: DataFrame) -> DataFrame:
    masked_df = df.withColumn("email", lit("masked_email@domain.com")) \
                  .withColumn("phone", lit("XXXXXXXXXX"))
    return masked_df

# 5. Drop duplicate orders based on order_id
def drop_duplicate_order( df: DataFrame) -> DataFrame:
    
    data_df=df.drop_duplicates()
    return data_df 


# 6. Flag zero amount orders
def flag_zero_amount_orders( df: DataFrame) -> DataFrame:
    
    data_df=df.withColumn('amount_flag',when(df['total_amount']==0,True).otherwise(False))
    return data_df


# 7. Join orders with customers on customer_id
def join_orders_customers(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    
    data_df=orders_df.join(customers_df,orders_df['customer_id']==customers_df['customer_id'],'left')
    return data_df 
	

# 8. Filter orders based on recent year threshold  # 9 timestamp casting 
def filter_recent_orders(df: DataFrame, year_threshold: int) -> DataFrame:
    
    df = df.withColumn("order_date", from_unixtime(col("order_date")).cast("timestamp"))
    data_df=df.withColumn('order_date',df['order_date'].cast(DateType()))
    print("Year df")
    data_df.show()
    data_df=data_df.filter(year(data_df['order_date'])>year_threshold)
    return data_df 


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

# 12. Customer age group distribution using when
def age_group_distribution(df: DataFrame) -> DataFrame:
    # Cast 'age' column to IntegerType
    df = df.withColumn("age", df["age"].cast(IntegerType()))
    
    # Use 'when' to define age groups
    df = df.withColumn("age_group", when((df["age"] >= 18) & (df["age"] <= 24), '18-24')
                                    .when((df["age"] >= 25) & (df["age"] <= 34), '25-34')
                                    .when((df["age"] >= 35) & (df["age"] <= 44), '35-44')
                                    .when(df["age"] >= 45, '45+')
                                    .otherwise('Unknown'))

    # Group by age group and count
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


# def generate_data_quality_report(df: DataFrame) -> dict:
#     # Initialize the report dictionary
#     report = {}

#     # Count nulls per column using a loop
#     null_counts = {}
#     for c in df.columns:
#         null_counts[c] = df.select(count(when(df[c].isNull(), c))).first()[0]
#     report['nulls'] = null_counts
    
#     # Count duplicates by comparing total and distinct row counts
#     total_rows = df.count()
#     distinct_rows = df.distinct().count()
#     report['duplicates'] = total_rows - distinct_rows

#     # Count unique values per column using a loop
#     unique_counts = {}
#     for c in df.columns:
#         unique_counts[c] = df.select(c).distinct().count()
#     report['unique_values'] = unique_counts

#     # Get the data types of columns using a loop
#     column_types = {}
#     for c in df.columns:
#         column_types[c] = dict(df.dtypes)[c]
#     report['data_types'] = column_types

#     return report


# 20. Count invalid customer age or missing emails
def count_invalid_customers(df: DataFrame) -> int:
    invalid_age_condition = (df['age'] < 18) | (df['age'] > 100)
    invalid_email_condition = df['email'].isNull()
    
    invalid_customers_df = df.filter(invalid_age_condition | invalid_email_condition)
    return invalid_customers_df.count()



if __name__=="__main__":
    
    spark=SparkSession.builder.appName('order_customer_pay').master('local[*]').getOrCreate()
    
    order_path='hdfs://localhost:9000/user/hadoop/HFS/Input/order_pay.csv'
    customer_path='hdfs://localhost:9000/user/hadoop/HFS/Input/customer_pay.csv'
    
    
    data_df=load_data_with_schema(spark,order_path,customer_path)
    # for i in data_df:
    #     i.show()
    
    clean_cust_df=clean_customers(data_df[0])
    clean_cust_df.show()

    clean_order_df=clean_orders(data_df[1])
    clean_order_df.show()
    
    masked_df=mask_customer_info(clean_cust_df)
    print("This is masked df")
    masked_df.show()
    # for i in data_df:
    #    clean_df=clean_orders(i)
    #    clean_df.show()
    
    dup_order_df=drop_duplicate_order(clean_order_df)
    dup_order_df.show()
    
    zero_df=flag_zero_amount_orders(clean_order_df)
    zero_df.show()
    
    join_df=join_orders_customers(clean_order_df,clean_cust_df)
    join_df.show()
    
    year_filter_df=filter_recent_orders(clean_order_df,2000 )
    year_filter_df.show()
    
    
    
    # Extract Year and Month from Order Date
    year_month_order_df = extract_order_year_month(year_filter_df)
    year_month_order_df.show()
    
    
    #order summary 
    summary_df=monthly_order_summary(year_month_order_df)
    summary_df.show()
    
    
    # Generate Reports
    print("Age Group Distribution:")
    age_group_dist = age_group_distribution(clean_cust_df)
    age_group_dist.show()
    
    
    top_df=top_customers_by_spend(year_month_order_df)
    top_df.show()
    
    pay_mode_df=payment_mode_distribution(year_month_order_df)
    pay_mode_df.show()
    
    avg_df=average_order_value(year_month_order_df)
    avg_df.show()
    
    loyalty_df=analyze_customer_loyalty(year_month_order_df)
    loyalty_df.show()
    
    
    anon_df=detect_anomalous_orders(year_month_order_df)
    anon_df.show()
    
    valid_df=validate_customer_schema(clean_cust_df)
    valid_df.show()
    
    
    # print("Data Quality Report:")
    # data_quality_report = generate_data_quality_report(join_df)
    # print(data_quality_report)
    
    print("Invalid Customers Count:")
    invalid_customer_count = count_invalid_customers(clean_cust_df)
    print(f"Invalid customer count: {invalid_customer_count}")

    