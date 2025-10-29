###Transformation practice 

from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql.window import Window

#order_id,customer_id,order_date,category,sub_category,region,quantity,unit_price,status


def load_orders(spark:SparkSession,path:str) -> DataFrame :
    
    order_schema = StructType( [
        StructField ('order_id',IntegerType()),
        StructField('customer_id',StringType()),
        StructField('order_date',DateType()),
        StructField('category',StringType()),
        StructField('sub_category',StringType()),
        StructField('region',StringType()),
        StructField('quantity',IntegerType()),
        StructField('unit_price',DoubleType()),
        StructField('status',StringType())
        
     ] )
    
    # 'hdfs://localhost:/user/hadoop/HFS/Input/order_data.csv'
    
    df_order=spark.read.options(header=True)\
        .schema(order_schema)\
        .csv(path)
        
    return df_order 

#order_id,return_date,reason,refund_amount
def load_returns(spark:SparkSession)->DataFrame:
    
    return_schema=StructType([
        StructField('order_id',IntegerType()),
        StructField('return_date',DateType()),
        StructField('reason',StringType()),
        StructField('refund_amount',DoubleType())
    ])
    
    
    return_df=spark.read.options(header=True)\
                    .schema(return_schema)\
                    .csv('hdfs://localhost:9000/user/hadoop/HFS/Input/return_data.csv')
                    
    return return_df 
    
    
##Create DataFrame 
def create_df(spark:SparkSession):
    
    emp_data=  [("E001", "Alice", "HR", 5500, "2023-01-10"),
    ("E002", "Bob", "HR", 6000, "2023-03-15"),
    ("E003", "Charlie", "HR", 5500, "2023-05-20"),
    ("E004", "David", "HR", 5000, "2023-02-05"),
    ("E005", "Eve", "IT", 7500, "2023-04-12"),
    ("E006", "Frank", "IT", 7200, "2023-05-25"),
    ("E007", "Grace", "Finance", 6500, "2023-01-30"),
    ("E008", "Hannah", "Finance", 6800, "2023-03-18"),
    ("E009", "Ivy", "Finance", 6600, "2023-06-10"),
    ("E010", "Jack", "Finance", 6400, "2023-07-12")]
    
    
    
    emp_schema=StructType([
        StructField('emp_id',StringType()),
        StructField('emp_name',StringType()),
        StructField('department',StringType()),
        StructField('salary',IntegerType()),
        StructField('hire_date',StringType())
    ])
    
    emp_df=spark.createDataFrame(emp_data,emp_schema)
    return emp_df 
#Add computed column (total_price)

def compute_column(df:DataFrame) -> DataFrame:
    
    compute_df=df.withColumn('revenue',df['quantity']*df['unit_price'])
    
    return compute_df 
    
#5. Filter by status = Delivered, Rename a column
def filter_status(df:DataFrame) -> DataFrame:
    
    filter_df=df.filter((df['status']=='Delivered') | (df['status']=='Cancelled')).withColumnRenamed('status','order_status')
    
    return filter_df 

    
def cast_column(df):
    
    cast_df=df.withColumn('order_date_casted',df['order_date'].cast(StringType()))
    return cast_df 


#Total quantity per region & output col: total_quantity
def quant_region(df):
    
    quant_df=df.groupBy('region').agg(sum('quantity').alias('total_quantity'))
    
    return quant_df 

#Top 3 categories by revenue
def top_categories(df):
    
    
    top_df=df.groupBy('category').agg(sum('revenue'))
    
    return top_df 
    #return top_df.orderBy(top_df['revenue']).desc().limit(3)

#Orders placed after specific date(31-12-2023) & Year-wise revenue 

def order_date_year_wise(df:DataFrame) -> DataFrame:
    
    #date_df=df.filter(df['order_date'] > to_date.lit('2023-12-31' ))
    date_df=df.filter(df['order_date'] > '2023-12-31' )
    date_df_year=date_df.withColumn('Year_column',year(date_df['order_date']))
    date_df_month_year=date_df_year.withColumn('Month_column',month(date_df_year['order_date']))
    
    return date_df_month_year.groupBy('Year_column').agg(sum(date_df_month_year['revenue']).alias('year_wise_revenue'))
    
    
#Find Customers who returned the orders with refund amount
def customer_refund(order_df,customer_df):
    
    # load_orders
    # load_returns
    refund_df=customer_df.join(order_df,customer_df['order_id']==order_df['order_id'],'left')
    #return refund_df.select(order_df['customer_id'],customer_df['order_id'],customer_df['refund_amount'])
    return refund_df.select(refund_df['customer_id'],refund_df['refund_amount']).orderBy(refund_df['refund_amount'].asc())

def fill_data(df):
    
    filled_df=df.fillna({'customer_id':100})
    
    return filled_df 


##conditional statements for datetime  and date format (unix_timestamp)
def change_status(df:DataFrame) -> DataFrame :
    
    new_df=df.withColumn('new_status',
                            when(df['status']== 'Delivered','D').
                            when(df['status']=='Cancelled','C').
                            when(df['status']=='Returned','R').
                            when(df['status'].isNull(),'its Null').otherwise('No Data'))
    #new_df=new_df.withColumn('new_format_order_date',date_format('order_date','dd/MM/yyyy'))
    new_df=new_df.withColumn('new_format_order_date',from_unixtime(unix_timestamp(new_df['order_date']),'dd/MM/yyyy'))
    
    return new_df



##Window function 

def win_fun(df:DataFrame) -> DataFrame:
    
    wcc1=Window.partitionBy(df['department']).orderBy(df['salary'].desc())
    
    rank_df=df.withColumn('emp_rank',rank().over(wcc1))
    dense_rank_df=rank_df.withColumn('emp_dense_rank',dense_rank().over(wcc1))
    row_dense_rank_df=dense_rank_df.withColumn('emp_row_dense_rank',row_number().over(wcc1))
    lag_df=row_dense_rank_df.withColumn('emp_lag',lag('salary',1,0).over(wcc1))
    lead_df=lag_df.withColumn('emp_lead',lead('salary',1,0).over(wcc1))
    
    return lead_df 

def concat_data(df):
    
    concat_df=df.withColumn('combine_column',concat_ws('-','category','sub_category'))
    
    return concat_df

#Driver Code :
if __name__== '__main__' :
    
    spark=SparkSession.builder.appName('Order_df').master('local[*]').getOrCreate()
    
    path='hdfs://localhost:9000/user/hadoop/HFS/Input/order_data_v2.csv'
    
    order_df=load_orders(spark,path)
    order_df.show(truncate=False)
    
    # return_df=load_returns(spark)
    # return_df.show(truncate=False)
    
    # compute_df=compute_column(order_df)
    # compute_df.show(truncate=False)
    
    # filter_df=filter_status(order_df)
    # filter_df.show(truncate=False)
    
    # cast_df=cast_column(order_df)
    # cast_df.show(truncate=False)
    # cast_df.printSchema()
    
    
    # quant_df=quant_region(order_df)
    # quant_df.show(truncate=False)
    
    
    
    # compute_df.show(truncate=False)
    # top_df=top_categories(compute_df)
    # top_df.show(truncate=False)
    
    
    # ody_df=order_date_year_wise(compute_df) 
    # ody_df.show(truncate=False)
    
    # cust_refund_df=customer_refund(order_df,return_df)
    # cust_refund_df.show(truncate=False)
    
    
    # fill_df=fill_data(cust_refund_df)
    # fill_df.show(truncate=False)
    
    
    # status_df=change_status(order_df)
    # status_df.show(truncate=False)
    
    
    
    # emp_df=create_df(spark)
    # emp_df.show(truncate=False)
    
    # win_df=win_fun(emp_df)
    # win_df.show(truncate=False)
    
    concat_df=concat_data(order_df)
    concat_df.show(truncate=False)
    
    
    
    
    
    