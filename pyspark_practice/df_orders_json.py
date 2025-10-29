from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 


def load_data(spark,path)->DataFrame:
    
    order_schema=StructType([
        StructField('customer_id',IntegerType()),
        StructField('customer_name',StringType()),
        StructField('orders',ArrayType(
            StructType([
            StructField('order_id',IntegerType()),
            StructField('product',StringType()),
            StructField('price',IntegerType()),
            StructField('quantity',IntegerType())
            
            
            
            ]))),
        
    ])
    
    #data_df=spark.read.options().schema(order_schema).json(path)
    data_df=spark.read.options(multiLine=True).schema(order_schema).json(path)
    return data_df 
    
def explode_df(df)->DataFrame:
    
    sel_df=df.select(df['customer_id'],df['customer_name'],explode(df['orders']).alias("Orders"))
    sel_explode_df=sel_df.select(sel_df['customer_id'],sel_df['customer_name'],
                                 sel_df['Orders.order_id'],
                                 sel_df['Orders.product'],
                                 sel_df['Orders.price'],
                                 sel_df['Orders.quantity']
                                 )
    
    return sel_explode_df 

    
def total_revenue(df:DataFrame)->DataFrame:
    
    data_df=df.withColumn('total_revenue',df['price']*df['quantity'])
    data_df=data_df.groupBy(data_df['customer_id']).agg(sum(data_df['total_revenue']).alias('total_cust_revenue'))
    data_df=data_df.orderBy(data_df['total_cust_revenue'].desc()).limit(3)
    return data_df 

def row_datatype(df:DataFrame)->DataFrame:
    
    for i in df.collect():
        print(i.asDict())
        
    for i in df.collect():
        print(tuple(i))
        
    for i in df.collect():
        print(list(i))
        
    return "All the above are dictionaries/tuple/list"


if  __name__ == "__main__" :
    
    spark=SparkSession.builder.appName('orders_json').master('local[*]').getOrCreate()
    path='hdfs://localhost:9000/user/hadoop/HFS/Input/orders.json'
    
    data_df=load_data(spark,path)
    data_df.show()
    
    sel_explode_df=explode_df(data_df)
    sel_explode_df.show()
    sel_explode_df.printSchema()
    
    revenue_df=total_revenue(sel_explode_df)
    revenue_df.show()
    
    row_df=row_datatype(revenue_df)
    print(row_df)
    
    
    