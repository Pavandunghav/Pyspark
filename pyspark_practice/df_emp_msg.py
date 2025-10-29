from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

#EmpID|Name|Department|Email|Phone|Message

def load_data(spark,path):
    
    
    data_schema=StructType([
        
        StructField('emp_id',StringType()),
        StructField("emp_name",StringType()),
        StructField('department',StringType()),
        StructField('email',StringType()),
        StructField('phone',StringType()),
        StructField('message',StringType())
    ])
    
    data_df=spark.read.options(header=True,delimiter='|').schema(data_schema).csv(path)
    
    return data_df 

def analyze_emp_contacts(df):
    
    urgent_df=df.withColumn('urgent_flag',when(((df['message'].contains('Urgent')) | (df['message'].contains('urgent') )),True).otherwise(False))
                            
    # print("The dataframe with the urgent keyword in message column")
    # urgent_df.show()
    
    phone_df=urgent_df.withColumn('1st 3 digits',df['phone'].substr(1,3))
    # print("The first 3 digits of phone number")
    # phone_df.show(truncate=True)
    
    clean_df=phone_df.withColumn('clean_phone',regexp_replace(phone_df['phone'],'[\\-\\(\\)\\s]',''))
    
    email_df=clean_df.withColumn('email_domain',regexp_extract(df['email'],r'@(.+)',0))
   
    emp_df=email_df.withColumn('employee_info',concat_ws('_',email_df['emp_name'],email_df['department']))
                                 
    
    
    
    return emp_df
    
    
    

if __name__=='__main__':
    
    
    path='hdfs://localhost:9000/user/hadoop/HFS/Input/emp_msg.txt'
    
    spark=SparkSession.builder.appName('emp_message').master('local[*]').getOrCreate()
    data_df=load_data(spark,path)
    data_df.show(truncate=False)
    
    data_msg_analysis_df=analyze_emp_contacts(data_df)
    data_msg_analysis_df.show(truncate=False)
    
    
    