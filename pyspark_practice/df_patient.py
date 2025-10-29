from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

#PatientID,Name,Department,VisitDate,BillAmount,InsuranceUsed,Email
def load_df(spark,path)->DataFrame:
    
    data_schema=StructType([
        StructField("Patient_id",IntegerType()),
        StructField("Name",StringType()),
        StructField("Department",StringType()),
        StructField("VisitDate",DateType()),
        StructField("BillAmount",StringType()),
        StructField("InsuranceUsed",StringType()),
        StructField("Email",StringType())
    ])
    
    data_df=spark.read.options(header=True).schema(data_schema).csv(path,header=True)
    
    return data_df 

def clean_missing_and_invalid_email(df:DataFrame)->DataFrame:
    email_df=df.filter((df['Email'].contains('@')) & df['Email'].isNotNull())
    
    return email_df 

def filter_valid_billing_records(df:DataFrame)-> DataFrame:
    
    bill_df=df.filter((df['BillAmount']>0) & (df['BillAmount'].isNotNull()))
    
    return bill_df 

def null_handling_patient(df:DataFrame)->DataFrame:
    
    bill_df=df.filter((df['BillAmount']>0) & (df['BillAmount'].isNotNull()))
    fill_df=bill_df.fillna({'Email':'Invalid','Department':'General'})
    fill_df=fill_df.withColumn('BillAmount',when(fill_df['BillAmount']==0,300).otherwise(fill_df['BillAmount']))
    
    return fill_df 
                       
def compute_department_avg(df:DataFrame)->DataFrame:
    
    avg_df=df.groupby('Department').agg(avg(df['BillAmount']))
    
    return avg_df 

def total_patient_avg_bill(df:DataFrame)->DataFrame:
    
    total_avg_bill=df.agg(count(df['Name']).alias('Patient_count'),avg(df['BillAmount']).alias('avg_bill'))
    data_dict=total_avg_bill.collect()[0].asDict()
    
    return data_dict

def count_insured_patients(df:DataFrame)->DataFrame:
    
    insure_count_df=df.filter(df['InsuranceUsed']=='true')
    insure_df=insure_count_df.agg(count(insure_count_df['Name']).alias('pat_count'))
    print(insure_df.collect()[0]['pat_count'])
    return insure_df 

def most_frequent_department(df:DataFrame)->DataFrame:
    
    count_df=df.groupBy('Department').agg(count(df['Department']).alias("department_count"))
    
    return count_df.orderBy(count_df['department_count'].desc()).limit(1)

if __name__=='__main__':
    
    spark=SparkSession.builder.appName('patient_df').master('local[*]').getOrCreate()
    path='hdfs://localhost:9000/user/hadoop/HFS/Input/patient_v2.csv'
    
    print("#Table1")
    data_df=load_df(spark,path)
    data_df.show()
    
    print("#Table2")
    email_df= clean_missing_and_invalid_email(data_df)
    email_df.show()
    
    print("#Table3")
    bill_df=filter_valid_billing_records(data_df)
    bill_df.show()
    
    print("#Table4")
    null_df=null_handling_patient(data_df)
    null_df.show()
    
    print("#Table5")
    avg_df=compute_department_avg(null_df) 
    avg_df.show()
    
    print("#Table6")
    count_avg_df=total_patient_avg_bill(data_df)
    print(count_avg_df)
    
    print("#Table7")
    insure_df=count_insured_patients(data_df)
    insure_df.show()
    
    print("#Table8")
    dept_df=most_frequent_department(data_df)
    dept_df.show()
    