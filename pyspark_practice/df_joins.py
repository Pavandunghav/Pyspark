##Resource Importing
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType,StructField,StringType,IntegerType



##Object Creation 
spark=SparkSession.builder.appName('df_joins').master('local[*]').getOrCreate()


# emp = [(1, "Saif", -1, "2018", "10", "M", 3000),
#            (2, "Ram", 1, "2010", "20", "M", 4000),
#            (3, "Aniket", 1, "2010", "10", "M", 1000),
#            (4, "Mitali", 2, "2005", "10", "F", 2000),
#            (5, "Nahid", 2, "2010", "40", "", -1),
#            (6, "Sufiyan", 2, "2010", "50", "", -1)]

# ##Creating the DataFrame
# emp_df=spark.createDataFrame(data=emp,
#                              schema=['emp_id','emp_name','emp_rank','emp_year','emp_age','emp_gender','emp_salary'])


# ##Showing the Data 
# emp_df.show(truncate=False)
# emp_df.printSchema()




emp = [(1, "Saif", -1, "2018", 10, "M", 3000),
           (2, "Ram", 1, "2010", 20, "M", 4000),
           (3, "Aniket", 1, "2010", 10, "M", 1000),
           (4, "Mitali", 2, "2005", 10, "F", 2000),
           (5, "Nahid", 2, "2010", 40, "", -1),
           (6, "Sufiyan", 2, "2010", 50, "", -1)]

emp_schema=StructType([
    StructField('emp_id',StringType()),
    StructField('emp_name',StringType()),
    StructField('emp_rank',IntegerType()),
    StructField('emp_year',StringType()),
    StructField('emp_dept_id',IntegerType()),
    StructField('emp_gender',StringType()),
    StructField('emp_salary',IntegerType())   
])


dept = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("IT", 40)]

dept_schema=StructType([
    StructField('dept_name',StringType()),
    StructField('dept_id',IntegerType())    ])
    

##Creating the DataFrame
dept_df=spark.createDataFrame(data=dept,
                             schema=dept_schema)



##Creating the DataFrame
emp_df=spark.createDataFrame(data=emp,
                             schema=emp_schema)


##Joins 


##Inner Join 
normal_join_df=emp_df.join(dept_df,
                            emp_df['emp_dept_id']==dept_df['dept_id'],
                            'inner')
emp_df.show(truncate=False)
dept_df.show(truncate=False)
normal_join_df.show(truncate=False)


##Left semi join 

leftsemi_join_df=emp_df.join(dept_df,
                             emp_df['emp_dept_id']==dept_df['dept_id'],
                             'leftsemi')
leftsemi_join_df.show(truncate=False)


##Antisemi 

antisemi_join_df=emp_df.join(dept_df,
                             emp_df['emp_dept_id']==dept_df['dept_id'],
                             'leftanti')
antisemi_join_df.show(truncate=False)



