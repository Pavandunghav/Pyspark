from pyspark import SparkConf 
from pyspark import SparkContext 
from pyspark import SQLContext 


SparkConf = SparkConf().setAppName('rdd_to_dataframe').setMaster('local[*]')
sc=SparkContext(conf=SparkConf)
sqlCxt=SQLContext(sc)
#print(sc)


dept = [("Finance",10), ("Marketing",20), ("Sales",30), ("IT",40)]

dept_rdd=sc.parallelize(dept)
#print(dept_rdd.collect())

my_df=dept_rdd.toDF(['department_name','department_id'])
my_df.show()





 

