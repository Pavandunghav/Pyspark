from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

spark=SparkSession.builder.appName("mysql_df").master('local[*]')\
                          .config('spark.jars','/home/user/Downloads/mysql-connector-j-8.4.0.jar')\
                          .getOrCreate()

emp_df=spark.read.format('jdbc')\
                 .option('url','jdbc:mysql://localhost:3306/pavan_db')\
                 .option('driver','com.mysql.cj.jdbc.Driver')\
                 .option('user','root')\
                 .option('password','Cloud@123$')\
                 .option("dbtable","emp")\
                 .load()
                 
     
# join_df=emp_df.join(emp_df,emp_df['deptno']==emp_df['deptno'],'inner')
# join_df

# join_df.show(5)            
#emp_df.show(5)
emp_df.createOrReplaceTempView('emp')

spark.sql('''select e1.empno as emp_empno, e2.ename as emp_name ,e2.mgr as mgr_empno ,e2.ename as mgr_name,e3.emp_count  as mgr_team_cnt 
             from 
             emp e1 ,emp e2,
             (select e1.mgr ,count(e1.ename) as emp_count
             from emp e1,emp e2 
             where e1.mgr=e2.empno
             group by e1.mgr) as e3 
             
             where e2.empno=e3.mgr
             and e1.empno=e2.mgr;
             
           ''').show(5)


emp_df.write.mode('overwrite').format('jdbc')\
                 .option('url','jdbc:mysql://localhost:3306/pavan_db')\
                 .option('driver','com.mysql.cj.jdbc.Driver')\
                 .option('user','root')\
                 .option('password','Cloud@123$')\
                 .option("dbtable","emp_mgr_cnt")\
                 .save()
                 
     
                 
                 
                 
                 