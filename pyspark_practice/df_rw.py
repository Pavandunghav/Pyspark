from pyspark.sql import SparkSession 

spark=SparkSession.builder.appName('df_rw').master('local[*]').getOrCreate()

# print(spark)

# movies_df=spark.read.format('csv')\
#             .option('header','true')\
#             .option('delimiter',',')\
#             .option('inferSchema','true')\
#             .load('hdfs://localhost:9000/user/hadoop/HFS/Input/movies.csv')

# movies_df.show(5,truncate=False)
# movies_df.printSchema()



##Another way to read the csv 

movies_df=spark.read.options(header=True,delimiter=',',inferSchema=True)\
            .csv('hdfs://localhost:9000/user/hadoop/HFS/Input/movies.csv')


# movies_df.show(5,truncate=False)
# movies_df.printSchema()


# movies_df.write.save('hdfs://localhost:9000/user/hadoop/HFS/Output/movies_output')



# Write Mode:
# 1) Error: Default, If the dir is not available it will create, if
# its available it will give an error.
# 2) Ignore: If the dir is not available it will create, if
# its available it will ignore.
# 3)  Append: If the dir is not available it will create, if
# its available it will append the file to the existing dir.
# 4) Overwrite: If the dir is not available it will create, if
# its available it will overwrite the data.




movies_df.write.mode('Overwrite').save('hdfs://localhost:9000/user/hadoop/HFS/Output/movies_output')



