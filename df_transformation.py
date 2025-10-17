from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 


spark=SparkSession.builder.appName('Transformation').master('local[*]').getOrCreate()

movies_df=spark.read.options(header=True,delimeter=',',inferSchema=True)\
               .csv('hdfs://localhost:9000/user/hadoop/HFS/Input/movies.csv')
               
# movies_df.show(5,truncate=False)
               

               
            
##Creating the dataframes and selection 


# #method 1 
# sel_df1=movies_df.select(movies_df['title'],movies_df['genres'])
# sel_df1.show(5,truncate=False)


# #method 2 
# sel_df2=movies_df.select(col('title'),col('genres'))
# sel_df2.show(5,truncate=False)


# filterdf=movies_df.filter(movies_df['movieId']==3)
#filterdf.show()

# filterdf=movies_df.filter(movies_df['movieId'].isin(3,2,4))
# filterdf.show(truncate=False)



# filterdf=movies_df.filter((movies_df['movieId']==3) | (movies_df['genres']=='Action'))
# filterdf.show(5,truncate=False)


# orderby_df=movies_df.filter((movies_df['movieId'].between(2,10)) | (movies_df['genres']=='Action'))\
#                     .orderBy(movies_df['movieID'].desc())
                    
                    
                    
                    
# orderby_df.show(5,truncate=False)


###Aggregation 




# groupby_df=movies_df.groupBy(movies_df['genres']).agg(count('*').alias('total_count'),min('movieId').alias("minimum"))
                    
# groupby_df.show(5,truncate=False)


##renaming the column names 

# rename_df=movies_df.withColumnRenamed('genres','category')\
#                     .drop('movieId')
                    

# rename_df.show(5,truncate=False)
                      

##withcolumn   : add , update , typecast 


# withcol_df=movies_df.withColumn('genres',split(movies_df['genres'],('\|')))
                                

# withcol_df.show(5,truncate=False)
# withcol_df.printSchema()  



##creating the new column 

# withcol_df=movies_df.withColumn('genres_data',split(movies_df['genres'],('\|')))
                                

# withcol_df.show(5,truncate=False)
# withcol_df.printSchema()  


##Typecasting 


# withcol_df=movies_df.withColumn('movieId',movies_df['movieId'].cast('string'))
                                

# withcol_df.show(5,truncate=False)
# withcol_df.printSchema()       ##It worked 









                                  