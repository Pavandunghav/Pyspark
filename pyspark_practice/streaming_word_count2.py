from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql.window import Window 


spark=SparkSession.builder.appName('streaming_v2').master('local[*]').getOrCreate()

#read  : batch processing 
#readStream : live streaming 

socket_df=spark.readStream.format('socket')\
                    .option("host","localhost")\
                    .option('port','9999')\
                    .load()
                    

wc_split_df=socket_df.withColumn('value',explode(split(socket_df['value']," ")))
wc_groupby=wc_split_df.groupBy('value').agg(count(wc_split_df['value']).alias("word_count"))

                    
result_df=wc_groupby.writeStream.format('console')\
                               .outputMode('update')\
                               .option('checkpointLocation','/home/hadoop/LFS/datasets/dir3')\
                               .start()
                               
result_df.awaitTermination()



