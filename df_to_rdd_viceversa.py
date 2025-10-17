from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

spark=SparkSession.builder.appName("streaming_prac").master('local[*]').getOrCreate()

#wc_df=spark.read.csv('/user/hadoop/HFS/Input/wordcount.txt').toDF('value')
wc_df=spark.read.csv('/user/hadoop/HFS/Input/wordcount.txt').withColumnRenamed("_c0","value")
wc_df.show(truncate=False)
#wc_df.rdd()

wc_split_df=wc_df.withColumn('value',explode(split(wc_df['value']," ")))
wc_split_df.show(truncate=False)

wc_groupby=wc_split_df.groupBy('value').agg(count(wc_split_df['value']).alias("word_count"))
wc_groupby.show(truncate=False)

wc_groupby.write.mode('overwrite').format('csv').save('hdfs://localhost:9000/user/hadoop/HFS/Output/streaming_output')

