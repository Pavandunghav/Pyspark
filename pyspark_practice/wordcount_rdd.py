from pyspark import SparkConf
from pyspark import SparkContext

sparkconf=SparkConf().setAppName('wc').setMaster("local[*]")
sc=SparkContext(conf=sparkconf)

## Practice Version 1

read_rdd=sc.textFile('hdfs://localhost:9000/user/hadoop/HFS/Input/wordcount.txt')
split_rdd=read_rdd.flatMap(lambda x : x.split(" "))
assign_rdd=split_rdd.map(lambda x : (x,1))
wc_rdd=assign_rdd.reduceByKey(lambda x,y: x+y)

print(wc_rdd.collect())

## Practice Version 2

read_rdd=sc.textFile('hdfs://localhost:9000/user/hadoop/HFS/Input/wordcount_v5.txt')
split_rdd=read_rdd.flatMap(lambda x : x.split(" "))
assign_rdd=split_rdd.map(lambda x : (x,1))
wc_rdd=assign_rdd.reduceByKey(lambda x,y: x+y)

print(wc_rdd.collect())

for i in wc_rdd.collect():
    print(i)
    
    
dept = [("Finance",10), ("Marketing",20), ("Sales",30), ("IT",40)]  






















