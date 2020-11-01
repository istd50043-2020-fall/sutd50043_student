import sys
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Wordcount Application")
sc = SparkContext(conf=conf)

hdfs_nn = "localhost"
text_file = sc.textFile("hdfs://%s:9000/input/" % (hdfs_nn))
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://%s:9000/output/" % (hdfs_nn))
sc.stop()

