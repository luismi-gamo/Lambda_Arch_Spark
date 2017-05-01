from pyspark import SparkContext
import os
import json

sc = SparkContext(appName="testHDFS")

##########################################################
##########################################################
#
#Para saber el puerto en el que corre HDFS
#   hdfs getconf -confKey fs.defaultFS
#
##########################################################
##########################################################

infile = os.path.join('hdfs://localhost:9000/lambda','json_text_file.txt')
outfile = os.path.join("hdfs://localhost:9000/lambda','salida_test_HDFS")

rdd = sc.textFile(infile)

if rdd.count() > 0:
    print outfile
    rdd.saveAsTextFile(outfile)

print rdd.collect()


