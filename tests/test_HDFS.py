from pyspark import SparkContext
import os
import json

#########################################################
##########################################################
#
#Para saber el puerto en el que corre HDFS
#   hdfs getconf -confKey fs.defaultFS
#
##########################################################
##########################################################

#Copies one file to HDFS
infile = os.path.join('hdfs://localhost:9000/lambda','json_text_file.txt')
outfile = os.path.join('hdfs://localhost:9000/lambda','salida_test_HDFS')
sc = SparkContext(appName="testHDFS")
rdd = sc.textFile(infile)
if rdd.count() > 0:
    print outfile
    rdd.saveAsTextFile(outfile)
print rdd.collect()

