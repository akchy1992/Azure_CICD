# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 13:32:00 2019

@author: avik
"""

#import sys
from operator import add

from pyspark.sql import SparkSession

def main():
    spark = SparkSession\
        .builder\
        .appName("PythonRowCount")\
        .getOrCreate()

    lines = spark.read.text("C:/Users/Avik/Downloads/Azure/demo.csv").rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split('\n')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
#    print("Y")
    counts.saveAsTextFile("C:/Users/Avik/Downloads/Azure/Moved")
    
    spark.stop()

if __name__ == "__main__":
	main()