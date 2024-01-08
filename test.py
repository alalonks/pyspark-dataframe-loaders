import sys
from random import random
from operator import add
from FileLoaders import FileLoader

from pyspark.sql import SparkSession, DataFrame



if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        # print ("x is %f" % x)
        # print ("y is %f" % y)
        retVal = 1 if x ** 2 + y ** 2 <= 1 else 0
        
        # print ("retVal is %f" % retVal)
        return  retVal

    fileloader = FileLoader(spark)
    df = fileloader.spark_load("./data/Pioneers_Dynamo.csv")
    
    df.show(10)
     
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()