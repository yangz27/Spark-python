import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME']="/home/open/spark-2.0/"

# Append pyspark  to Python Path
sys.path.append("/home/open/spark-2.0/python/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)