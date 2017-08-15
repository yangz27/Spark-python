from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("my application")\
        .getOrCreate()

    # your code

    spark.stop()
