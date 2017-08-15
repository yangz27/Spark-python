from pyspark import SparkConf,SparkContext,SQLContext,Row
from pyspark.sql import functions as F
APP_NAME="count unique vistor of everyday"

def main(sc,sqlC):
    # 模拟数据
    userAccessLog=[
        "2017-01-01,1122",
        "2017-01-01,1122",
        "2017-01-01,1123",
        "2017-01-01,1124",
        "2017-01-01,1124",
        "2017-01-02,1122",
        "2017-01-01,1121",
        "2017-01-01,1123",
        "2017-01-01,1123"
    ]
    accessLogRDD=sc.parallelize(userAccessLog)
    RowRDD=accessLogRDD.map(lambda e:Row(e.split(",")[0],int(e.split(",")[1])))
    df=sqlC.createDataFrame(RowRDD,['date','userid'])
    df.show()
    df.printSchema()
    df.groupBy('date').agg(F.countDistinct(df.userid)).show()

if __name__=="__main__":
    conf=SparkConf().setAppName(APP_NAME).setMaster("local[*]")
    sc=SparkContext(conf=conf)
    sqlC=SQLContext(sc)

    main(sc,sqlC)