from pyspark import SparkConf,SparkContext,SQLContext
from pyspark.sql import Row

APP_NAME="count sale of everyday"

def main(sc,sqlC):
    # 模拟数据，表示销售记录，单条数据：时间，销售额，用户id
    userSaleLog=[
        "2017-02-01,55,1122",
        "2017-02-01,23,1133",
        "2017-02-01,15,",
        "2017-02-02,56,1155",
        "2017-02-01,78,1123",
        "2017-02-01,113,1144"
    ]
    userSaleLogRDD=sc.parallelize(userSaleLog)
    # 过滤掉没有用户id的log
    filterUserRDD=userSaleLogRDD.filter(lambda e:True if len(e.split(",")[2])>0 else False)
    # 逗号分隔的字符串转换为一行三列的数据
    rowRDD=filterUserRDD.map(lambda e:Row(e.split(",")[0],int(e.split(",")[1]),int(e.split(",")[2])))
    # 将RDD转化为data frame，并给定格式
    df=sqlC.createDataFrame(rowRDD,schema=['date','sale_amount','userid'])
    # 显示数据
    df.show()
    # 显示数据格式
    df.printSchema()
    # 按照时间来分组，分组后将销售额加和，以统计每日销售额
    df=df.groupBy("date").agg({"sale_amount":"sum"})
    # 显示数据
    df.show()
    # 显示数据格式
    df.printSchema()


if __name__=="__main__":
    conf=SparkConf().setAppName(APP_NAME).setMaster("local[*]")
    sc=SparkContext(conf=conf)
    sqlC=SQLContext(sc)
    main(sc,sqlC)