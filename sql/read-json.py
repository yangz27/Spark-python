from pyspark import SparkConf,SparkContext,HiveContext

APP_NAME="read-json"

def main(sc,sqlC):
    df=sqlC.read.json("./data.json")
    df.show()
    df.printSchema()
    df.select("age").show()
    # rdd的transformation
    df.filter(df["age"]>20).show()

    # 可选格式:json,orc,parquet
    df.write.format("orc").saveAsTable("people",mode="overwrite")
    df.groupBy("age").count().show()

if __name__=="__main__":
    conf=SparkConf().setAppName(APP_NAME)
    conf=conf.setMaster("local[*]")
    sc=SparkContext(conf=conf)
    sqlC=HiveContext(sc)

    main(sc,sqlC)