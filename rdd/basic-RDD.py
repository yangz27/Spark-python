## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext

## Module Constants
APP_NAME = "basic function in RDD"

## Closure Functions

## Main functionality
def main(sc):
    nums=sc.parallelize([1,2,3,4,5,6])
    for num in nums.take(4):
        print num

    words=sc.textFile("data.txt")
    print words.first()

    print "after map"
    new_nums=nums.map(lambda x:x*2)
    for num in new_nums.take(4):
        print num

    print "after flatmap"
    s=sc.parallelize(['i love office','you hate study'])
    new_s=s.flatMap(lambda str:str.split())
    for str in new_s.take(4):
        print str



    print "after filter"
    new_str_2=s.filter(lambda line:'y' in line)
    print new_str_2.first()

    num1=sc.parallelize([1,2,3])
    num2=sc.parallelize([4,5,6])
    num3=num1.union(num2)
    print "after union"
    for num in num3.take(6):
        print num

    print "count=",num3.count()

    new_nums3=num3.collect()
    print "collect=",new_nums3

    nums4=sc.parallelize([1,2,3,3,4])
    print "before distinct=",nums4.count()
    nums5=nums4.distinct()
    print "after fistinct=",nums5.count()

    nums5=nums4.intersection(num1)
    print "after intersection"
    for num in nums5.take(4):
        print num

    nums5=nums4.subtract(num1)
    print "after subtract"
    for num in nums5.take(4):
        print num

    nums5=nums4.cartesian(num1)
    print "after cartesian"
    for num in nums5.take(20):
        print num

    print "after reduce"
    print nums4.reduce(lambda x,y:x+y)

    print "after aggregate"
    sumCount=nums4.aggregate((0,0),(lambda acc,value:(acc[0]+value,acc[1]+1)),(lambda acc1,acc2:(acc1[0]+acc2[0],acc1[1]+acc2[1])))
    print sumCount

    print "after top"
    print nums4.top(2)

    print "foreach"
    print nums4.foreach(add)

def add(x):
    print x," and ",x+2

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)