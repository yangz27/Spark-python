## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
import logging
from operator import add
import os
import string

## Module Constants
APP_NAME = "IR in Spark"

## Closure Functions
def normalize(line):
    _line=line
    for i in string.punctuation:
        _line=_line.replace(i," ")
    _line=_line.replace("\n"," ")
    _line = _line.replace("\t", " ")
    _line = _line.replace("\x0d\x0a", " ")
    return _line

## Main functionality
class Count:
    num_of_token=0
    num_of_doc=0
    num_of_term=0
    average_token_of_doc=0.0
    @classmethod
    def toString(self):
        logging.info("# counting:")
        logging.info("@the number of tokens is %s" % Count.num_of_token)
        logging.info("@the number of docs is %s" % Count.num_of_doc)
        logging.info("@the number of terms is %s" % Count.num_of_term)
        logging.info("@the average token number of docs is %s" % Count.average_token_of_doc)

logging.basicConfig(level=logging.INFO,format="%(message)s")
inverted_index={}
doc_id=-1
def init(sc):
    global inverted_index
    global doc_id
    inputfiles=os.listdir("shakespeare")
    local_index={}
    for fname in inputfiles:
        logging.info("@filename %s"% fname)
        file=sc.textFile("shakespeare/"+fname)
        # count docs
        Count.num_of_doc+=1
        # normalize and count
        _file=file.map(normalize)
        doc_id=fname[:-4]
        words_count=_file.flatMap(lambda str:str.split()).map(lambda word:(word.lower(),1)).reduceByKey(add)
        # count tokens
        Count.num_of_token=words_count.count()
        # build inverted index
        for (k,v) in words_count.collect():
            _k=k.encode('utf8')
            if inverted_index.has_key(_k):
                inverted_index[_k].append(doc_id)
            else:
                inverted_index[_k]=[doc_id]
    # write into files
    logging.info("@inverted_index %s"%inverted_index)
    fp=open("inverted_index.txt","w")
    fp.write(str(inverted_index))
    fp.close()
    logging.info("@inverted_index has been saved in inverted_index.txt")

    # count terms
    Count.num_of_term=len(inverted_index.keys())
    Count.average_token_of_doc=Count.num_of_token/Count.num_of_doc

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    init(sc)
    Count.toString()

    sc.stop()
