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

## Main functionality
logging.basicConfig(level=logging.INFO,format="%(message)s")
inverted_index={}
doc_id=-1
_words=[]
def init(sc):
    global inverted_index
    global doc_id
    inputfiles=os.listdir("shakespeare")
    for fname in inputfiles:
        logging.info("@filename %s"% fname)
        file=sc.textFile("shakespeare/"+fname)
        _file=file.map(normalize)
        words=_file.flatMap(lambda str:str.split())
        doc_id=fname[:-4]
        words.foreach(build)
        logging.info("@inverted_index "%inverted_index)
    print _words
    fp=open("inverted_index.txt","w")
    fp.write(str(inverted_index))
    fp.close()
    logging.info("@inverted_index has been saved in inverted_index.txt")


def build(word):
    global inverted_index
    global doc_id
    global _words
    word=word.lower()
    print word
    _words.append(word)
    #print doc_id
    if inverted_index.has_key(word):
        if doc_id not in inverted_index[word]:
            inverted_index[word].append(doc_id)
    else:
        inverted_index[word] = [doc_id]
    print inverted_index


def normalize(line):
    _line=line
    for i in string.punctuation:
        _line=_line.replace(i," ")
    _line=_line.replace("\n"," ")
    _line = _line.replace("\t", " ")
    _line = _line.replace("\x0d\x0a", " ")
    return _line

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    init(sc)