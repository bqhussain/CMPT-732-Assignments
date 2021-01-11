from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    line = wordsep.split(line)
    for w in line:
        # if w!="":
            yield (w.lower(), 1)

def removeSpace(words):
    if words[0] != "":
        return words

def add(x, y):
    print(x,y)
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordFilter = words.filter(removeSpace)
wordcount = wordFilter.reduceByKey(add)
outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)