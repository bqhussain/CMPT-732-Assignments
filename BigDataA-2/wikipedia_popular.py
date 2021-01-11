from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('view count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def breakLines(line):

    separated_list=line.split()
    yield separated_list

def removeRecords(lines):
    for line in lines:
        if line[2] != "Main_Page" and (not(str(line[2]).startswith("Special:"))) and line[1] == "en":
            yield line

def returnMax(x,y):

    if x > y:
        return x
    else:
        return y

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

def kvPairs(line):
    for lines in line:
        dateTime = lines[0]
        visitCount = [int(lines[3]), lines[2]]
    return [dateTime,visitCount]

text = sc.textFile(inputs)
line = text.map(breakLines)
lineFilter = line.filter(removeRecords)
lineMapper = lineFilter.map(kvPairs)
pageViewCount = lineMapper.reduceByKey(returnMax)
outdata = pageViewCount.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)
