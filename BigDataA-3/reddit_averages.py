from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def breaklines(line):
    pythonDict = json.loads(line)
    subreddit = pythonDict["subreddit"]
    countScore = (1.0, float(pythonDict["score"]))
    return (subreddit, countScore)

def add_pairs(x, y):

    return (x[0]+y[0], x[1]+y[1])

def getAvg(line):
    list = [line[0], line[1][1]/line[1][0]]
    return json.dumps(list)

def main(inputs, output):
    text = sc.textFile(inputs)
    line = text.map(breaklines).reduceByKey(add_pairs)
    reducing = line.map(getAvg)
    reducing.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
