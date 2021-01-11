from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def extract_fields(pythonDict):

    subreddit = pythonDict["subreddit"]
    countScore = (1.0, float(pythonDict["score"]))
    return (subreddit, countScore)
    # (subreddit,(1.0, score))

def add_pairs(x, y):

    return (x[0]+y[0], x[1]+y[1])
    # (total count, sum_of_score)

def getAvg(line):

    subreddit_average = (line[0], line[1][1]/line[1][0])
    return subreddit_average
    # (subreddit, average)

def get_relative_score(joined_rdd,broadcast_object):

    author = joined_rdd[1][1]
    subreddit_score = float(joined_rdd[1][0])
    subreddit_average = (broadcast_object.value[joined_rdd[0]])
    rel_score = (subreddit_score/subreddit_average)
    return (author,rel_score)

def main(inputs, output):

    text = sc.textFile(inputs).map(json.loads).cache()
    line = text.map(extract_fields).reduceByKey(add_pairs)
    get_average = line.map(getAvg).filter(lambda i: i[1] >= 0) #check if average > 0
    dict_averages = sc.broadcast(dict(get_average.collect())) #broadcast object creation
    commentbysub = text.map(lambda c: (c['subreddit'], (c['score'], c['author'])))
    relative_RDD = commentbysub.map(lambda i: get_relative_score(i, dict_averages)) # to get relative scores
    sorted_RDD= relative_RDD.sortBy(lambda i: i[1], ascending=False)# sorting by keys in accending order
    sorted_RDD.map(json.dumps).saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('relative bcast score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

