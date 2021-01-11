from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

#Keep only the fields subreddit, score, and author.
def extract_fields(line):
    required_keys= ["author", "score", "subreddit"]
    return {k:v for k,v in line.items() if k in required_keys}

#Keep only data from subreddits that contain an 'e' in their name ('e' in comment['subreddit']).
def check_for_e(comment):
    if("e" in comment['subreddit']):
        return comment

def get_positive_score(comment):
    if(comment['score']>0):
        return comment

def get_negative_score(comment):
    if (comment['score'] < 0):
        return comment

def main(inputs, output):
    text = sc.textFile(inputs).map(json.loads).map(extract_fields)
    comment_without_e = text.filter(check_for_e).cache()
    comment_without_e.filter(get_positive_score).map(json.dumps).saveAsTextFile(output + '/positive')
    comment_without_e.filter(get_negative_score).map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
