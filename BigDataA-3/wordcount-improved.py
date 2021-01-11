from pyspark import SparkConf, SparkContext
import sys
import re, string, operator


wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
def words_once(line):
    for w in wordsep.split(line):
        yield (w.lower(), 1)

def removeSpace(words):
    if words[0] != "":
        return words

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    text = sc.textFile(inputs).repartition(40)
    words = text.flatMap(words_once).filter(removeSpace)
    wordcount = words.reduceByKey(operator.add)
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)