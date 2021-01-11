from pyspark import SparkConf, SparkContext
import sys
import random
import operator

def finding_euler(sample):
    total_iterations = 0
    random.seed()
    for samples in range(sample):
        sum = 0.0
        while sum < 1:
            sum += random.random()
            total_iterations+=1
    return total_iterations

def main(samples):
    num_partitions = 28
    sample_per_batch = [int(samples/num_partitions) for i in range(num_partitions)]
    sample_rdd = sc.parallelize(sample_per_batch, numSlices=num_partitions)
    getting_euler = sample_rdd.map(finding_euler).reduce(operator.add)
    print("Euler's Constant: ", getting_euler/samples)

if __name__ == '__main__':
    conf = SparkConf().setAppName('eulers calculation')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = int(sys.argv[1])
    main(inputs)