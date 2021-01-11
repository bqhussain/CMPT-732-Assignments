import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

def get_kv_pairs(node_list):

    node_edges = node_list.split(":")
    node = int(node_edges[0])
    remove_white_spaces = node_edges[1].split()
    edges = [int(x) for x in remove_white_spaces]
    return (node, edges)

def mapping(x):

    nodes = x[1][0]
    for n in nodes:
     yield n,(x[0],x[1][1][1]+1)

def main(inputs,output,source,destination):

    list_of_nodes = sc.textFile(inputs+ '/links-simple-sorted.txt').map(get_kv_pairs).cache()
    source_node = sc.parallelize([(source, ['-', 0])])
    path_to_dest =[destination]

    for i in range(6):
        #[(1, ([3, 5], ('-', 0)))]
        joined_rdd= list_of_nodes.join(source_node).flatMap(mapping)
        source_node = source_node.union(joined_rdd).reduceByKey(lambda x,y : x if x[1]<=y[1] else y).sortByKey()
        source_node.coalesce(1).saveAsTextFile(output + '/iter-' + str(i))
        while destination != source and destination != " ":
            constructing_path = source_node.lookup(destination)
            if len(constructing_path)!= 0:
                destination = constructing_path[0][0]
                path_to_dest.append(destination)
            else:
                break

    path_to_dest.reverse()
    if len(path_to_dest)==1:

        print(f'Path does not exist between {source} and {destination}')
        finalpath = sc.parallelize(["NO","PATH","FOUND"])
        finalpath.coalesce(1).saveAsTextFile(output + '/path')

    else:

        finalpath = sc.parallelize(path_to_dest)
        finalpath.coalesce(1).saveAsTextFile(output + '/path')

if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    spark = SparkSession.builder.appName('shortest path').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,outputs,int(source),int(destination))


