import sys
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster
cluster_seeds = ['199.60.17.103', '199.60.17.105']

def main(keyspace,output_keyspace):

    orders_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="orders", keyspace=keyspace).load()
    orders_df.createOrReplaceTempView('orders')
    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="lineitem", keyspace=keyspace).load()
    lineitem_df.createOrReplaceTempView('lineitem')
    parts_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="part", keyspace=keyspace).load()
    parts_df.createOrReplaceTempView('part')

    joined_tables = spark.sql('''SELECT o.*, p.name from orders o 
                               JOIN lineitem l ON o.orderkey = l.orderkey 
                               JOIN part p ON l.partkey = p.partkey '''
                              )

    output_summary_table = joined_tables.groupBy('orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate',
	                                       'order_priority', 'clerk', 'ship_priority', 'comment').agg(
        functions.collect_set('name').alias('part_names')).sort('orderkey')
    input_table = 'orders_parts'
    output_summary_table.write.format("org.apache.spark.sql.cassandra").options(table=input_table, keyspace=output_keyspace).save()


if __name__ == '__main__':
    input_keyspace = sys.argv[1]
    output_keyspace = sys.argv[2]
    spark = SparkSession.builder.appName('Tpch Denormalize') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds))\
        .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_keyspace,output_keyspace)

