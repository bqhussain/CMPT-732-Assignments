import sys
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, functions

cluster_seeds = ['199.60.17.103', '199.60.17.105']

def output_line(order_summary):
    orderkey= order_summary['orderkey']
    totalprice = order_summary['totalprice']
    item_names = order_summary['item_names']
    namestr = ', '.join(sorted(list(item_names)))
    return 'Order #%d $%.2f: %s' % (orderkey, totalprice, namestr)

def main(keyspace,outdir,orderkeys):

    orderkeys = tuple(orderkeys)
    orders_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="orders", keyspace=keyspace).load()
    orders_df.createOrReplaceTempView('orders')
    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="lineitem", keyspace=keyspace).load()
    lineitem_df.createOrReplaceTempView('lineitem')
    parts_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="part", keyspace=keyspace).load()
    parts_df.createOrReplaceTempView('part')

    joined_tables = spark.sql('''SELECT o.orderkey, o.totalprice, p.name from orders o 
                             JOIN lineitem l ON o.orderkey = l.orderkey 
                             JOIN part p ON l.partkey = p.partkey WHERE o.orderkey in '''
                             + str(orderkeys))

    output_summary_table = joined_tables.groupBy('orderkey', 'totalprice').agg(functions.collect_set('name').alias('item_names')).sort('orderkey')
   
    # output_summary_table.explain()
    output_rdd = output_summary_table.rdd
    output_rdd= output_rdd.map(output_line)
    output_rdd.coalesce(1).saveAsTextFile(outdir)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    spark = SparkSession.builder.appName('Tpch Orders DF') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds))\
        .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace,outdir,orderkeys)

