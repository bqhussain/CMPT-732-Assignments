import sys
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, functions

cluster_seeds = ['199.60.17.103', '199.60.17.105']

def output_line(order_summary):
    orderkey= order_summary[0]
    totalprice = order_summary[1]
    item_names = order_summary[2]
    namestr = ', '.join(sorted(list(item_names)))
    return 'Order #%d $%.2f: %s' % (orderkey, totalprice, namestr)

def main(keyspace,outdir,orderkeys):

    orders_parts_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="orders_parts", keyspace=keyspace).load()
    output_summary_table = orders_parts_df.select('orderkey','totalprice','part_names')\
        .filter(orders_parts_df['orderkey'].isin(orderkeys)).sort('orderkey')
    output_rdd = output_summary_table.rdd
    output_rdd= output_rdd.map(output_line)
    output_rdd.coalesce(1).saveAsTextFile(outdir)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    spark = SparkSession.builder.appName('Tpch Orders Denorm') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds))\
        .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace,outdir,orderkeys)

