import sys
import re
import uuid
from pyspark.sql import SparkSession, types
assert sys.version_info >= (3, 5)
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from datetime import datetime

cluster_seeds = ['199.60.17.103', '199.60.17.105']
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def clean_logs(logs):

    if line_re.match(logs):
        return True

def disassemble_logs(logs):

    log = line_re.split(logs)
    host = log[1]
    id_ = str(uuid.uuid4())
    dateTime = datetime.strptime(log[2], "%d/%b/%Y:%H:%M:%S")
    path = log[3]
    byte = int(log[4])
    return [host,id_,dateTime,path,byte]

def main(inputs,keyspace,table):

    nasa_logs = sc.textFile(inputs).repartition(150)
    nasa_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('id', types.StringType()),
        types.StructField('datetime', types.TimestampType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.IntegerType()),
    ])

    nasa_logs = nasa_logs.filter(clean_logs).map(disassemble_logs)
    nasa_logs_df = spark.createDataFrame(nasa_logs,nasa_schema)
    cluster = Cluster(cluster_seeds)
    session = cluster.connect(keyspace)
    session.execute('TRUNCATE ' + table_name)
    nasa_logs_df.write.format("org.apache.spark.sql.cassandra") .options(table=table, keyspace=keyspace).save()



if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    spark = SparkSession.builder.appName('Spark Load Logs') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_dir,keyspace,table_name)

