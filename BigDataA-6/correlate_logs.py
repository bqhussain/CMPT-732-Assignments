import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from math import sqrt

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
def clean_logs(logs):

    if line_re.match(logs):
        return True

def disassemble_logs(logs):

    log = line_re.split(logs)
    return [log[1],1,int(log[4])]

def main(inputs):

    nasa_logs = sc.textFile(inputs)
    nasa_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('visits', types.IntegerType()),
        types.StructField('bytes', types.IntegerType()),
    ])

    nasa_logs = nasa_logs.filter(clean_logs).map(disassemble_logs)
    nasa_logs_df = spark.createDataFrame(nasa_logs, nasa_schema)
    nasa_logs_df = nasa_logs_df.groupBy('host').agg(functions.sum('visits').alias('x'),functions.sum('bytes').alias('y'))
    nasa_logs_df_total = nasa_logs_df.withColumn('n',functions.lit(1)).withColumn('x_squared',nasa_logs_df.x **2)\
        .withColumn('y_squared',nasa_logs_df.y **2).withColumn('xy',(nasa_logs_df.x * nasa_logs_df.y))
    summation_df = nasa_logs_df_total.select(functions.sum('n'),functions.sum('x'),functions.sum('y'),functions.sum('x_squared'),functions.sum('y_squared'),functions.sum('xy'))
    sigma_list=summation_df.collect()[0]
    #            0          1           2               3                       4                               5
    #Row(sum(n)=232, sum(x)=1972, sum(y)=36133736, sum(x_squared)=32560.0, sum(y_squared)=25731257461526.0, sum(xy)=662179733)
    n = sigma_list[0]
    numerator_term_one = n*sigma_list[5]
    numerator_term_two = sigma_list[1] * sigma_list[2]
    numerator = numerator_term_one-numerator_term_two
    denominator_term_one = sqrt((n*sigma_list[3]) - (sigma_list[1]**2))
    denominator_term_two = sqrt((n*sigma_list[4]) - (sigma_list[2]**2))
    denominator = denominator_term_one * denominator_term_two
    r = numerator/denominator
    r_squared = r**2
    print("Value of r: ", r)
    print("Value of r_Squared: ", r_squared)

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)

