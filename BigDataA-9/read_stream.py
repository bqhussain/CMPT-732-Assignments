import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    x_y_split = functions.split(values['value'], ' ')
    values = values.withColumn('n',functions.lit(1))
    values = values.withColumn('x', x_y_split.getItem(0))
    values = values.withColumn('y', x_y_split.getItem(1))
    values = values.withColumn('xy', values['x']*values['y'])
    values = values.withColumn('x_squared', values['x']**2)
    summation_df = values.select(functions.sum('n').alias('sigma_n'),functions.sum('x').alias('sigma_x'),functions.sum('y').alias('sigma_y'),
                                 functions.sum('x_squared').alias('sigma_x^2'),functions.sum('xy').alias('sigma_xy'))
    beta_df = summation_df.withColumn('BETA',
                                      (summation_df['sigma_xy'] - ((summation_df['sigma_x']*summation_df['sigma_y'])/summation_df['sigma_n']))
                                      /
                                      (summation_df['sigma_x^2']-((summation_df['sigma_x']**2)/summation_df['sigma_n']))
                                      )
    alpha_df = beta_df.withColumn('ALPHA', ((beta_df['sigma_y']/beta_df['sigma_n'])-(beta_df['BETA']*(beta_df['sigma_x']/beta_df['sigma_n']))))
    beta_alpha = alpha_df.select(alpha_df['BETA'], alpha_df['ALPHA'])
    stream = beta_alpha.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(600)
    stream.stop()

if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('Read Stream').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(topic)