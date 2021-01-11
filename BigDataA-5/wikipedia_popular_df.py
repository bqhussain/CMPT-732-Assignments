import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types



@functions.udf(returnType=types.StringType())
def path_to_hour(path):

    complete_path = path.split("/")
    return complete_path[-1][11:22]

def main(inputs, output):

    wikipedia_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.LongType()),
        types.StructField('page_size', types.LongType()),

    ])

    wiki_dframe = spark.read.csv(inputs, sep=" ", schema=wikipedia_schema).withColumn('hour', functions.input_file_name())

    wiki_filtered = wiki_dframe.filter(wiki_dframe.language == 'en').filter(wiki_dframe.title != 'Main_Page')\
        .filter(~wiki_dframe.title.startswith('Special:'))

    wiki_filtered = wiki_filtered.withColumn('hour', path_to_hour(wiki_dframe['hour'])).cache()

    wiki_max_views = wiki_filtered.groupBy('hour').agg(functions.max('views').alias('max_views')).withColumnRenamed('hour','hours').cache()

    joined_wiki_df = wiki_filtered.join(functions.broadcast(wiki_max_views),[wiki_max_views['max_views'] == wiki_filtered['views'],
                                         wiki_max_views.hours == wiki_filtered.hour])

    joined_wiki_df = joined_wiki_df.select(wiki_filtered.hour, 'title', 'views').sort('hour', 'title')
    joined_wiki_df.explain()
    joined_wiki_df.coalesce(1).write.json(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)