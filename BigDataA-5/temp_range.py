import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    weather = spark.read.csv(inputs, schema=observation_schema)
    corrected_data = weather.filter(weather['qflag'].isNull())

    #get tmax dataframe
    tmax_df = corrected_data.filter(weather['observation'] == 'TMAX').\
        select('station','date', (corrected_data['value'] / 10).alias('tmax')).cache()

    #get tmin dataframe
    tmin_df = corrected_data.filter(weather['observation'] == 'TMIN').\
        select('station','date', (corrected_data['value'] / 10).alias('tmin')).cache()

    #conditions for the join
    join_condition=[tmax_df.station == tmin_df.station,tmax_df.date == tmin_df.date]

    #join tmax with tmin on the above conditions and get separate column or tmax and tmin
    tmax_joined_tmin = tmax_df.join(tmin_df,join_condition).select(tmax_df.station,tmax_df.date,'tmax','tmin')
    # station | data | tmax | tmin

    #minus tmin from tmax to get the ranged df
    ranged_df = tmax_joined_tmin.select(tmax_joined_tmin.station, tmax_joined_tmin.date, (tmax_joined_tmin.tmax - tmax_joined_tmin.tmin).
                                        alias('range')).cache()
    # station | date | range

    #find the maximum range.
    max_range_df = ranged_df.groupBy('date').agg(functions.max('range').alias('max_range')).withColumnRenamed('date','max_date')
    # max_date | max_range

    join_condition= [ranged_df.date == max_range_df.max_date , ranged_df.range == max_range_df.max_range ]
    #join to get the station with maximum range.
    station_range_df = ranged_df.join(max_range_df,join_condition).select('date','station', functions.format_number('range',2))
    # date | station | range

    station_range_df.orderBy('date','station').write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)