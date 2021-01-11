import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types

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
    weather.createOrReplaceTempView("weather")

    corrected_data = spark.sql("SELECT station, weather.date, observation, (weather.value/10) AS val "
                               "FROM weather WHERE qflag IS NULL").cache()
    corrected_data.createOrReplaceTempView("corrected_data")

    tmax_df = spark.sql("SELECT station, corrected_data.date AS tmax_date, val AS tmax "
                        "FROM corrected_data WHERE observation=='TMAX' ").cache()
    tmax_df.createOrReplaceTempView('tmax_df')

    tmin_df  = spark.sql("SELECT station, corrected_data.date AS tmin_date , val AS tmin"
                         " FROM corrected_data WHERE observation=='TMIN' ").cache()
    tmin_df .createOrReplaceTempView('tmin_df ')

    #join tmax with tmin on the above conditions and get separate column or tmax and tmin
    tmax_joined_tmin = spark.sql("SELECT tmax_df.station,tmax_df.tmax_date ,tmax_df.tmax,tmin_df.tmin FROM tmax_df "
                                 "JOIN tmin_df ON (tmax_df.station = tmin_df.station and tmax_df.tmax_date = tmin_df.tmin_date)")
    tmax_joined_tmin.createOrReplaceTempView('tmax_joined_tmin')

    #minus tmin from tmax to get the ranged df
    ranged_df= spark.sql("SELECT tmax_joined_tmin.station, tmax_joined_tmin.tmax_date, (tmax_joined_tmin.tmax - tmax_joined_tmin.tmin) AS range "
                         "FROM tmax_joined_tmin ").cache()
    ranged_df.createOrReplaceTempView('ranged_df')

    #find the maximum range.
    max_range_df= spark.sql("SELECT ranged_df.tmax_date AS max_date, MAX(ranged_df.range) AS max_range FROM ranged_df GROUP BY ranged_df.tmax_date")
    max_range_df.createOrReplaceTempView('max_range_df')

    #join to get the station with maximum range.
    station_range_df = spark.sql("SELECT ranged_df.tmax_date AS date,  ranged_df.station, ROUND(ranged_df.range,3) AS range FROM ranged_df "
                                 "JOIN max_range_df ON (ranged_df.tmax_date = max_range_df.max_date and ranged_df.range = max_range_df.max_range) "
                                 "ORDER BY ranged_df.tmax_date, ranged_df.station")

    station_range_df.coalesce(1).write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range sql').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)

