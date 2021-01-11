import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
import datetime

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

station = 'sfu_lab'
date_13 = datetime.date(2020, 11, 13)
date_14 = datetime.date(2020, 11, 14)
lab_lat = 49.2771
lab_long = -122.9146
lab_elevation = 330.0
due_temp = 12.0
after_temp = 13.0


def test_model(model_file):

    model = PipelineModel.load(model_file)

    input_columns = [(station,date_13,lab_lat,lab_long,lab_elevation,due_temp),
                     (station,date_14,lab_lat,lab_long,lab_elevation,after_temp)
                     ]
    prediction_df = spark.createDataFrame(input_columns,schema=tmax_schema)

    predictions = model.transform(prediction_df)
    predictions.show()

    prediction = predictions.collect()
    prediction = prediction[0]['prediction']
    print(f'Predicted tmax for {date_14} : {prediction}')

if __name__ == '__main__':

    model_file = sys.argv[1]
    test_model(model_file)
