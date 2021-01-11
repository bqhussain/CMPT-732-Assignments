import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.functions import dayofyear

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(inputs, model_file):
    # get the data
    test_tmax = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = test_tmax.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    sql_transformer_query = 'SELECT today.latitude as latitude, today.longitude as longitude, today.elevation as elevation,' \
                'dayofyear(today.date) as dayofyear, today.tmax as tmax, yesterday.tmax AS yesterday_tmax ' \
                'FROM __THIS__ as today INNER JOIN __THIS__ as yesterday' \
                ' ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station'

    sql_transformer = SQLTransformer(statement=sql_transformer_query)
    tmax_assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation','dayofyear','yesterday_tmax'],
                                     outputCol='features')
    tmax_regressor = RandomForestRegressor(featuresCol='features',labelCol='tmax',numTrees=40, maxDepth=10, seed=123)
    #tmax_regressor = GBTRegressor(featuresCol='features', labelCol='tmax')

    reg_pipeline = Pipeline(stages=[sql_transformer, tmax_assembler, tmax_regressor])
    reg_model = reg_pipeline.fit(train)

    # use the model to make predictions
    prediction = reg_model.transform(validation)

    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
                                       metricName='r2')
    r2 = r2_evaluator.evaluate(prediction)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
                                         metricName='rmse')
    rmse = rmse_evaluator.evaluate(prediction)

    print('r2 =', r2)
    print('rmse =', rmse)
    #print(reg_model.stages[-1].featureImportances)

    reg_model.write().overwrite().save(model_file)

if __name__ == '__main__':

    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs, model_file)
