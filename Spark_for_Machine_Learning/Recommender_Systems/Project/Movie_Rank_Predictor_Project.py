from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName('rec').getOrCreate()

# Import the data that is already nicely formatted into label and features:
data = spark.read.csv("movielens_ratings.csv",
                      inferSchema=True, header=True)
data.show()
data.describe().show()

# Split the data set into train and test set:
train, test = data.randomSplit([0.8, 0.2])

# Run the "collaborate filtering" model (Alternating Least Squares):
als = ALS(maxIter=5, regParam=0.01, userCol='userId',
          itemCol='movieId', ratingCol='rating')
model = als.fit(train)

# Run predictions for the test set:
predictions = model.transform(test)
predictions.show()

# Evaluate:
evaluator = RegressionEvaluator(metricName='rmse',
                                labelCol='rating',
                                predictionCol='prediction')
# rout mean square error
rmse = evaluator.evaluate(predictions)
print('RMSE: \n', rmse)


# Predict how user 11 is going to rank some movies after it already had ranked others:
single_user = test.filter(test['userId'] == 11).select(['movieId', 'userId'])
single_user.show()

recommendations = model.transform(single_user)
recommendations.orderBy('prediction', ascending=False).show()
