from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression

# Initiate Spark session:
spark = SparkSession.builder.appName('lrex').getOrCreate()

# Uploading data:
training = spark.read.format('libsvm').load('sample_linear_regression_data.txt')

# Model set:
lr = LinearRegression(featuresCol='features', labelCol='label', predictionCol='prediction')

# Model without train/dev split:
lrModel = lr.fit(training)
# Print the coefficients and intercept for linear regression
print("Coefficients: {}".format(str(lrModel.coefficients))) # For each feature...
print('\n')
print("Intercept:{}".format(str(lrModel.intercept)))
training_summary = lrModel.summary
print(training_summary.rootMeanSquaredError)


# Input the data and split it randomly into train/test set
all_data = training = spark.read.format('libsvm').load('sample_linear_regression_data.txt')
train_data, test_data = all_data.randomSplit([0.7, 0.3])
train_data.describe().show()
test_data.describe().show()

correct_model = lr.fit(train_data)
test_results = correct_model.evaluate(test_data)
test_results.residuals.show()
print(test_results.rootMeanSquaredError)


# Simulation of a prediction with this trained model:
unlabeled_data = test_data.select('features')
unlabeled_data.show()

predictions = correct_model.transform(unlabeled_data)
predictions.show()

