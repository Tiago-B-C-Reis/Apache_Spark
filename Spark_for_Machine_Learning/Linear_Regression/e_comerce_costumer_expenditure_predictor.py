from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression

# A few things we need to do before Spark can accept the data!
# It needs to be in the form of two columns
# ("label","features")
# Import VectorAssembler and Vectors
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


# Initiate Spark session:
spark = SparkSession.builder.appName('lr_example').getOrCreate()

# Uploading data:
data = spark.read.csv("Ecommerce_Customers.csv", inferSchema=True, header=True)
data.printSchema()
data.show()
data.head()
for item in data.head():
    print(item)
print(data.columns)

# Input the columns values and aggregate them to form a vector that is the output features
# hat we need to feed on the mode:
assembler = VectorAssembler(
    inputCols=["Avg Session Length", "Time on App",
               "Time on Website", 'Length of Membership'],
    outputCol="features")

output = assembler.transform(data)
output.select('features').show()

# Create the final usable data for the model:
final_data = output.select('features', 'Yearly Amount Spent')
final_data.show()

# Split the data:
train_data, test_data = final_data.randomSplit([0.7, 0.3])
train_data.describe().show()
test_data.describe().show()

# Model set:
lr = LinearRegression(featuresCol='features',
                      labelCol='Yearly Amount Spent',
                      predictionCol='prediction')
lr_model = lr.fit(train_data)

# Evaluate the model on the test set:
test_results = lr_model.evaluate(test_data)
test_results.residuals.show()

# This is the 'Root Mean Square Error'
# one of the regression evaluation models (used for continuous values - lesson 37:
print(test_results.rootMeanSquaredError) # result is 10.332
print(test_results.r2) # result of 0.9824, which means that the trained
# model is feting very well on the test set

final_data.describe().show()
# if we compare the mean of 499.31 with a std of 79.31,
# having and error on the model of 10.33 is already good.


# Deploy the model on unlabeled data:
unlabeled_data = test_data.select('features')
unlabeled_data.show()
# Predict on the features of the test set without the labels:
predictions = lr_model.transform(unlabeled_data)
predictions.show()






