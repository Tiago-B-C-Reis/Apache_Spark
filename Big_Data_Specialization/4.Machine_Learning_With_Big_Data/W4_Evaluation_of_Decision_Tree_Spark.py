from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics


spark = SparkSession.builder.appName('dtc_evaluator_spark').getOrCreate()

# Import data set:
predictions = spark.read.csv('predictions.csv', inferSchema=True, header=True)
predictions.show(5)


# Compute accuracy:
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = ", accuracy)


# Convert predictions DataFrame to RDD for MulticlassMetrics
predictionAndLabels = predictions.select("prediction", "label").rdd.map(tuple)
# Create MulticlassMetrics object
metrics = MulticlassMetrics(predictionAndLabels)
# Display confusion matrix
confusionMatrix = metrics.confusionMatrix()
print("Confusion Matrix:\n", confusionMatrix)
