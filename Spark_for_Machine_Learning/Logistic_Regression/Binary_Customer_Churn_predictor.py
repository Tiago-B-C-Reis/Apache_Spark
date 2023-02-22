from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import (VectorAssembler, VectorIndexer,
                                OneHotEncoder, StringIndexer)
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName('bccp_logreg').getOrCreate()

# Load training data (not split yet)
data = spark.read.csv("customer_churn.csv",
                      inferSchema=True, header=True)


# Show basic info about the data set
data.show()
for row in data.columns:
    print(row)
data.describe().show()
data.printSchema()


# Assemble all the columns info that we need to have as features in one vector (column).
assembler = VectorAssembler(inputCols=['Age', 'Total_Purchase', 'Account_Manager',
                                       'Years', 'Num_Sites'],
                            outputCol='features')

# Set the final data set, ready to be feed onto the model.
output = assembler.transform(data)
final_data = output.select('features', 'churn')

# Train and Test set creation.
train_data, test_data = final_data.randomSplit([0.7, 0.3])


# Implement and fit the model.
log_reg_churn = LogisticRegression(featuresCol='features',
                                   labelCol='churn', maxIter=100)
fitted_churn_model = log_reg_churn.fit(train_data)
training_sum = fitted_churn_model.summary
training_sum.predictions.show()
training_sum.predictions.describe().show()


# Evaluate results
pred_and_labels = fitted_churn_model.evaluate(test_data)
pred_and_labels.predictions.show()
pred_and_labels.predictions.describe().show()

churn_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',
                                           labelCol='churn')


# Use the evaluater AUC.
AUC = churn_eval.evaluate(pred_and_labels.predictions)
print(AUC)


# Predict on brand new unlabeled data.
# Fit all the available data from 'customer_churn' into the model.
final_lr_model = log_reg_churn.fit(final_data)

new_data = spark.read.csv("new_customers.csv", inferSchema=True, header=True)
new_data.show()
new_data.printSchema()

test_new_data = assembler.transform(new_data)
test_new_data.describe().show()
test_new_data.printSchema()

final_results = final_lr_model.transform(test_new_data)
# This new data set has only 6 new clients and the line below
# gives predictions for each one of them.
final_results.select('Company', 'prediction').show()
