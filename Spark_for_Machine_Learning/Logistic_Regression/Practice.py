from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)

spark = SparkSession.builder.appName('mylogreg').getOrCreate()

# Load training data (not split yet)
my_data = spark.read.format('libsvm').load('sample_libsvm_data.txt')
my_data.show()

my_log_reg_model = LogisticRegression()

# Fit the model
fitted_logreg = my_log_reg_model.fit(my_data)
log_summary = fitted_logreg.summary
print(log_summary.predictions)
log_summary.predictions.show()

# Data split into train and test set.
lr_train, lr_test = my_data.randomSplit([0.7, 0.3])

# Model fit.
final_model = LogisticRegression()
fit_final = final_model.fit(lr_train)

# Evaluate on test set.
prediction_and_labels = fit_final.evaluate(lr_test)
prediction_and_labels.predictions.show()

# Evaluators.
my_eval = BinaryClassificationEvaluator()
my_final_roc = my_eval.evaluate(prediction_and_labels.predictions)
# The result of this shows the precision that we are getting:
print(my_final_roc)


