from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import (RandomForestClassifier,
                                       GBTClassifier,
                                       DecisionTreeClassifier)

spark = SparkSession.builder.appName('mytree').getOrCreate()

# Import the data that is already nicely formatted into label and features:
data = spark.read.format('libsvm').load('sample_libsvm_data.txt')
data.show()

# Slit the data:
train_data, test_data = data.randomSplit([0.7, 0.3])

# Use the Decision Tree Classifier model with default values:
dtc = DecisionTreeClassifier()

# Use the Random Forest Classifier model:
rfc = RandomForestClassifier(numTrees=100)

# Use the GBPT Classifier model:
gbt = GBTClassifier()

# Fit the models to the data (train them):
dtc_model = dtc.fit(train_data)
rfc_model = rfc.fit(train_data)
gbt_model = gbt.fit(train_data)

# Transform the test data to get predictions:
dtc_preds = dtc_model.transform(test_data)
rfc_preds = rfc_model.transform(test_data)
gbt_preds = gbt_model.transform(test_data)

dtc_preds.show()
rfc_preds.show()
gbt_preds.show() # this one don't have the raw prediction column

# Evaluations:
acc_eval = MulticlassClassificationEvaluator(metricName='accuracy')
print('DTC ACCURACY: ', acc_eval.evaluate(dtc_preds))
print('RFC ACCURACY: ', acc_eval.evaluate(dtc_preds))
print('GBT ACCURACY: ', acc_eval.evaluate(gbt_preds))


# Feature importance:
print(rfc_model.featureImportances)
