from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)
from pyspark.ml.classification import (DecisionTreeClassifier,
                                       GBTClassifier,
                                       RandomForestClassifier)

spark = SparkSession.builder.appName('tree_methods').getOrCreate()

# Import the data:
data = spark.read.csv("College.csv",
                      inferSchema=True, header=True)
data.show()
data.printSchema()
data.describe().show()

print(data.columns)

# Create a vector per row and join them as the "features" column:
assembler = VectorAssembler(inputCols=['Apps', 'Accept', 'Enroll', 'Top10perc',
                                       'Top25perc', 'F_Undergrad', 'P_Undergrad',
                                       'Outstate', 'Room_Board', 'Books', 'Personal',
                                       'PhD', 'Terminal', 'S_F_Ratio', 'perc_alumni',
                                       'Expend', 'Grad_Rate'], outputCol='features')

# Join the "features" column into the "data" table:
output = assembler.transform(data)
output.show()

# The "Private" column is filed with strings ('yes' or 'no'), but we need "Int", so this does the work:
indexer = StringIndexer(inputCol='Private', outputCol='PrivateIndex')
# If we need to do this in more than one column, then it should be used a pipeline.
# Pipeline it's used when the code is going to be repeated a lot.
output_fixed = indexer.fit(output).transform(output)
output_fixed.show()

# Create the final nice and usable data to feed the model:
final_data = output_fixed.select('features', 'PrivateIndex')
# Split it into train + test set:
train_data, test_data = final_data.randomSplit([0.7, 0.3])


# Use the models (just using the defaults arguments):
dtc = DecisionTreeClassifier(labelCol='PrivateIndex', featuresCol='features')
gbt = GBTClassifier(labelCol='PrivateIndex', featuresCol='features')
# Here we change one parameter out of default and see the improvement:
rfc = RandomForestClassifier(labelCol='PrivateIndex',
                             featuresCol='features',
                             numTrees=150)

# Train the models:
dtc_model = dtc.fit(train_data)
rfc_model = rfc.fit(train_data)
gbt_model = gbt.fit(train_data)

# Transform the test data to get predictions:
dtc_preds = dtc_model.transform(test_data)
rfc_preds = rfc_model.transform(test_data)
gbt_preds = gbt_model.transform(test_data)

# Run Evaluations ('area under the curve'):
my_binary_eval = BinaryClassificationEvaluator(labelCol='PrivateIndex')
print('DTC ACCURACY: ', my_binary_eval.evaluate(dtc_preds))
print('RFC ACCURACY: ', my_binary_eval.evaluate(rfc_preds))
print('GBT ACCURACY: ', my_binary_eval.evaluate(gbt_preds))

# ------------------------------------------------------------------------------------------------------
# In the lecture the author says that:
rfc_preds.printSchema()
# the gbt doesn't have the 'rawPrediction:' and 'probability',
# so he does the following to get it done:
gbt_preds.printSchema()
my_binary_eval2 = BinaryClassificationEvaluator(labelCol='PrivateIndex',
                                                rawPredictionCol='prediction')
# But it looks like in this Apache Spark latest version it GBT works on "my_binary_eval"
# without the need to this "my_binary_eval2" thing, and it performs better, so I will ignore this part.
print('GBT ACCURACY: ', my_binary_eval2.evaluate(gbt_preds))
# ------------------------------------------------------------------------------------------------------


# Run evaluations using the multi classificator:
acc_eval = MulticlassClassificationEvaluator(labelCol='PrivateIndex',
                                             metricName='accuracy')
rfc_acc = acc_eval.evaluate(rfc_preds)
print(rfc_acc)
