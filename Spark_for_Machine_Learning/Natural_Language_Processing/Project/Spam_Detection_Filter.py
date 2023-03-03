from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import (MulticlassClassificationEvaluator,
                                   BinaryClassificationEvaluator)
from pyspark.ml.classification import (NaiveBayes,
                                       GBTClassifier,
                                       RandomForestClassifier,
                                       LogisticRegression)
from pyspark.ml.feature import (IDF,
                                CountVectorizer,
                                Tokenizer,
                                StopWordsRemover,
                                StringIndexer,
                                VectorAssembler)


# ---------------- Initiate session and load the raw data: ----------------
spark = SparkSession.builder.appName('nlp').getOrCreate()

data = spark.read.csv('smsspamcollection/SMSSpamCollection',
                      inferSchema=True,
                      sep='\t')
data = data.withColumnRenamed('_c0', 'class').withColumnRenamed('_c1', 'text')
data.show()


# ---------------- Clean and Prepare the Data ----------------
# Create a new length feature:
data = data.withColumn('length', length(data['text']))
data.show()
data.groupBy('class').mean().show()


# ---------------- Feature Transformations ----------------
# The line bellow transforms the text into a 'list' of tokens:
tokenizer = Tokenizer(inputCol='text', outputCol='token_text')
# Here we remove the tokens that do not add value for the ML model:
stop_remove = StopWordsRemover(inputCol='token_text', outputCol='stop_token')
count_vec = CountVectorizer(inputCol='stop_token', outputCol='c_vec')
idf = IDF(inputCol='c_vec', outputCol='tf_idf')
# The line bellow converts the words on the column 'class' to numeric:
ham_spam_to_numeric = StringIndexer(inputCol='class', outputCol='label')
clean_up = VectorAssembler(inputCols=['tf_idf', 'length'], outputCol='features')


# ---------------- The Model (deploying varius models to latter comparing the results) ----------------
# NaiveBayes:
nb = NaiveBayes()
# Gradient-boosted tree classifier:
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=50)
# Random forest classifier:
rft = RandomForestClassifier(labelCol='label',
                             featuresCol='features',
                             numTrees=100)
# Binomial logistic regression:
lr = LogisticRegression(labelCol='label', featuresCol='features',
                        maxIter=100, regParam=0.3, elasticNetParam=0.8)


# ---------------- Pipeline ----------------:
data_prep_pipe = Pipeline(stages=[ham_spam_to_numeric, tokenizer,
                                  stop_remove, count_vec, idf, clean_up])
cleaner = data_prep_pipe.fit(data)
clean_data = cleaner.transform(data)
clean_data = clean_data.select('label', 'features')
clean_data.show(truncate=False)


# ---------------- Training ----------------:
trainingData, testData = clean_data.randomSplit([0.7, 0.3], 1234)

# NaiveBayes:
spam_detector_nb = nb.fit(trainingData)
# Make predictions.
test_nb_results = spam_detector_nb.transform(testData)
# Select example rows to display.
test_nb_results.select("prediction", "label", "features").show()

# Gradient-boosted tree classifier:
spam_detector_gbt = gbt.fit(trainingData)
# Make predictions.
test_gbt_results = spam_detector_gbt.transform(testData)
# Select example rows to display.
test_gbt_results.select("prediction", "label", "features").show()

# Random forest classifier:
spam_detector_rft = rft.fit(trainingData)
# Make predictions.
test_rft_results = spam_detector_rft.transform(testData)
# Select example rows to display.
test_rft_results.select("prediction", "label", "features").show()

# Binomial logistic regression
# Fit the model
spam_detector_fr = lr.fit(trainingData)
test_fr_results = spam_detector_fr.transform(testData)
# Select example rows to display.
test_fr_results.select("prediction", "label", "features").show()


# ---------------- Evaluation ----------------:
acc_eval = MulticlassClassificationEvaluator(metricName="accuracy")
auc_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',
                                         labelCol='label')
# NB:
acc_nb = acc_eval.evaluate(test_nb_results)
AUC_nb = auc_eval.evaluate(test_nb_results)
print(f'Accuracy of FR Model:\n --> {acc_nb}\nThe AUC of FR Model:\n --> {AUC_nb}')
# GBT:
acc_gbt = acc_eval.evaluate(test_gbt_results)
AUC_gbt = auc_eval.evaluate(test_gbt_results)
print(f'Accuracy of FR Model:\n --> {acc_gbt}\nThe AUC of FR Model:\n --> {AUC_gbt}')
# RFT:
acc_rft = acc_eval.evaluate(test_rft_results)
AUC_rft = auc_eval.evaluate(test_rft_results)
print(f'Accuracy of FR Model:\n --> {acc_rft}\nThe AUC of FR Model:\n --> {AUC_rft}')
# FR:
acc_fr = acc_eval.evaluate(test_fr_results)
AUC_fr = auc_eval.evaluate(test_nb_results)
print(f'Accuracy of FR Model:\n --> {acc_fr}\nThe AUC of FR Model:\n --> {AUC_fr}')
