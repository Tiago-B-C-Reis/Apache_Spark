from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.feature import (Binarizer, VectorAssembler,
                                VectorIndexer, StringIndexer, OneHotEncoder)
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName('classification_spark').getOrCreate()

# Import data set:
df = spark.read.csv('daily_weather.csv', inferSchema=True, header=True)

# print all the columns in the df.
print(df.columns)

# Make a list with all the columns names need for the features.
feature_cols = ['air_pressure_9am', 'air_temp_9am', 'avg_wind_direction_9am',
                'avg_wind_speed_9am', 'max_wind_direction_9am', 'max_wind_speed_9am', 'rain_accumulation_9am',
                'rain_duration_9am', 'relative_humidity_9am', 'relative_humidity_3pm']

# Drop the column "number" because it's not needed:
df = df.drop("number")

# Drop all missing values:
df = df.na.drop()

# number of rows and columns in our DataFrame
print(df.count(), len(df.columns))


# -------------------------------------------------------------------------------------------------------------
# Create categorical variable. Let's create a categorical variable to denote if the humidity is not low.
# If the value is less than 25%, then we want the categorical value to be 0,
# otherwise the categorical value should be 1.
# We can create this categorical variable as a column in a DataFrame using Binarizer:
binarizer = Binarizer(threshold=24.9999, inputCol="relative_humidity_3pm", outputCol="label")
binarizedDF = binarizer.transform(df)
binarizedDF.printSchema()
binarizedDF.select("relative_humidity_3pm", "label").show(5)
# -------------------------------------------------------------------------------------------------------------

# Aggregate features.
# Create a vector per row and join them as the "features" column:
assembler = VectorAssembler(inputCols=feature_cols + ['label'], outputCol='features')

# Join the "features" column into the "df" table:
assembled = assembler.transform(binarizedDF)
assembled.show(5)


# Split it into train + test set:
train_data, test_data = assembled.randomSplit([0.7, 0.3], seed=13234)


# Use the models (just using the defaults arguments):
dtc = DecisionTreeClassifier(labelCol='label', featuresCol='features',
                             maxDepth=5, minInstancesPerNode=20, impurity="gini")


# Train the model:
dtc_model = dtc.fit(train_data)

# Transform the test data to get predictions:
dtc_preds = dtc_model.transform(test_data)

dtc_preds.select("prediction", "label").show(10)

# Run Evaluations ('area under the curve'):
my_binary_eval = BinaryClassificationEvaluator(labelCol='label')
print('DTC ACCURACY: ', my_binary_eval.evaluate(dtc_preds))


# 1- With the original dataset split into 80% for training and 20% for test,
# how many of the first 20 samples from the test set were correctly classified?
predictions = dtc_preds.select("prediction", "label").limit(20)
count = predictions.filter(predictions.prediction == predictions.label).count()
print("Number of correctly classified samples in the first 20 test samples:", count)


# 2- If we split the data using 70% for training data and 30% for test data,
# how many samples would the training set have (using seed 13234)?
print("Nº of rows in the 'train_data' split: {}\n"
      "Nº of rows in the 'test_data' split: {}".format(train_data.count(), test_data.count()))


# 3- Save the predictions results in a csv file:
dtc_preds.select("prediction", "label").write.csv(
    "/home/tiagor/PycharmProjects/Apache_Spark_Projects_and_Courses/Big_Data_Specialization/4.Machine_Learning_With_Big_Data/predictions.csv",
    header=True
)

