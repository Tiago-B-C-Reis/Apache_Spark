from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import (VectorAssembler, VectorIndexer,
                                OneHotEncoder, StringIndexer)
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName('titaniclogreg').getOrCreate()

# Load training data (not split yet)
data = spark.read.csv("titanic.csv",
                      inferSchema=True, header=True)
data.show()
data.printSchema()

# Select the columns that are useful for our features
my_cols = data.select(['Survived', 'Pclass',
                       'Sex', 'Age',
                       'SibSp', 'Parch',
                       'Fare', 'Embarked'])

# Dealing with missing data: (simply dropping the elements with 'Null')
my_final_data = my_cols.na.drop()


# Convert the strings into integers:
gender_indexer = StringIndexer(inputCol='Sex',
                               outputCol='SexIndex')
embark_indexer = StringIndexer(inputCol='Embarked',
                               outputCol='EmbarkIndex')

# Since this is logist regression we need to 'one hot' encode these integers (keep then 0 or 1).
gender_encoder = OneHotEncoder(inputCol='SexIndex',
                               outputCol='SexVec')
embark_encoder = OneHotEncoder(inputCol='EmbarkIndex',
                               outputCol='EmbarkVec')

# Assemble all the columns info that we need to have as features in one vector (column).
assembler = VectorAssembler(inputCols=['Pclass', 'SexVec', 'EmbarkVec',
                                       'Age', 'SibSp', 'Parch', 'Fare'],
                            outputCol='features')

# Implement the model.
log_reg_titanic = LogisticRegression(featuresCol='features',
                                     labelCol='Survived')

# Create a pipeline.
pipeline = Pipeline(stages=[gender_indexer, embark_indexer, gender_encoder,
                            embark_encoder, assembler, log_reg_titanic])

# Train and Test set creation.
train_data, test_data = my_final_data.randomSplit([0.7, 0.3])

# Run the pipeline using the train data set to train the model.
fit_model = pipeline.fit(train_data)

# Evaluate results using test data set.
results = fit_model.transform(test_data)

# Evaluate model. (there is 'prediction' bellow because that's the default column name
# that the function 'transform' above call it)
my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',
                                        labelCol='Survived')

results.select('Survived', 'Prediction').show()

# Use the evaluater.
AUC = my_eval.evaluate(results)
print(AUC)




