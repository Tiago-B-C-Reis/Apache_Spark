from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import (RandomForestClassifier)

spark = SparkSession.builder.appName('trees_project').getOrCreate()

# Import the data that is already nicely formatted into label and features:
data = spark.read.csv("dog_food.csv",
                      inferSchema=True, header=True)
data.show()
data.printSchema()
data.describe().show()
print(data.columns)

# Create a vector per row and join them as the "features" column:
assembler = VectorAssembler(inputCols=['A', 'B', 'C', 'D'],
                            outputCol='features')

# Join the "features" column into the "data" table:
output = assembler.transform(data)

# Create the final nice and usable data to feed the model:
final_data = output.select('features', 'Spoiled')
final_data.show()

# Run the model:
rfc = RandomForestClassifier(labelCol='Spoiled',
                             featuresCol='features',
                             numTrees=150)

# Train the models:
rfc_model = rfc.fit(final_data)

# Feature importance:
print("Feature importance: \n", rfc_model.featureImportances)

# Result:
# (4,[0,1,2,3],[0.0201527008056547,0.01809033692117003,0.9416301803831627,0.02012678189001246])
# Based on the analysis, it can be inferred that feature 2 (column 'C' of the data) has the
# strongest correlation with the prediction, indicating that the preservative chemical 'C' may be
# the leading cause of spoilage in the dog food.
