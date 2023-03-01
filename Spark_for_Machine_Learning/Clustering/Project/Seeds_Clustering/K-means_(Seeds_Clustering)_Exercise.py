from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder.appName('cluster').getOrCreate()

# Import the data that is already nicely formatted into label and features:
data = spark.read.csv("seeds_dataset.csv",
                      inferSchema=True, header=True)
data.show()
data.printSchema()
data.describe().show()
print(data.columns)

# Create a vector per row and join them as the "features" column:
# Since the data don't have 'labels' because this is an unsupervised learning algorithm
# all the columns in the 'data' is to be transformed into a 'features' column:
assembler = VectorAssembler(inputCols=data.columns,
                            outputCol='features')

# Join the "features" column into the "data" table:
final_data = assembler.transform(data)
final_data.show()
final_data.printSchema()

# The data set that is being used doesn't have the real necessity for "Feature Scaling",
# because the int scale difference between columns it's not so big, nevertheless it does no
# harm to use it, and it can improve the model performance even here.
scaler = StandardScaler(inputCol='features',
                        outputCol='scaledFeatures')
scaler_model = scaler.fit(final_data)
final_data = scaler_model.transform(final_data)
final_data.printSchema()
print(final_data.head(1))

# Set and train a k-means model:
Kmeans = KMeans(featuresCol='scaledFeatures', k=3)
model = Kmeans.fit(final_data)

# Make predictions
predictions = model.transform(final_data)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("\nSilhouette with squared euclidean distance = " + str(silhouette))

print("\n")

# Shows the results:
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
# Display a table that shows at each cluster the feature or value was assigned:
results = model.transform(final_data)
results.select("area", "prediction").show()

