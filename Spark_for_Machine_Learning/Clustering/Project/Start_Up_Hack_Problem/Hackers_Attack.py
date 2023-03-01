from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName('cluster').getOrCreate()

# Import the data that is already nicely formatted into label and features:
data = spark.read.csv("hack_data.csv",
                      inferSchema=True, header=True)
data.show()
data.printSchema()
data.describe().show()
print(data.columns)


# Don't use the column "LLocation", because the hackers are using a VPN, so it became
# a useless feature.
# Assemble all the columns info that we need to have as features in one vector (column).
assembler = VectorAssembler(inputCols=['Session_Connection_Time', 'Bytes Transferred',
                                       'Kali_Trace_Used', 'Servers_Corrupted',
                                       'Pages_Corrupted', 'WPM_Typing_Speed'],
                            outputCol='features')
final_data = assembler.transform(data)
final_data.printSchema()

# Feature Scaling:
scaler = StandardScaler(inputCol='features',
                        outputCol='scaledFeatures')
scaler_model = scaler.fit(final_data)
cluster_final_data = scaler_model.transform(final_data)

# Set and train a k-means model:
Kmeans_k2 = KMeans(featuresCol='scaledFeatures', k=2)
model_1 = Kmeans_k2.fit(cluster_final_data)
Kmeans_k3 = KMeans(featuresCol='scaledFeatures', k=3)
model_2 = Kmeans_k3.fit(cluster_final_data)

# Make predictions
model_1.transform(cluster_final_data).groupBy('prediction').count().show()
model_2.transform(cluster_final_data).groupBy('prediction').count().show()

# Result:
# According to the last part of the instructions for this project, it seems to be 2 hackers,
# because when K=2, the member of 'hacks' becomes 167 per cluster and when K=3 the number
# of 'hacks' became uneven.
