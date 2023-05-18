import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from notebooks import utils

spark = SparkSession.builder.appName('cluster_analysis').getOrCreate()

# Import data set:
df = spark.read.csv('minute_weather.csv', inferSchema=True, header=True)

# Subset and remove unused data
print("Total nº of rows = ", df.count())

# Reduce the data size in order to process the data faster.
filteredDF = df.filter((df.rowID % 10) == 0)
print("Nº of rows in the data set after taking away 90% of the rows: ", filteredDF.count())

# Compute the statistical summary
summary_df = filteredDF.describe()
# Convert to Pandas DataFrame and display
pd.set_option('display.max_rows', None)  # To display all rows
pd.set_option('display.max_columns', None)  # To display all columns
print("\nStatistics summary:\n", summary_df.toPandas().transpose())

print("How many values of 'rain_accumulation' are iqual to 0 = ", filteredDF.filter(filteredDF.rain_accumulation == 0.0).count())
print("How many values of 'rain_duration' are iqual to 0 = ", filteredDF.filter(filteredDF.rain_duration == 0.0).count())

# Since almost all of the above rows are zero, let's drop those columns
# and also drop 'hpwren_timestamp' since it's not used here:
workingDF = filteredDF.drop("rain_accumulation").drop("rain_duration").drop("hpwren_timestamp")

# drop rows with missing values and count how many rows were dropped:
before = workingDF.count()
workingDF = workingDF.na.drop()
after = workingDF.count()
print("\nNº of rows before 'na.drop' - Nº of rows after 'na.drop' = ", before - after)

# Create a data frame to use on the scale feature:
print(workingDF.columns)
featuresUsed = ['air_pressure', 'air_temp', 'avg_wind_direction', 'avg_wind_speed',
                'max_wind_direction', 'max_wind_speed', 'relative_humidity']
assembler = VectorAssembler(inputCols=featuresUsed, outputCol="features_unscaled")
assembled = assembler.transform(workingDF)

# StandardScaler to scale the data:
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=True)
scalerModel = scaler.fit(assembled)
scaledData = scalerModel.transform(assembled)

# # Create elbow plot:
# In order to find the optimal 'K' for the k-means, we are going to apply k-means using different values for k,
# and calculating the within-cluster sum-of-squared error (WSSE).

scaledData = scaledData.select("features", "rowID")
elbowset = scaledData.filter((scaledData.rowID % 3) == 0).select("features")
elbowset.persist()
print("\nNº of rows in the data set every 3th example: ", elbowset.count())


# Create elbow plot to find optimal k for K-means
cluster = range(2, 31)  # Run k from 2 to 30
wsseList = utils.elbow(scaledData, cluster)
utils.elbow_plot(wsseList, cluster)

# Cluster using selected k.
scaledDataFeat = scaledData.select("features")
scaledDataFeat.persist()

kmeans = KMeans(k=12, seed=1)
model = kmeans.fit(scaledDataFeat)
transformed = model.transform(scaledDataFeat)

centers = model.clusterCenters()
print(centers)

# Create parallel plots of clusters and analysis.
P = utils.pd_centers(featuresUsed, centers)

utils.parallel_plot(P[P["relative_humidity"] < -0.5], P)
utils.parallel_plot(P[P["air_temp"] > 0.5], P)
utils.parallel_plot(P[(P["relative_humidity"] < -0.5) & (P["air_temp"] > 0.5)], P)
utils.parallel_plot(P.iloc[[2]], P)











