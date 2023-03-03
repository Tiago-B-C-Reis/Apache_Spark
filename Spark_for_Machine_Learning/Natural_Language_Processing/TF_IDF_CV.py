from pyspark.sql import SparkSession
from pyspark.ml.feature import (HashingTF,
                                IDF,
                                CountVectorizer,
                                Tokenizer)

spark = SparkSession.builder.appName('nlp').getOrCreate()

# Create the data set:
sentenceData = spark.createDataFrame([
    (0, 'Hi I heard about Spark'),
    (1, 'I wish java could use case classes'),
    (2, 'Logistic,regression,models,are,neat'),
], ['label', 'sentence'])
sentenceData.show()

# Transform every word of each sentence in a token (tokenizing):
tokenizer = Tokenizer(inputCol='sentence', outputCol='words')
words_data = tokenizer.transform(sentenceData)
words_data.show()

# Term Frequency (Phase_1):
hashing_tf = HashingTF(inputCol='words', outputCol='rawFeatures')
featurized_data = hashing_tf.transform(words_data)
# Inverse Document Frequency of SPARC (Phase_2):
idf = IDF(inputCol='rawFeatures', outputCol='features')
idf_model = idf.fit(featurized_data)
rescale_data = idf_model.transform(featurized_data)
rescale_data.select('label', 'features').show(truncate=False)
# now the data is ready to be feed into a NLP ML_algorithm


# Count Vectorization of SPARC:
# (convert a collection of text documents into vectors of words counts)
# Input data: Each row is a bag of words with a ID:
df = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" "))
], ["id", "words"])
df.show()

# fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol='words', outputCol='features',
                     vocabSize=3, minDF=2.0)
model = cv.fit(df)
result = model.transform(df)
result.show(truncate=False)
