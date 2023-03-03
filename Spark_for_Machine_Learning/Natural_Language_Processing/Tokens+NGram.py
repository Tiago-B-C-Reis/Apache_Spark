from pyspark.sql import SparkSession
from pyspark.ml.feature import (Tokenizer,
                                RegexTokenizer)
from pyspark.sql.functions import (col,
                                   udf)
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram

spark = SparkSession.builder.appName('nlp').getOrCreate()

# Create the data set:
sen_df = spark.createDataFrame([
    (0, 'Hi I heard about Spark'),
    (1, 'I wish java could use case classes'),
    (2, 'Logistic,regression,models,are,neat'),
], ['id', 'sentence'])
sen_df.show()

# Transform every word of each sentence in a token (tokenizing):
tokenizer = Tokenizer(inputCol='sentence', outputCol='words')
regex_tokenizer = RegexTokenizer(inputCol='sentence', outputCol='words',
                                 pattern='\\W')
count_tokens = udf(lambda words: len(words), IntegerType())
tokenizer = tokenizer.transform(sen_df)
tokenizer.show()

# Count the amount of token in each sentence:
tokenizer.withColumn('tokens', count_tokens(col('words'))).show()

# The sentence ID=2 have only one token because the words are separated by commas and
# not by spaces, the algorith considers the entire phrase just one word,
# but here we wil fix that:
rg_tokenized = regex_tokenizer.transform(sen_df)
rg_tokenized.withColumn('tokens', count_tokens(col('words'))).show()


# Stops words remover (words like 'the', 'a', 'I', ...):
# First create the data set:
sentenceDataFrame = spark.createDataFrame([
    (0, ['I', 'saw', 'the', 'green', 'horse']),
    (1, ['Mary', 'had', 'a', 'little', 'lamb'])
], ['id', 'tokens'])
sentenceDataFrame.show()
# Then code the remover:
remover = StopWordsRemover(inputCol='tokens', outputCol='filtered')
remover.transform(sentenceDataFrame).show()


# n-gram:
# Create the data set:
wordDataFrame = spark.createDataFrame([
    (0, ["Hi", "I", "heard", "about", "Spark"]),
    (1, ["I", "wish", "Java", "could", "use", "case", "classes"]),
    (2, ["Logistic", "regression", "models", "are", "neat"])
], ["id", "words"])
# Create the n-gram: (will show what is the most likely word to appear next)
ngram = NGram(n=2, inputCol='words', outputCol='grams')
ngram.transform(wordDataFrame).select('grams').show(truncate=False)
