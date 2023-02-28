#!/usr/bin/env python
# coding: utf-8

# # Spark DataFrames Project Exercise 

# Let's get some quick practice with your new Spark DataFrame skills, you will be asked some basic questions about some stock market data, in this case Walmart Stock from the years 2012-2017. This exercise will just ask a bunch of questions, unlike the future machine learning exercises, which will be a little looser and be in the form of "Consulting Projects", but more on that later!
# 
# For now, just answer the questions and complete the tasks below.

# #### Use the walmart_stock.csv file to Answer and complete the  tasks below!

# #### Start a simple Spark Session

# In[3]:


import findspark
findspark.init('/home/tiagor/spark-3.3.2-bin-hadoop3')


# In[5]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('exercises').getOrCreate()


# #### Load the Walmart Stock CSV File, have Spark infer the data types.

# In[6]:


df = spark.read.csv('walmart_stock.csv', inferSchema=True, header=True)


# In[54]:


df.show()


# #### What are the column names?

# In[22]:


df.columns


# #### What does the Schema look like?

# In[17]:


df.printSchema()


# #### Print out the first 5 columns.

# In[181]:


for row in df.head(5):
    print(row, '\n')


# #### Use describe() to learn about the DataFrame.

# In[77]:


df.describe().show()


# ## Bonus Question!
# #### There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar. [Check this link for a hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast)
# 
# If you get stuck on this, don't worry, just view the solutions.

# In[183]:


df.describe().printSchema()


# In[185]:


from pyspark.sql.functions import format_number


# In[186]:


result = df.describe()


# In[194]:


result.select(result['Summary'], 
              format_number(result['Open'].cast('float'), 2).alias('Open'), 
              format_number(result['High'].cast('float'), 2).alias('Open'), 
              format_number(result['Low'].cast('float'), 2).alias('Open'), 
              format_number(result['Close'].cast('float'), 2).alias('Open'), 
              result['Volume'].cast('int').alias('Volume')
             ).show()


# #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.

# In[51]:


df.createOrReplaceTempView('walmart_stock')
results = spark.sql("SELECT High/Volume AS HV_Ratio FROM walmart_stock")
results.show()


# In[197]:


newdf = df.withColumn("HV Ratio", (df['High'] / df['Volume']))
newdf.select('HV Ratio').show()


# In[230]:


from pyspark.sql.functions import max, min, mean


# #### What day had the Peak High in Price?

# In[234]:


result = df.filter(df['High'] >= df.select(max('High')).head(1)[0][0]).collect()
row = result[0]
row.asDict()['Date']


# In[235]:


df.orderBy(df['High'].desc()).head(1)[0][0]


# #### What is the mean of the Close column?

# #### What is the max and min of the Volume column?

# In[240]:


df.select(max('Volume'), min('Volume')).show()


# In[241]:


df.agg({'Volume': 'max', 'Volume': 'min'}).show()
df.agg({'Volume': 'max'}).show()


# #### How many days was the Close lower than 60 dollars?

# In[246]:


df.filter((df['Close'] < 60)).count()


# In[247]:


df.createOrReplaceTempView('walmart_stock')
results = spark.sql("SELECT COUNT(Close) FROM walmart_stock WHERE Close < 60")
results.show()


# In[248]:


from pyspark.sql.functions import count


# In[249]:


result = df.filter((df['Close'] < 60))


# In[250]:


result.select(count('Close')).show()


# #### What percentage of the time was the High greater than 80 dollars ?
# #### In other words, (Number of Days High>80)/(Total Days in the dataset)

# In[139]:


(df.filter((df['High'] > 80)).count() / df.select('Date').count()) * 100


# #### What is the Pearson correlation between High and Volume?
# #### [Hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameStatFunctions.corr)

# In[140]:


from pyspark.sql.functions import corr


# In[253]:


df.select(corr('High', 'Volume')).show()


# #### What is the max High per year?

# In[152]:


from pyspark.sql.functions import (dayofmonth, dayofweek, dayofyear,
                                   hour, month, year, format_number,
                                   date_format)


# In[161]:


newdf = df.withColumn("Year", year(df['Date']))
result = newdf.groupby("Year").max().select(["Year", "max(High)"])
result.show()


# #### What is the average Close for each Calendar Month?
# #### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 

# In[175]:


newdf = df.withColumn("Year", month(df['Date']))
result = newdf.groupby("Year").mean().select(["Year", "avg(Close)"])
new_result = result.withColumnRenamed("avg(Close)", "Average Closing Price")
new_result.select(['Year', format_number('Average Closing Price', 4).alias("Avg Close")]).orderBy('Year').show()


# # Great Job!
