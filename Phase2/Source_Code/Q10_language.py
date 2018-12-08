# Python version 2.7.6
# Import the datetime and pytz modules.
import datetime, pytz
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys,tweepy,csv,re
from textblob import TextBlob
import matplotlib.pyplot as plt
import matplotlib.patches
datetime_obj = datetime.datetime.strptime('Sun Oct 12 10:53:51 +0000 2014', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Sun Oct 12 10:53:51 +0000 2014','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("/user/hadoop/extractTweetsM.json")
df.createOrReplaceTempView("cricket")
sqldf= spark.sql("SELECT user.lang language,count(*) no_of_tweets  FROM cricket WHERE user.lang is NOT NULL  GROUP BY user.lang ORDER BY 2 DESC limit 10")
sqldf.show(150)

sqldf.toPandas().to_csv('language.csv')
#Code for bar graph
data = pd.read_csv('language.csv')

plt.bar(data['language'],data['no_of_tweets'])
#data.plot.bar(x='loc',y='number_of_tweets')
plt.ylabel('no_of_tweets')
plt.xlabel('language')
plt.title('Top Ten Users')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()