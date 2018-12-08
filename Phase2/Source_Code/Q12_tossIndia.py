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
import pandas as pd
import matplotlib.pyplot as plt
datetime_obj = datetime.datetime.strptime('Sun Oct 12 10:53:51 +0000 2014', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Sun Oct 12 10:53:51 +0000 2014','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("/user/hadoop/extractTweetsM.json")
df=df.filter('(upper(text) LIKE "%TOSS%")')
df.createOrReplaceTempView("cricket")
sqldf= spark.sql("SELECT count(DISTINCT id) no_of_toss\
    FROM cricket\
    WHERE 1=1\
    AND (upper(text) LIKE '%INDIA%WIN%' OR upper(text) LIKE '%WIN%INDIA%')\
    ")
sqldf.show(150)
sqldf.toPandas().to_csv('toss.csv')

#Code for bar graph
data = pd.read_csv('toss.csv')
#plt.bar(data['player'],data['count'])
data.plot(kind="bar",x='no_of_toss',y='count',legend=None)
#plt.legend().remove()
plt.ylabel('count')
plt.xlabel('no_of_toss')
plt.title('Trending Players')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()