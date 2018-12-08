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
import pandas as pd
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
sqldf= spark.sql("SELECT user.id,user.name,count(*) FROM cricket WHERE (user.id is not null and user.name is not null) group by user.id,user.name order by 3 desc limit 10")
sqldf.show(150)


sqldf.toPandas().to_csv('topUsers.csv')
#Code for bar graph
data = pd.read_csv('topUsers.csv')

plt.bar(data['name'],data['count(1)'])
plt.ylabel('count(1)')
plt.xlabel('name')
plt.title('Tweets from top Users')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()
