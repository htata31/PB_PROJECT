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
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("/user/hadoop/extractTweetsM.json")
date= df.select("created_at")
def dateMTest(dateval):
    dt=datetime.datetime.strptime(dateval, '%a %b %d %H:%M:%S +0000 %Y')
    return dt
d = udf(dateMTest , DateType())
df=df.withColumn("created_date",d(date.created_at))
df.createOrReplaceTempView("cricket")
sqldf= spark.sql("SELECT id,text FROM cricket WHERE 1=1 AND upper(text) LIKE '%INDIA%'")
i=0
positive=0
neutral=0
negative=0
for t in sqldf.select("text").collect():
    i=i+1
    # print("It is ",i,str(t.text))
    analysis = TextBlob(str((t.text).encode('ascii', 'ignore')))
    # print(analysis.sentiment.polarity)
    if (analysis.sentiment.polarity<0):
        negative=negative+1
        # print(i," in negative")
    elif(analysis.sentiment.polarity==0.0):
        neutral=neutral+1
        # print(i," in neutral")
    elif(analysis.sentiment.polarity>0):
        positive=positive+1

print("Percentage of people supported team India is",((positive)*100)/i)
