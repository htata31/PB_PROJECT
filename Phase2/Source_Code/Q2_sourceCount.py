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
import pandas as pd
import matplotlib.pyplot as plt
import sys,tweepy,csv,re
from textblob import TextBlob
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("/user/hadoop/extractTweetsM.json")
df.createOrReplaceTempView("cricket")
sqldf= spark.sql("SELECT substr(source,instr(source,'>')+1,(instr(source,'</a>')-instr(source,'>'))-1) source_info\
    ,count(*) source_count\
    FROM cricket\
    WHERE 1=1\
    GROUP BY substr(source,instr(source,'>')+1,(instr(source,'</a>')-instr(source,'>'))-1)\
    ORDER BY source_count desc\
    LIMIT 10")
sqldf.show()
sqldf.toPandas().to_csv('source_count.csv')

#Code for bar graph
data = pd.read_csv('source_count.csv')
#plt.bar(data['player'],data['count'])
data.plot(kind="bar",x='source_info',y='source_count',legend=None)
#plt.legend().remove()
plt.ylabel('Count')
plt.xlabel('Sources')
plt.title('Tweets from different sources')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()