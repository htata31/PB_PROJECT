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
import matplotlib.patches
datetime_obj = datetime.datetime.strptime('Sun Oct 12 10:53:51 +0000 2014', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Sun Oct 12 10:53:51 +0000 2014','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("file:///home/saran/Downloads/PBPhase2/extractTweetsM.json")
df.createOrReplaceTempView("cricket")
sqldf= spark.sql("SELECT 'CSK' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%CSK%' \
UNION \
SELECT 'RCB' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%RCB%' \
UNION \
SELECT 'SRH' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%SRH%'")
sqldf.show(150)

sqldf.toPandas().to_csv('Iplteam.csv')

#Code for the doing pie chart.
df1 =  pd.read_csv('Iplteam.csv')
tournament_data = df1["ipl_team"]
count_data = df1["count(1)"]
colors = ["#808080","#FA8072","#2ca02c"]
#explode = (0.1, 0, 0, 0, 0)
total_count= (sum(count_data))
data=[]
handles = []
for i, l  in enumerate(tournament_data):
    handles.append(matplotlib.patches.Patch(color=colors[i]))
    data.append(tournament_data[i]+"-"+str("{0:.2f}".format(float(count_data[i]*100)/total_count))+"%")



plt.legend(handles,data, bbox_to_anchor=(0.85,1.025), loc="upper left")
plt.pie(count_data, colors=colors, shadow=False, startangle=90)
plt.title("Tweet counts for different formats of cricket")
plt.show()