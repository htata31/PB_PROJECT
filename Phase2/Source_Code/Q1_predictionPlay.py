import datetime, pytz
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import sys,tweepy,csv,re
from textblob import TextBlob

spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("/user/hadoop/extractTweetsM.json")
df=df.filter('(upper(text) LIKE "%WORLD%CUP%2019%" OR upper(text) LIKE "%WC%2019%")')
df.createOrReplaceTempView("cricket_wc")
sqldf= spark.sql("SELECT country_name,text FROM(SELECT 'INDIA' country_name,text FROM cricket_wc WHERE 1=1 AND (upper(text) LIKE '%IND%' OR UPPER(TEXT) LIKE '%INDIA%') UNION ALL SELECT 'WEST INDIES' country_name,text FROM cricket_wc WHERE  (upper(text) LIKE '%WI%' or upper(text) LIKE '%WEST%INDIES%'))")
# i=0
positive=0
neutral=0
negative=0


xdf = sqldf.select("country_name").distinct().collect()
for t in sqldf.select("country_name").distinct().collect():
    print(t.country_name)
    i=0
    positive=0
    neutral=0
    negative=0
    
    for t in sqldf.select("text","country_name").where('country_name=='+'"'+t.country_name+'"').collect():
        i=i+1
        analysis = TextBlob(str((t.text).encode('ascii', 'ignore')))
        if (analysis.sentiment.polarity<0):
            negative=negative+1
        elif(analysis.sentiment.polarity==0.0):
            neutral=neutral+1
        elif(analysis.sentiment.polarity>0):
            positive=positive+1
    negative_percent=((negative)*100)/i
    positive_percent=((positive)*100)/i
    neutral_percent=((neutral)*100)/i

    #Draw a donut pie chart
    size_of_groups=[negative_percent,positive_percent,neutral_percent]
    names='negative_percent', 'positive_percent', 'neutral_percent' 
    # Create a pieplot
    plt.pie(size_of_groups,labels=names, colors=['red','green','blue'])
    
    # add a circle at the center
    my_circle=plt.Circle( (0,0), 0.7, color='white')
    p=plt.gcf()
    p.gca().add_artist(my_circle)
    plt.title("Prediction of the play")
    print("\n",t.country_name)
    plt.show()
    print("\n",t.country_name)