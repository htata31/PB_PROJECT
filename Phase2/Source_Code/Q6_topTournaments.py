import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd
import requests
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("HashtagCount")\
        .getOrCreate()
    df = spark.read.json("file:///home/saran/Downloads/PBPhase2/extractTweetsM.json")
    df.createOrReplaceTempView("cricket")
    sqlhash = spark.sql("SELECT 'T20 World Cup' tournament,count(text) as count \
        FROM cricket\
        WHERE 1=1\
        AND (upper(text) LIKE '%T20 World Cup%' or upper(text) LIKE '%T20WORLDCUP%' or upper(text) LIKE '%WT20%' or (text) LIKE '%wt20%' or (text) LIKE '%t20worldcup%')\
        GROUP BY tournament\
        UNION\
        SELECT 'IPL' tournament,count(text) as count \
        FROM cricket\
        WHERE 1=1\
        AND (upper(text) LIKE '%IPL%' or upper(text) LIKE '%IPLT20%' or upper(text) LIKE '%IPLAUCTION%')\
        GROUP BY tournament\
        UNION\
        SELECT 'ODI World Cup ' tournament,count(text) as count \
        FROM cricket\
        WHERE 1=1\
        AND (upper(text) LIKE '% CRICKETWORLDCUP%' or upper(text) LIKE '%CWC%' )\
        GROUP BY tournament \
        UNION\
        SELECT 'ICC Champions Trophy' tournament,count(text) as count \
        FROM cricket\
        WHERE 1=1\
        AND (upper(text) LIKE '%CHAMPIONSTROPHY%' or upper(text) LIKE '%CT%')\
        GROUP BY tournament\
        UNION\
        SELECT 'BBL' tournament,count(text) as count \
        FROM cricket\
        WHERE 1=1\
        AND (upper(text) LIKE '%BBL%' or upper(text) LIKE '%BIGBASHLEAGUE%')\
        GROUP BY tournament\
        UNION\
        SELECT 'CPL' tournament,count(text) as count \
        FROM cricket\
        WHERE 1=1\
        AND (upper(text) LIKE '%CPL%' or upper(text) LIKE '%CARIBBEANPREMIERLEAGUE%' or (text) LIKE '%cplt20%' or upper(text) LIKE '%CPLT20%')\
        GROUP BY tournament\
        ")
    sqlhash.show()
    sqlhash.toPandas().to_csv('top_tournaments.csv')


#Code for the doing pie chart.
df1 =  pd.read_csv('top_tournaments.csv')
tournament_data = df1["tournament"]
count_data = df1["count"]
colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#8c564b","#FFFF00"]
#explode = (0.1, 0, 0, 0, 0)  
total_count= (sum(count_data))
data=[]
handles = []
for i, l  in enumerate(tournament_data):
    handles.append(matplotlib.patches.Patch(color=colors[i]))
    data.append(tournament_data[i]+"-"+str("{0:.2f}".format(float(count_data[i]*100)/total_count))+"%")



plt.legend(handles,data, bbox_to_anchor=(0.85,1.025), loc="upper left")
plt.pie(count_data, colors=colors, shadow=False, startangle=90)
plt.title("Top Tournaments ")
plt.show()
    