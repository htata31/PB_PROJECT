import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd
import requests
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("HashtagCount")\
        .getOrCreate()
    df = spark.read.json("/user/hadoop/extractTweetsM.json")
    words = df.select(
        explode(
            split(df.text, " ")
        ).alias("word")
    )
    def extract_tags(word):
        if word.lower().startswith("#"):
            return word
        else:
            return "nonTag"
    extract_tags_udf = udf(extract_tags, StringType())
    resultDF = words.withColumn("tags", extract_tags_udf(words.word))
    resultDF.createOrReplaceTempView("hashtag_count")
    sqlhash = spark.sql("SELECT tag_value,\
    	times\
    	FROM (SELECT upper(tags) tag_value,\
    	count(*) times\
    	FROM hashtag_count\
    	WHERE 1=1\
    	AND tags!='nonTag'\
    	GROUP BY upper(tags)\
    	ORDER BY times desc, tag_value asc) limit 10")
    sqlhash.show(70)

    sqlhash.toPandas().to_csv('trendingTags.csv')

#Code for bar graph
data = pd.read_csv('trendingTags.csv')
#plt.bar(data['player'],data['count'])
data.plot(kind="bar",x='tag_value',y='times',legend=None)
#plt.legend().remove()
plt.ylabel('Count')
plt.xlabel('Hash Tags')
plt.title('Trending Tags')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()