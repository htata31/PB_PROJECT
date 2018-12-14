# Python version 2.7.6
# Import the datetime and pytz modules.
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from platform import python_version
from textblob import TextBlob
from pyspark.sql.types import *
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("extractTweetsM.json")
df.createOrReplaceTempView("cricket")
loc = df.withColumn('newcol',regexp_replace('user.location',', ','*'))
loc.createOrReplaceTempView("cricket_contri")
toss_df=df.filter('(upper(text) LIKE "%TOSS%")')
toss_df.createOrReplaceTempView("cricket_toss")
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
cricket_wc_df=df.filter('(upper(text) LIKE "%WORLD%CUP%2019%" OR upper(text) LIKE "%WC%2019%")')
cricket_wc_df.createOrReplaceTempView("cricket_wc")
print(" ")
print("A system to store, analyze, and visualize Twitter’s tweets")
print(" ")
print("Team Details")
print("Mastan Krishna Neeraj Padarthi – nmpkxr")
print("Harish Tata – hthnv")
print("Saran Akkiraju – saf2k")
print(" ")
print("In the Below section, we load the data into Spark and ran the queries to obtain the results.")
print(" ")
print("1.Tweets from multiple sources")
sqldf= spark.sql("SELECT substr(source,instr(source,'>')+1,(instr(source,'</a>')-instr(source,'>'))-1) source_info\
    ,count(*) source_count\
    FROM cricket\
    WHERE 1=1\
    GROUP BY substr(source,instr(source,'>')+1,(instr(source,'</a>')-instr(source,'>'))-1)\
    ORDER BY source_count desc\
    LIMIT 10")
sqldf.show()
print("2.Top 10 Trending Tags, with count of number of tweets made on particular tag")
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
print("3.Number of tweets from each country")
sqldf= spark.sql("SELECT loc,sum(no_of_values) number_of_tweets\
 	FROM (\
 	(SELECT loc,count(*) no_of_values FROM (SELECT (CASE WHEN (substring_index(upper(newcol),'*',-1) LIKE '%UNITED%STATES%' OR substring_index(upper(newcol),'*',-1) LIKE '%USA%' OR substring_index(upper(newcol),'*',-1)='USA' OR substring_index(upper(newcol),'*',-1) IN ('TX','FL','CA','NY','NC'))\
 													   	THEN 'UNITED STATES'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%UNITED%KINGDOM%' OR substring_index(upper(newcol),'*',-1)='UK' OR substring_index(upper(newcol),'*',-1) IN ('LONDON/ESSEX','SCOTLAND','IRELAND','LONDON','ENGLAND','ENGLAND ','FRANCE','SWEDEN'))\
 													   	THEN 'UNITED KINGDOM'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%KENYA%'\
 													   	THEN 'KENYA'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%PAKISTAN%' OR substring_index(upper(newcol),'*',-1) IN ('KARACHI','LAHORE'))\
 													   	THEN 'PAKISTAN'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%CANADA%' OR substring_index(upper(newcol),'*',-1) IN ('ONTARIO'))\
 													   	THEN 'CANADA'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%SOUTH%AFRICA%' OR substring_index(upper(newcol),'*',-1) IN ('CAPE TOWN'))\
 													   	THEN 'SOUTH AFRICA'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%GERMANY%'\
 													   	THEN 'GERMANY'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%ZIMBABWE%'\
 													   	THEN 'ZIMBABWE'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%VENEZUELA%'\
 													   	THEN 'VENEZUELA'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%JAPAN%'\
 													   	THEN 'JAPAN'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%SINGAPORE%'\
 													   	THEN 'SINGAPORE'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%NEPAL%'\
 													   	THEN 'NEPAL'\
 													   	WHEN substring_index(upper(newcol),'*',-1) LIKE '%BANGLADESH%'\
 													   	THEN 'BANGLADESH'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%INDIA%' OR substring_index(upper(newcol),'*',-1) IN ('INDIA.','SRINAGAR','LUCKNOW','NAGPUR','PONDA GOA','COIMBATORE','TAMILNADU','BHARAT','KARNATAKA','UT','GA','KOLKATA','BOMBAY','MAHARASHTRA','WEST BENGAL','CHENNAI','PUNE','BANGALORE','HYDERABAD','GURGAON','MI','MUMBAI','JAMMU AND KASHMIR','AHMEDABAD','GHAZIABAD','RAJKOT','NEW DELHI','DELHI','TAMIL NADU'))\
 													   	THEN 'INDIA'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%UNITED%ARAB%' OR substring_index(upper(newcol),'*',-1) IN ('SAUDI ARABIA','QATAR','DUBAI','ABU DHABI'))\
 													   	THEN 'UNITED ARAB EMIRATES'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%NEW%ZEALAND%' OR substring_index(upper(newcol),'*',-1) IN ('AUCKLAND'))\
 													   	THEN 'NEW ZEALAND'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%SRI%LANKA%' OR substring_index(upper(newcol),'*',-1) IN ('COLOMBO'))\
 													   	THEN 'SRI LANKA'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%AUSTRALIA%' OR substring_index(upper(newcol),'*',-1) IN ('SYDNEY','MELBOURNE','MELBOURNE AUSTRALIA','AUSTRIA','QUEENS LAND','QUEENSLAND','NEW SOUTH WALES','ADELAIDE','SOUTH AUSTRALIA','VICTORIA'))\
 													   	THEN 'AUSTRALIA'\
 													   	WHEN (substring_index(upper(newcol),'*',-1) LIKE '%WEST%INDIES%' OR substring_index(upper(newcol),'*',-1) IN ('JAMAICA'))\
 													   	THEN 'WEST INDIES'\
 														ELSE null\
 														END) loc\
	FROM\
	cricket_contri\
	WHERE 1=1 \
	AND place IS NULL)\
	WHERE loc IS NOT NULL\
	GROUP BY loc)\
	UNION\
	(SELECT upper(place.country) loc,\
	count(*) no_of_values\
	FROM\
	cricket_contri\
	WHERE 1=1 \
	AND place.country!=''\
	GROUP BY upper(place.country))\
	)\
	GROUP BY loc\
	ORDER BY number_of_tweets desc")
sqldf.show()
print("4.Tweet counts for different formats of cricket")
sqldf= spark.sql("SELECT 'ODI' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%ODI%' \
UNION \
SELECT 'TEST' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%TEST%MATCH%' \
UNION \
SELECT 'T20' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%T20%'")
sqldf.show(150)
print("5.Tweet counts on top Tournments")
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
print("6.Most retweeted tweets by the users")
sqldf= spark.sql("SELECT text, count(*) no_of_tweets \
FROM cricket \
WHERE 1=1 \
AND retweeted_status.id IS NOT NULL \
AND lang='en' \
GROUP BY text \
ORDER BY no_of_tweets DESC \
limit 10")
sqldf.show(150)
df.createOrReplaceTempView("player_count")
print("7.Numbers of tweers for top players")
sqlhash = spark.sql("SELECT 'Virat' player,count(text) as count  \
    FROM player_count\
    WHERE 1=1\
    AND (upper(text) LIKE '%KHOLI%' or upper(text) LIKE '%VIRAT%' or upper(text) LIKE '%VIR%')\
    GROUP BY player\
    UNION\
    SELECT 'Dhoni' player,count(text) as count \
    FROM player_count\
    WHERE 1=1\
    AND (upper(text) LIKE '%MSD%' or upper(text) LIKE '%DHONI%')\
    GROUP BY player\
    UNION\
    SELECT 'Yuvi' player,count(text) as count \
    FROM player_count\
    WHERE 1=1\
    AND (upper(text) LIKE '%YUVI%' or upper(text) LIKE '%YWC%' or upper(text) LIKE '%YUVRAJ%')\
    GROUP BY player\
    UNION\
    SELECT 'Sachin' player,count(text) as count\
    FROM player_count\
    WHERE 1=1\
    AND (upper(text) LIKE '%SACHIN%' or upper(text) LIKE '%TENDULKAR%')\
    GROUP BY player\
    UNION\
    SELECT 'Rohit' player,count(text) as count \
    FROM player_count\
    WHERE 1=1\
    AND (upper(text) LIKE '%ROHIT%SHARMA%' or upper(text) LIKE '%HITMAN%')\
    GROUP BY player\
    UNION\
    SELECT 'Shikar' player,count(text) as count \
    FROM player_count\
    WHERE 1=1\
    AND (upper(text) LIKE '%SHIKAR%DAWAN%' or upper(text) LIKE '%SHIKAR%' or upper(text) LIKE '%DHAWAN%')\
    GROUP BY player")
sqlhash.show()
print("8.Tweets based on Language")
sqldf= spark.sql("SELECT user.lang language,count(*) no_of_tweets  FROM cricket WHERE user.lang is NOT NULL  GROUP BY user.lang ORDER BY 2 DESC limit 10")
sqldf.show(150)
print("9.Top 10 user's tweet counts")
sqldf= spark.sql("SELECT user.id,user.name,count(*) FROM cricket WHERE (user.id is not null and user.name is not null) group by user.id,user.name order by 3 desc limit 10")
sqldf.show(150)
print("10.Number of times India won the toss over the time period")
sqldf= spark.sql("SELECT count(DISTINCT id) no_of_toss\
    FROM cricket_toss\
    WHERE 1=1\
    AND (upper(text) LIKE '%INDIA%WIN%' OR upper(text) LIKE '%WIN%INDIA%')\
    ")
sqldf.show(150)
print("11.Number of tweets for famous IPL teams")
sqldf= spark.sql("SELECT 'CSK' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%CSK%' \
UNION \
SELECT 'RCB' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%RCB%' \
UNION \
SELECT 'SRH' ipl_team,count(*) FROM cricket WHERE upper(text) LIKE '%SRH%'")
sqldf.show(150)
print("12.Tweets to display Verified and Unverfified users")
sqldf= spark.sql("SELECT 'Verified' player,count(DISTINCT id) as count \
        FROM cricket \
        WHERE 1=1 \
        AND user.verified = 'true' \
        GROUP BY player \
        UNION \
SELECT 'UnVerified' player,count(DISTINCT id) as count \
        FROM cricket \
        WHERE 1=1 \
        AND user.verified = 'false' \
        GROUP BY player")
sqldf.show(150)
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
print("13.Percentage of people supported team India is",((positive)*100)/i)
print(" ")
print(" ")
sqldf= spark.sql("SELECT country_name,text FROM(SELECT 'INDIA' country_name,text FROM cricket_wc WHERE 1=1 AND (upper(text) LIKE '%IND%' OR UPPER(TEXT) LIKE '%INDIA%') UNION ALL SELECT 'WEST INDIES' country_name,text FROM cricket_wc WHERE  (upper(text) LIKE '%WI%' or upper(text) LIKE '%WEST%INDIES%'))")
# i=0
positive=0
neutral=0
negative=0
xdf = sqldf.select("country_name").distinct().collect()
for t in sqldf.select("country_name").distinct().collect():
    print("14.Prediction for the country winning World Cup 2019",t.country_name)
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
    print("Negative % is",negative_percent)
    print("Positive % is",positive_percent)
    print("Neutral % is",neutral_percent)
print(" ")
print(" ")
sqldf= spark.sql("SELECT id,text FROM cricket WHERE 1=1 AND (upper(text) LIKE '%BANGLADESH%' OR text LIKE '%bangladesh%')")
i=0
positive=0
neutral=0
negative=0
for t in sqldf.select("text").collect():
    i=i+1
    # print("It is ",i,str(t.text))
    analysis = TextBlob(str((t.text).encode('ascii', 'ignore')))
    if (analysis.sentiment.polarity<0):
       	negative=negative+1
    elif(analysis.sentiment.polarity==0.0):
        neutral=neutral+1
    elif(analysis.sentiment.polarity>0):
        positive=positive+1		
print("15.Sentiment Analysis for Bangladesh Team")
print("Total negative % is",((negative)*100)/i)
print("Total neutral % is",((neutral)*100)/i)
print("Total positive % is",((positive)*100)/i)