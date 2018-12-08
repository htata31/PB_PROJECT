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
from IPython.core.display import display
datetime_obj = datetime.datetime.strptime('Sun Oct 12 10:53:51 +0000 2014', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())


spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()

df = spark.read.json("file:///home/saran/Downloads/PBPhase2/extractTweetsM.json")


# df.select("created_date").show()
# df.printSchema()

# loc= df.select("user.location")
# loc.show(7)

loc = df.withColumn('newcol',regexp_replace('user.location',', ','*'))
# loc.select('newcol').show(7)

loc.createOrReplaceTempView("cricket")
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
	cricket\
	WHERE 1=1 \
	AND place IS NULL)\
	WHERE loc IS NOT NULL\
	GROUP BY loc)\
	UNION\
	(SELECT upper(place.country) loc,\
	count(*) no_of_values\
	FROM\
	cricket\
	WHERE 1=1 \
	AND place.country!=''\
	GROUP BY upper(place.country))\
	)\
	GROUP BY loc\
	ORDER BY number_of_tweets desc")
sqldf.show(13) 
#sqldf.toPandas().to_csv('country_contribution.csv')

#Code for bar graph
data = pd.read_csv('country_contribution.csv')
plt.bar(data['loc'],data['number_of_tweets'])
#data.plot.bar(x='loc',y='number_of_tweets')
plt.ylabel('Number of Tweets')
plt.xlabel('Name of the Country')
plt.title('Country Contributions')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()


