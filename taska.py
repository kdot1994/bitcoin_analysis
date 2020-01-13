import pyspark
from datetime import datetime

sc = pyspark.SparkContext() #creates SparkContext, enables us to connect to the cluster

def is_good_line(line): #filter for malformed lines
    try:
        fields = line.split(',')
        if len(fields)!=5:
            return False

        return True

    except:
        return False

lines = sc.textFile("/data/bitcoin/transactions.csv") #loads data as lines
header = lines.first() #takes the header of the filer, the first line and saves it as an element
lines = lines.filter(lambda line: line != header) #filter for every line which is no the header

clean_lines = lines.filter(is_good_line) #filter for good lines, not malformed

unixtime = lines.map(lambda line: int(line.split(",")[2])) #safes the unixtime of when a transactionis made in an element and makes it an integer

real_dates = unixtime.map(lambda l: datetime.utcfromtimestamp(l).strftime("%Y-%m")) #converts unixtime to the format YYYY-MM

real_dates = real_dates.filter(lambda l: (l>="2009-01") & (l<="2014-12")) #filter for dates in between 2009-01 and 2014-12

monthcount = real_dates.map(lambda word: (word, 1)) #creates key value pairs (YYYY-MM, 1)

counted = monthcount.reduceByKey(lambda a, b: a + b) # reduces key value pairs, we obtain (YYYY-MM, number of transactions in that month)

counted =  counted.sortByKey() #sorts the values

counted.saveAsTextFile("out_courseworkC") #safe to personal HDFS
