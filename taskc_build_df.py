import pyspark
import re
from datetime import datetime

sc = pyspark.SparkContext() #creates SparkContext, enables us to connect to the cluster

def clean_transactions(line):
    try:
        fields = line.split(',')
        if len(fields)!=5:
            return False

        return True

    except:
        return False

def clean_vout(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        return True

    except:
        return False

def clean_blocks(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        return True

    except:
        return False

#daily_number_of_transactions
tran = sc.textFile("/data/bitcoin/transactions.csv") #loads data as lines
header_tran = tran.first() #define header
tran = tran.filter(lambda line: line != header_tran) #filter for every line which is no the header
tran_unixtime = tran.filter(clean_transactions).map(lambda line: int(float(line.split(",")[2]))) #filter for malformed lines and take unixtime into an element
tran_real_dates = tran_unixtime.map(lambda l: datetime.utcfromtimestamp(l).strftime("%Y-%m-%d")) #transform unixtime to YYYY-MM-DD
tran_real_dates = tran_real_dates.filter(lambda l: (l>="01-01-2009") & (l<="31-12-2014")) #filter for dates in the range 01-01-2009 - 31-12-2014
tran_daycount = tran_real_dates.map(lambda word: (word, 1)) #creates key value pairs (YYYY-MM-DD, 1)
daily_number_of_transactions = tran_daycount.reduceByKey(lambda a, b: a + b) # reduces key value pairs, we obtain (YYYY-MM-DD, number of transactions on that day)

#complexity of math problem three features are exctracted average block blocks_difficulty, combined blocks_difficulty & blocks per day
blocks = sc.textFile("/data/bitcoin/blocks.csv") #loads data as lines
header_blocks = blocks.first() #define header
blocks = blocks.filter(clean_blocks).filter(lambda line: line != header_blocks).map(lambda l: l.split(","))  #filter for malformedlines, filter for every line which is no the header, split lines
blocks = blocks.map(lambda f: (f[0], (f[0], f[1], f[2], f[3]))) # create element ((height), (all variables from the blocks data set))
blocks_difficulty = blocks.map(lambda f: (datetime.utcfromtimestamp(int(f[1][2])).strftime("%Y-%m-%d"), float(f[1][3]))) #create element (time, blocks_difficulty)
blocks_difficulty = blocks_difficulty.reduceByKey(lambda a, b: a + b) #reduce to (YYYY-MM-DD, combined blocks_difficulty)
blocks_difficulty_join = blocks_difficulty.map(lambda f: (f[0], (f[0],f[1]))) #prepare for join
blocks_unixtime = blocks.map(lambda f: datetime.utcfromtimestamp(int(f[1][2])).strftime("%Y-%m-%d")) #safes the unixtime of when a block is mined in an element
blocks_per_day = blocks_unixtime.map(lambda word: (word, 1)) #creates key value pairs (YYYY-MM-DD, 1)
blocks_per_day = blocks_per_day.reduceByKey(lambda a, b: a + b) #blocks per day mined
blocks_per_day_join = blocks_per_day.map(lambda f: (f[0], (f[0],f[1]))) #prepare for join

blocks_difficulty_and_per_day = blocks_difficulty_join.join(blocks_per_day_join) #join
blocks_difficulty_and_per_day = blocks_difficulty_and_per_day.sortByKey() #sort

blocks_average_difficulty_per_day = blocks_difficulty_and_per_day.map(lambda f: ((f[0]),(float(f[1][0][1])/float(f[1][1][1])))) #calculate average block blocks_difficulty
blocks_average_difficulty_per_day_join = blocks_average_difficulty_per_day.map(lambda f: ((f[0]),(f[0], f[1]))) #prepare for join


#daily_transaction_volume  #vout has number of btc spent of each transaction, but no timestamp
vout_full_lines = sc.textFile("/data/bitcoin/vout.csv") #loads data as lines
vout = vout_full_lines.filter(clean_vout).map(lambda l: l.split(",")) #filter and split lines
vout_join = vout.map(lambda f: (f[0], (f[0], f[1], f[2], f[3]))) #prepare for join
tran = tran.filter(clean_transactions).map(lambda l: l.split(",")) #filter and split lines
tran_join = tran.map(lambda f: (f[0], (f[0], f[1], f[2], f[3], f[4]))) #prepare for join

vout_tran = vout_join.join(tran_join) #join
vout_tran = vout_tran.map(lambda a: (a[1][1][2], float(a[1][0][1])) ) #reshape
vout_tran = vout_tran.map(lambda l: (datetime.utcfromtimestamp(int(l[0])).strftime("%Y-%m-%d"), l[1])) #reshape to (YYYY-MM-DD, amount transacted)
daily_transaction_volume_btc = vout_tran.reduceByKey(lambda a, b: a + b) #reduce to (YYYY-MM-DD, volume transacted on that day)
daily_transaction_volume_btc_join = daily_transaction_volume_btc.map(lambda f: (f[0], (f[0], f[1]))) #prepare for join
daily_number_of_transactions_join = daily_number_of_transactions.map(lambda f: (f[0], (f[0], f[1]))) #prepare for join

#join all frames and reshape
df = daily_number_of_transactions.join(daily_transaction_volume_btc)
df = df.map(lambda f: (f[0], (f[1][0], f[1][1])))
df = df.join(blocks_average_difficulty_per_day_join)
df = df.map(lambda f: (f[0], (f[1][0][0], f[1][0][1], f[1][1][1])))
df = df.join(blocks_per_day_join)
df = df.map(lambda f: (f[0], (f[1][0][0], f[1][0][1], f[1][0][2],f[1][1][1])))
df = df.join(blocks_difficulty_join)
df = df.map(lambda f: ((f[0]), (f[1][0][0], f[1][0][1], f[1][0][2], f[1][0][3], f[1][1][1])))
df = df.sortByKey() #(date, number_of_transactions_daily)
df = df.map(lambda f: (f[0],f[1][0],f[1][1],f[1][2],f[1][3],f[1][4]))


vout_tran.saveAsTextFile("out_courseworkC") #safe to HDFS
