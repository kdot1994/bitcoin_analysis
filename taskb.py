import pyspark
from datetime import datetime

def filter_for_wikileaks_vout(line): #filter for malformed lines and transactions to the BTC wallet
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        if "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}" in fields[3]:
            return True
        else:
            return False
    except:
        return False


def clean_vin(line): #filter for malformed lines
    try:
        fields = line.split(',')
        if len(fields)!=3:
            return False

        return True

    except:
        return False

def clean_vout(line): #filter for malformed lines
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False

        return True

    except:
        return False

sc = pyspark.SparkContext() #creates SparkContext, enables us to connect to the cluster

vout_full_lines = sc.textFile("/data/bitcoin/vout.csv") #loads data as lines
vout = vout_full_lines.filter(filter_for_wikileaks_vout).map(lambda l: l.split(",")) #apply for fitler and split lines
vout_join = vout.map(lambda f: (f[0], (f[0], f[1], f[2], f[3]))) #preparation for join ((key_hash),(all features of vout dataset))

vin = sc.textFile("/data/bitcoin/vin.csv") #loads data as lines
vin = vin.filter(clean_vin).map(lambda l: l.split(","))
vin_join = vin.map(lambda f: (f[0], (f[0], f[1], f[2])) ) #preparation for join ((key_txid),(all features of vin dataset))

joined_data1 = vout_join.join(vin_join) #first join
joined_data1 = joined_data1.map(lambda a: (a[0], a[1][1]) ) #reshaping

joined_data1_join = joined_data1.map(lambda f: ((f[1][1], f[1][2]), (f[1][0], f[1][1], f[1][2])))  #preparation for join ((key_tx_hash, key_vout ),(all features of the dataset))

vout_full = vout_full_lines.filter(clean_vout).map(lambda l: l.split(",")) #apply for fitler and split lines
vout_full_join = vout_full.map(lambda f: ((f[0], f[2]), (f[0], f[1], f[2], f[3])))  #preparation for join ((key_hash, key_n ),(all features of vout dataset))

joined_data2 = joined_data1_join.join(vout_full_join) #second join
joined_data2 = joined_data2.map(lambda a: (a[1][1][3], float(a[1][1][1])))#reshape to prepare for key value pairs

all_donours = joined_data2.reduceByKey(lambda a, b: a + b) #reduce to (donor_wallet_adress, amount donated)

top10 = all_donours.takeOrdered(10, key = lambda x: -x[1]) #order and take the first ten elements
top10RDD = sc.parallelize(top10) #create RDD again
top10RDD.saveAsTextFile("out_courseworkC") #safe to HDFS
