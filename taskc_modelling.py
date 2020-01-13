import pyspark
import re
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import *

sc= SparkContext() #creates SparkContext, enables us to connect to the cluster

#prepare data and conversion to pypspark Dataframe
data_info_btc = sc.textFile("/user/kms33/Cdf.txt").map(lambda l : l.strip("(").strip(")").strip("'")).map(lambda l: l.split(",")) #loads data as lines
data_usdbtc = sc.textFile("/user/kms33/BTC-USD_for_taskC.txt").map(lambda l: l.split(",")) #loads data as lines
header_usdbtc = data_usdbtc.first() #define header
data_usdbtc = data_usdbtc.filter(lambda line: line != header_usdbtc) #filter for every line which is no the header

data_info_btc_join = data_info_btc.map(lambda l:  ((l[0][:-1]),(l[0][:-1], l[1], l[2], l[3], l[4], l[5]))) #prepare for join
data_usdbtc_join= data_usdbtc.map(lambda l:  ((l[0]),(l[0], l[1], l[2], l[3], l[4], l[5],l[6]))) #prepare for join

#preparation to create Dataframe
data = data_info_btc_join.join(data_usdbtc_join)
data = data.map(lambda l:((l[0]), (l[1][0][1], l[1][0][2], l[1][0][3], l[1][0][4], l[1][0][5], l[1][1][5])))
data = data.sortByKey()
data = data.map(lambda l:(l[0], float(l[1][0]), float(l[1][1]), float(l[1][2]), float(l[1][3]), float(l[1][4]), float(l[1][5])))

schema = StructType([StructField(str(i), StringType(), True) for i in range(7)]) #create schema for Dataframe

df = sqlContext.createDataFrame(data, schema) #creates Dataframe
df = df.withColumnRenamed("0", "date").withColumnRenamed("1", "number_transactions").withColumnRenamed("2", "volume_transacted_btc").withColumnRenamed("3", "blocks_average_difficulty_per_day").withColumnRenamed("4", "number_blocks").withColumnRenamed("5", "combined_block_difficulty_per_day").withColumnRenamed("6","value_usd") #rename columns

#change type of attributes in the df
from pyspark.sql.types import IntegerType
df = df.withColumn("date", df["date"].cast("float"))
df = df.withColumn("number_transactions", df["number_transactions"].cast("float"))
df = df.withColumn("volume_transacted_btc", df["volume_transacted_btc"].cast("float"))
df = df.withColumn("blocks_average_difficulty_per_day", df["blocks_average_difficulty_per_day"].cast("float"))
df = df.withColumn("combined_block_difficulty_per_day", df["combined_block_difficulty_per_day"].cast("float"))
df = df.withColumn("number_blocks", df["number_blocks"].cast("float"))
df = df.withColumn("value_usd", df["value_usd"].cast("double"))
#df.toPandas().to_csv('df_full.csv') #safes df if wanted

##Regression
# use VectorAssembler to define target value and features
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['number_transactions', 'volume_transacted_btc', 'blocks_average_difficulty_per_day', 'combined_block_difficulty_per_day', "number_blocks"], outputCol = 'features')
v_df = vectorAssembler.transform(df)
#v_df.show(3)

#train test split
splits = v_df.randomSplit([0.8, 0.2])
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol = 'features', labelCol='value_usd', maxIter=10, regParam=0.3, elasticNetParam=0.8)#schema for Model
lr_model = lr.fit(train_df) #train model on train_df

lr_predictions = lr_model.transform(test_df) #use model to make predictions on test_df
lr_predictions.select("prediction","value_usd","features","date").toPandas().to_csv("predLR.csv") #safe predictions to local drive

#Random Forest
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

data_full = sc.textFile("/user/kms33/df_full.csv") #read full Dataframe as lines
header_data_full = data_full.first() #define header
data_full = data_full.filter(lambda line: line != header_data_full)  #filter for every line which is no the header
data_full = data_full.map(lambda l: l.split(",")) #split lines

(data_train, data_test) = data_full.randomSplit([0.8, 0.2]) #traintest split

dates_train = data_train.map(lambda l: l[0]) #create element with the dates for train
dates_test = data_test.map(lambda l: l[0]) #create element with the dates for test

#use labelpoint to define target variable and features
from pyspark.mllib.regression import LabeledPoint
data_train = data_train.map(lambda l: LabeledPoint(l[6],l[1:5]))
data_test = data_test.map(lambda l: LabeledPoint(l[6],l[1:5]))

#train Random Forest
model = RandomForest.trainRegressor(data_train, categoricalFeaturesInfo={},numTrees=100, featureSubsetStrategy="auto", impurity='variance', maxDepth=4, maxBins=32) #train Random Forest Model

# make predictions and safe them to the HDFS
predictions = model.predict(data_test.map(lambda x: x.features))
labels_predictions_date = data_test.map(lambda lp: lp.label).zip(predictions).zip(dates_test)
labels_predictions_date.saveAsTextFile("out_courseworkC_predRF")


#Calculate the MSE for both models plus coefficients, intercept, R2 for Regression
#MSE for Linear Regression
import numpy as np
array_valueusd = np.array(lr_predictions.select("value_usd").collect()).astype(float)
array_prediction = np.array(lr_predictions.select("prediction").collect()).astype(float)
array_squarederrors =  np.subtract(array_valueusd, array_prediction)
array_squarederrorssquare = array_squarederrors*array_squarederrors
sumsquarederrorsLR = array_squarederrorssquare.sum()
test_MSE_LR = sumsquarederrorsLR/float(test_df.count())

#MSE Randon Forest
sumsquarederrorsRF = labels_predictions_date.map(lambda l: (l[0][0] - l[0][1]) * (l[0][0] - l[0][1])).sum()
test_MSE_RF = sumsquarederrorsRF/float(data_test.count())

#print all the information about the Regression
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="value_usd",metricName="r2")
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))


#print MSE for both Models
print('Test Mean Squared Error Linear Regression = ' + str(test_MSE_LR))
print('Test Mean Squared Error Random Forest = ' + str(test_MSE_RF))
