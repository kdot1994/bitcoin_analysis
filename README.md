## bitcoin_analysis
The following code shows how Apache Hadoop and Apache Spark can be used to analyse the bitcoin currency. I used a Dataframe of all bitcoin transactions to create different insights:
# task a
Create a bar plot showing the number of transactions which occurred every month between the start and end of the dataset. What do you notice about the overall trend in the utilisation of bitcoin?
# task b
Obtain the top 10 donors over the whole dataset for the Wikileaks bitcoin address: {1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}. Is there any information on who these wallets belong to? Can you work out how much was being donated if converted into pounds?
# task c
Find a dataset containing the prices of bitcoin throughout time (Many sites exist with such information). Utilising the Spark MlLib library, combine this, characteristics extracted from the data (volume of bitcoins sold per day, transactions per pay), and any other external sources into a model for price forecasting. How far into the future from our subset end date (September 2013) is your model accurate? How does the volatility of bitcoin prices affect the effectiveness of your model?
