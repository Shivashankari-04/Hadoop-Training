

# STEP 1. START SPARK

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder \
    .appName("CapstoneCustomerStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


#STEP 2. LOAD CUSTOMER DATA

customer_df = spark.read.csv("/user/cloudera/capstone/cust.csv",header=True,inferSchema=True)

# STEP 3: Live streaming


# DEFINE TRANSACTION SCHEMA

transaction_schema = "txn_id INT, customer_id INT, amount INT"


# READ STREAMING TRANSACTIONS

transactions_df = spark.readStream \
    .schema(transaction_schema) \
    .option("header", "true") \
    .csv("/user/cloudera/capstone/")


#  JOIN WITH CUSTOMER DATA

joined_df = transactions_df.join(customer_df, "customer_id")


# STEP 5: Total Spending per customer

total_spending = joined_df.groupBy("customer_id", "name") \
    .agg(sum("amount").alias("Total_Spent"))



# STEP 6: High Value Transactions

high_value = joined_df.filter(col("Amount") > 3000) \
    .select("txn_id", "name", "amount")


# STEP 7: Live Transaction Count

txn_count = joined_df.groupBy().agg(count("*").alias("Total_Transactions"))



# STEP 8: City-wise spending analysis

city_spending = joined_df.groupBy("City") \
    .agg(sum("Amount").alias("Total_Spending"))


# Output to Stream

query1 = total_spending.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query2 = high_value.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query3 = txn_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query4 = city_spending.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
# Keep streaming queries alive
spark.streams.awaitAnyTermination()

