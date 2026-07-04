from pyspark.sql import functions as F

storage_account = ""
key = dbutils.secrets.get(scope="fraud-kv-scope", key="adls-access-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", key)

bronze_path = f"abfss://processed@{storage_account}.dfs.core.windows.net/bronze/transactions"
df = spark.read.format("delta").load(bronze_path)

silver = (df
    .withColumn("trans_ts", F.to_timestamp("trans_date_trans_time"))
    .withColumn("dob", F.to_date("dob"))
    .withColumn("customer_age", F.floor(F.datediff(F.col("trans_ts"), F.col("dob")) / 365.25))
    .withColumn("trans_hour", F.hour("trans_ts"))
    .withColumn("trans_date", F.to_date("trans_ts"))
    .withColumn("full_name", F.concat_ws(" ", "first", "last"))
    .drop("_c0")                      # drop the useless index column
    .dropDuplicates(["trans_num"])    # dedupe on transaction id
    .filter(F.col("amt") > 0)         # remove invalid amounts
)

silver_path = f"abfss://processed@{storage_account}.dfs.core.windows.net/silver/transactions"
silver.write.format("delta").mode("overwrite").save(silver_path)
print(f"Silver rows: {silver.count()}")