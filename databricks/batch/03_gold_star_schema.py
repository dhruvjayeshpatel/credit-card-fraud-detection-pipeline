from pyspark.sql import functions as F

storage_account = ""
key = dbutils.secrets.get(scope="fraud-kv-scope", key="adls-access-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", key)

silver = spark.read.format("delta").load(
    f"abfss://processed@{storage_account}.dfs.core.windows.net/silver/transactions")
gold = f"abfss://curated@{storage_account}.dfs.core.windows.net/gold"

# DimCustomer: one row per unique card holder
dim_customer = (silver
    .select("cc_num", "full_name", "gender", "dob", "customer_age", "job",
            "street", "city", "state", "zip")
    .dropDuplicates(["cc_num"])
    .withColumn("customer_key", F.monotonically_increasing_id()))
dim_customer.write.format("delta").mode("overwrite").save(f"{gold}/dim_customer")

# DimMerchant: one row per merchant
dim_merchant = (silver
    .select("merchant", "category")
    .dropDuplicates(["merchant"])
    .withColumn("merchant_key", F.monotonically_increasing_id()))
dim_merchant.write.format("delta").mode("overwrite").save(f"{gold}/dim_merchant")

# DimDate: one row per calendar date
dim_date = (silver
    .select("trans_date").dropDuplicates()
    .withColumn("date_key", F.date_format("trans_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("trans_date"))
    .withColumn("month", F.month("trans_date"))
    .withColumn("day", F.dayofmonth("trans_date"))
    .withColumn("day_of_week", F.date_format("trans_date", "EEEE")))
dim_date.write.format("delta").mode("overwrite").save(f"{gold}/dim_date")

# FactTransaction: the events, with keys pointing at dimensions
fact = (silver
    .join(dim_customer.select("cc_num", "customer_key"), "cc_num")
    .join(dim_merchant.select("merchant", "merchant_key"), "merchant")
    .withColumn("date_key", F.date_format("trans_date", "yyyyMMdd").cast("int"))
    .select("trans_num", "customer_key", "merchant_key", "date_key",
            "trans_ts", "trans_hour", "amt", "lat", "long",
            "merch_lat", "merch_long", "is_fraud"))
fact.write.format("delta").mode("overwrite").save(f"{gold}/fact_transaction")

print(f"Customers: {dim_customer.count()}, Merchants: {dim_merchant.count()}, "
      f"Dates: {dim_date.count()}, Facts: {fact.count()}")