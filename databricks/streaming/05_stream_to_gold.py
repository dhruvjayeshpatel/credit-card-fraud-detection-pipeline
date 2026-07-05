from delta.tables import DeltaTable
from pyspark.sql import functions as F

storage_account = ""
key = dbutils.secrets.get(scope="fraud-kv-scope", key="adls-access-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", key)

gold = f"abfss://curated@{storage_account}.dfs.core.windows.net/gold"
silver_stream = spark.read.format("delta").load(
    f"abfss://processed@{storage_account}.dfs.core.windows.net/silver_streaming/transactions")

dim_customer = spark.read.format("delta").load(f"{gold}/dim_customer")
dim_merchant = spark.read.format("delta").load(f"{gold}/dim_merchant")

# Build fact rows from streamed data (same shape as batch gold)
new_facts = (silver_stream
    .join(dim_customer.select("cc_num", "customer_key"), "cc_num", "left")
    .join(dim_merchant.select("merchant", "merchant_key"), "merchant", "left")
    .withColumn("date_key", F.date_format("trans_date", "yyyyMMdd").cast("int"))
    .select("trans_num", "customer_key", "merchant_key", "date_key",
            "trans_ts", "trans_hour", "amt", "lat", "long",
            "merch_lat", "merch_long", "is_fraud"))

# MERGE (upsert): insert only transactions not already in the fact table
fact = DeltaTable.forPath(spark, f"{gold}/fact_transaction")
(fact.alias("t")
 .merge(new_facts.alias("s"), "t.trans_num = s.trans_num")
 .whenNotMatchedInsertAll()
 .execute())

print("Merged. New fact count:",
      spark.read.format("delta").load(f"{gold}/fact_transaction").count())