from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, LongType, IntegerType)

# --- Auth to ADLS (same pattern as batch) ---
storage_account = ""   # your storage account
key = dbutils.secrets.get(scope="fraud-kv-scope", key="adls-access-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", key)

# --- Event Hubs connection (from Key Vault) ---
conn = dbutils.secrets.get(scope="fraud-kv-scope", key="eventhub-conn")
conn = conn + ";EntityPath=transactions"   # append the specific event hub
eh_conf = {
    "eventhubs.connectionString":
        sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn)
}

# --- Read the stream ---
raw_stream = spark.readStream.format("eventhubs").options(**eh_conf).load()

# Event Hubs delivers the payload as binary in the 'body' column
json_stream = raw_stream.select(
    F.col("body").cast("string").alias("json_str"),
    F.col("enqueuedTime").alias("event_enqueued_ts")
)

# --- Parse JSON into columns (schema matches the CSV) ---
schema = StructType([
    StructField("trans_date_trans_time", StringType()),
    StructField("cc_num", LongType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("amt", DoubleType()),
    StructField("first", StringType()),
    StructField("last", StringType()),
    StructField("gender", StringType()),
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("city_pop", IntegerType()),
    StructField("job", StringType()),
    StructField("dob", StringType()),
    StructField("trans_num", StringType()),
    StructField("unix_time", LongType()),
    StructField("merch_lat", DoubleType()),
    StructField("merch_long", DoubleType()),
    StructField("is_fraud", IntegerType()),
])

parsed = (json_stream
    .withColumn("data", F.from_json("json_str", schema))
    .select("data.*", "event_enqueued_ts"))

# --- Write to streaming bronze (append mode + checkpoint) ---
bronze_stream_path = f"abfss://processed@{storage_account}.dfs.core.windows.net/bronze_streaming/transactions"
checkpoint_path   = f"abfss://processed@{storage_account}.dfs.core.windows.net/checkpoints/bronze_streaming"

query = (parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_stream_path))

# silver stream

silver_stream = (spark.readStream.format("delta").load(bronze_stream_path)
    .withColumn("trans_ts", F.to_timestamp("trans_date_trans_time"))
    .withColumn("trans_hour", F.hour("trans_ts"))
    .withColumn("trans_date", F.to_date("trans_ts"))
    .withColumn("full_name", F.concat_ws(" ", "first", "last"))
    .dropDuplicates(["trans_num"]))

silver_stream_path = f"abfss://processed@{storage_account}.dfs.core.windows.net/silver_streaming/transactions"
silver_ckpt        = f"abfss://processed@{storage_account}.dfs.core.windows.net/checkpoints/silver_streaming"

(silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", silver_ckpt)
    .start(silver_stream_path))

# stop streaming processes.
for q in spark.streams.active: q.stop()