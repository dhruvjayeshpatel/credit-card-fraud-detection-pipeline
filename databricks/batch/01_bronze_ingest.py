# Authenticate to ADLS using the secret from Key Vault
storage_account = "" 
key = dbutils.secrets.get(scope="fraud-kv-scope", key="adls-access-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", key
)

# Read raw CSV
raw_path = f"abfss://raw@{storage_account}.dfs.core.windows.net/credit_card/fraudTrain.csv"
df = spark.read.csv(raw_path, header=True, inferSchema=True)

print("Columns as read:", df.columns)   # you'll spot 'Unnamed: 0' here

# Drop the junk index column (whatever it's called)
for junk in ["Unnamed: 0", "_c0"]:
    if junk in df.columns:
        df = df.drop(junk)

# Safety net: replace any remaining invalid characters in column names
import re
df = df.toDF(*[re.sub(r"[ ,;{}()\n\t=]", "_", c) for c in df.columns])

print(f"Rows: {df.count()}")
df.printSchema()

# Write to bronze as Delta
bronze_path = f"abfss://processed@{storage_account}.dfs.core.windows.net/bronze/transactions"
df.write.format("delta").mode("overwrite").save(bronze_path)