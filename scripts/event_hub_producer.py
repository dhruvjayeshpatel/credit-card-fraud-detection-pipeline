import json
import time
from pathlib import Path

import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData

# --- Config ---
CONN_STR = ""   
EVENTHUB_NAME = "transactions"
BATCH_SIZE = 50          # events per send
DELAY_SECONDS = 1        # pause between batches -> ~50 events/sec -> full file in ~3.5 min

df = pd.read_csv("fraudTest.csv")

# Sort chronologically so the "stream" arrives in realistic order
df = df.sort_values("unix_time").reset_index(drop=True)

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONN_STR, eventhub_name=EVENTHUB_NAME
)

sent = 0
try:
    for start in range(0, len(df), BATCH_SIZE):
        chunk = df.iloc[start:start + BATCH_SIZE]
        batch = producer.create_batch()
        for _, row in chunk.iterrows():
            batch.add(EventData(json.dumps(row.to_dict())))
        producer.send_batch(batch)
        sent += len(chunk)
        print(f"Sent {sent}/{len(df)} events", end="\r")
        time.sleep(DELAY_SECONDS)
finally:
    producer.close()

print(f"\nDone. {sent} events sent to '{EVENTHUB_NAME}'.")  