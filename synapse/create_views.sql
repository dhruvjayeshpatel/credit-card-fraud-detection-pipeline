/* ============================================================
   Credit Card Fraud Detection - Synapse Serverless SQL Layer
   ------------------------------------------------------------
   Purpose : Expose the gold-layer star schema (Delta tables in
             ADLS Gen2) as SQL views for Power BI consumption.
   Pool    : Built-in serverless SQL pool (pay-per-query, no
             dedicated infrastructure)
   Access  : Uses the Synapse workspace Managed Identity to read
             storage, so any authorized login (incl. SQL auth)
             can query the views.
   Prereq  : Synapse workspace managed identity must have the
             "Storage Blob Data Reader" role on the storage
             account.
   ============================================================ */

-- 1. Create a database
CREATE DATABASE fraud_detection_db;
GO

USE fraud_detection_db;
GO

-- 2. Master key: required one-time setup before creating
--    database-scoped credentials (encrypts them at rest)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<STRONG_PASSWORD>';
GO

-- 3. Credential: tells Synapse to authenticate to storage as the workspace's own managed identity
CREATE DATABASE SCOPED CREDENTIAL synapse_mi
WITH IDENTITY = 'Managed Identity';
GO

-- 4. External data source: reusable pointer to the curated (gold) container, bound to the managed identity credential
CREATE EXTERNAL DATA SOURCE gold_ds
WITH (
    LOCATION   = 'https://<STORAGE_ACCOUNT>.dfs.core.windows.net/curated',
    CREDENTIAL = synapse_mi
);
GO

/* ------------------------------------------------------------
   5. Star schema views (Delta format read via OPENROWSET)
   ------------------------------------------------------------ */

-- Fact table: one row per transaction
CREATE OR ALTER VIEW vw_fact_transaction AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/fact_transaction/',
    DATA_SOURCE = 'gold_ds',
    FORMAT = 'DELTA'
) AS f;
GO

-- Dimension: customers (one row per card holder)
CREATE OR ALTER VIEW vw_dim_customer AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/dim_customer/',
    DATA_SOURCE = 'gold_ds',
    FORMAT = 'DELTA'
) AS c;
GO

-- Dimension: merchants (one row per merchant, with category)
CREATE OR ALTER VIEW vw_dim_merchant AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/dim_merchant/',
    DATA_SOURCE = 'gold_ds',
    FORMAT = 'DELTA'
) AS m;
GO

-- Dimension: calendar dates
CREATE OR ALTER VIEW vw_dim_date AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/dim_date/',
    DATA_SOURCE = 'gold_ds',
    FORMAT = 'DELTA'
) AS d;
GO

-- 6. Allow non-AAD logins to use
--    the managed identity credential when querying the views
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::synapse_mi TO PUBLIC;
GO

/* ------------------------------------------------------------
   7. Validation queries (run after setup to verify)
   ------------------------------------------------------------ */

-- Fraud vs legit summary
SELECT is_fraud,
       COUNT(*)        AS txn_count,
       AVG(amt)        AS avg_amount,
       SUM(amt)        AS total_amount
FROM vw_fact_transaction
GROUP BY is_fraud;

-- Fraud count by merchant category (joins fact to dimension)
SELECT m.category,
       COUNT(*) AS fraud_txns
FROM vw_fact_transaction f
JOIN vw_dim_merchant m
  ON f.merchant_key = m.merchant_key
WHERE f.is_fraud = 1
GROUP BY m.category
ORDER BY fraud_txns DESC;