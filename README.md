# Credit Card Fraud Detection Pipeline on Azure (Batch + Streaming)

An end-to-end data engineering project on Microsoft Azure — a hybrid
batch + streaming pipeline covering ingestion, transformation,
star-schema modeling, and Power BI reporting.

## Tech Stack

Azure Event Hubs | Azure Databricks (PySpark, Structured Streaming) |
ADLS Gen2 | Azure Data Factory | Azure Synapse (Serverless SQL) |
Power BI | Azure Key Vault | SQL

## Batch Pipeline ✅

Raw CSV → ADLS Gen2 → Databricks (bronze → silver → gold, Delta Lake)
→ Synapse Serverless SQL views → Power BI dashboard,
orchestrated by Azure Data Factory.

- Medallion architecture (raw / processed / curated)
- Star schema: FactTransaction + DimCustomer, DimMerchant, DimDate
- Key Vault–secured credentials, managed identity access to storage

## Streaming Pipeline 🚧

Python producer → Event Hubs → Databricks Structured Streaming → Delta.

## Repository Structure

```
├── scripts/          # data prep + event producer
├── databricks/       # batch & streaming notebooks
├── adf/              # pipeline definition
├── synapse/          # SQL view scripts
├── powerbi/          # dashboard + screenshots
└── docs/             # setup notes
```