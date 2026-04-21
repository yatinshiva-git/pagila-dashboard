# Pagila Analytics Dashboard

A full-stack analytics dashboard built on Azure PostgreSQL.
Demonstrates cloud migration, replication, and HA concepts.


## Features
- Live analytics dashboard (6 tabs)
- Overview: DB health and record counts
- Films: Top 10 rented films by category
- Customers: Top 10 customers by revenue
- Revenue: Monthly trends from partitioned tables
- Replication Monitor: Live sync status local → Azure
- Failover Simulation: HA demonstration

## Tech Stack
- Python 3.12 + Flask 3.1
- PostgreSQL 16 (Local WSL2 + Azure Flexible Server)
- psycopg2 (PostgreSQL driver)
- Chart.js (revenue visualizations)
- Azure PostgreSQL Flexible Server (eastus2)
- Azure App Service (deployment)

## Author
Sudhakar — DBA → Data Platform Engineer -
AZ-104 Certified | PostgreSQL | Python | Azure
