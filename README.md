# T-Shop
## Project Description:
T-Shop is an online clothing store from a parallel universe. It has 500,000 customers, the number of which is constantly growing.

## Project Objective:
Build a simplified full-cycle process (from raw data generation to visualisation).

## Requirements:
Need to generate tables and pour them into PostgreSQL. In it write a package that makes other tables with SCD2 versioning based on raw tables. There should also be a table with metadata for calculating increment from RAW-layer (only those records with update time higher than the maximum update time in the target table on DDS-layer are selected). A final Data Mart is then built in the CDM layer (also from SCD2).
This data is then poured into Clickhouse, and then used in Superset to build the dashboard. 
Everything should run under AirFlow (except Superset). The implementation is Docker container based (except for the data generator).

## Tools used:
- Python (data generation)
- PostgreSQL (primary data storage)
- Greenplum (data warehouse)
- Clickhouse (storage of the final Data Mart)
- Superset (visualisation)
- AirFlow (flow orchestration)
- Docker (ensuring isolation)
- k3s / Docker Swarm (container orchestration)

## Layers:
1. RAW (SCD1 sources)
2. DDS (sources with SCD2)
3. CDM (showcase with SCD2)
4. SL (metadata)


## Dashboard

The constructed dashboard should show:
- Revenue chart
- Revenue chart by category
- Pie chart of revenue by age
- Amount of revenue
- Number of customers
- Average number of items per cheque
- Average amount of cheque
- Average age of customers
- Most popular size
