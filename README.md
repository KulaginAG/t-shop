# T-Shop
## Project Description:
T-Shop is an online clothing shop from a parallel universe. It has 5 million customers, the number of which is constantly growing. Let's imagine that the daily conversion to purchase is 1% on weekdays and 5% on weekends. Several products in different quantities can be purchased as part of a purchase.

## Project Objective:
Build a simplified full-cycle ETL process (from raw data generation to visualisation).

## Requirements:
I need to generate 3 tables (customer, product, sales) and pour them into PostgreSQL (RAW-layer). In it write a package that makes other tables with SCD2 versioning based on raw tables. There should also be a table with metadata for calculating increment from RAW-layer (only those records with update time higher than the maximum update time in the target table on DDS-layer are selected). A final datamart is then built in the CDM layer (also from SCD2).
This data is then poured into Clickhouse, and then used in Superset to build the dashboard. 
Everything should run under AirFlow (except Superset). The implementation is Docker container based (except for the data generator).

## Tools used:
- Python (data generation)
- PostgreSQL (data storage)
- Clickhouse (storage of the final showcase)
- Superset (visualisation)
- AirFlow (flow orchestration)
- Docker (ensuring isolation)

## Layers:
1. RAW (SCD1 sources)
2. DDS (sources with SCD2)
3. CDM (showcase with SCD2)
4. SL (metadata)

## Entities:

**Customer**
- ID
- First Name
- Last Name
- Date of birth
- Record update time

**Product**
- ID
- Name
- Category
- Size
- Price
- Record update time

**Sales**
- ID
- Date
- Customer
- Item
- Quantity
- Record update time

**Datamart**:
- Date of sale
- Customer
- Customer Age
- Product
- Product category
- Size
- Product price
- Quantity of goods
- Total cost

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
