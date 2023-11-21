# multinational-retail-data-centralisation
## Objectives

The project objective is to centralised the sales data for an organisations. 
The data needs to be accessible by team members of the organisation. You work for a multinational company that sells various goods across the globe.

The first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. We used a postgresQL database to store the relevant database tables. Develop the star-based schema of the database, ensuring that the columns are of the correct data types.
The tables, dim_users_table, dim_store_details, dim_products, dim_date_times, dim_card_details were casted to the correct data types.
The five tables; dim_users_table, dim_store_details, dim_products, dim_date_times, dim_card_details will be linked to the central table orders_table.

After storing this data we can then obtain some key metrics that will drive business decisions. These queries can be found in 

## Future work
- Optimise the sql queries. 
- modularise the metthods within data_cleaning so that there is less redundancy in the methods.