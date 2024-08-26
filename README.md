# real_estate_etl_with_pgadmin4
Zipco Real Estate Data ETL Script Documentation
Overview
This Python script performs an ETL (Extract, Transform, Load) process on real estate property data obtained via an API. The data is extracted from the Realty Mole Property API, transformed to fit into a relational database schema, and loaded into a PostgreSQL database. The script is modular, with distinct functions for each step of the ETL process.
1. Extraction Layer
Function: extract_data()
Description: Responsible for extracting data from the Realty Mole Property API using the requests library. Data is retrieved in JSON format and stored locally as rawData.json.
Endpoint: Queries the randomProperties endpoint to fetch up to 200,000 property records.
Authentication: Includes an API key and host information.
Error Handling: Checks the HTTP status code of the API response. If successful (status_code == 200), data is saved as a JSON file; otherwise, an error message is printed.
2. Transformation Layer
Function: transform_data(file_name)
Process:
Loading Raw Data: Loads raw JSON data into a Pandas DataFrame.
Flattening Nested Data: Normalizes nested columns such as 'features', 'taxAssessment', 'propertyTaxes', etc. into separate columns. Utilizes dynamic renaming based on year-specific data (e.g., '2023.value' to 'value_2023').
Handling Missing Values: Fills missing values with predefined placeholders. Removes irrelevant or incomplete data fields.
Renaming Columns: Converts all column names from camelCase to snake_case for SQL consistency.
Data Partitioning: Splits transformed data into four distinct DataFrames:
property_dim: Contains property-specific information like bedrooms, bathrooms, and square footage.
location_dim: Stores geographical details such as addresses and coordinates.
ownership_dim: Includes legal information such as owner details and legal descriptions.
financials_dim: Houses financial details including yearly tax assessments and sale prices.
3. Database Connection Setup
Function: get_db_connection()
Purpose: Establishes a PostgreSQL database connection using psycopg2.
Connection Parameters: Requires host, database name, username, and password.
Error Handling: Prints an error message if the connection fails, specifying the issue.
4. Schema and Table Creation
Function: setup_database(conn)
Process:
Schema Creation: Creates a schema named zipco_real_estate to group the tables logically.
Table Creation: Constructs tables like property_features, location, legal_ownership, and sales with appropriate columns matching the structure of the DataFrames.
Error Handling: Catches and prints SQL errors to ensure that the database setup does not proceed if an issue arises.
5. Data Loading
Function: load_data_to_table(engine, df, table_name)
Method: Uses an SQLAlchemy engine to perform bulk inserts of DataFrame data into corresponding database tables.
Data Integrity: Inserts data with if_exists='append' parameter to add new data without overwriting existing records.
Error Handling: Catches exceptions during data loading and provides a detailed error message.
6. Main Execution Flow
Description: Coordinates the ETL process by sequentially invoking the extraction, transformation, database setup, and data loading functions:
Step 1: extract_data() fetches raw data.
Step 2: transform_data(file_name) cleans and structures the data.
Step 3: get_db_connection() and setup_database(conn) prepare the database.
Step 4: load_data_to_table(engine, df, table_name) loads data into the database.

Conclusion
This script efficiently manages the ETL process for real estate property data, transforming raw API data into a structured relational database format. It emphasizes data integrity through meticulous error handling and transformation, preparing the data for analytical or further processing purposes.
