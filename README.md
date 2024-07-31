Data Integration and Transformation Pipeline
This project is a Python-based data pipeline designed to extract, transform, and load data from multiple providers JSON files into MySQL and PostgreSQL databases. It supports combining data from multiple sources, validating and transforming it, and then inserting it into relational databases which is MySQL and PostgreSQL.

Architecture
The pipeline consists of the following components:
As an example the pipeline orchestrateted by using airflow.

Data Extraction: Uploads and reads multiple JSON files.
Validation: Ensures data integrity by checking for missing columns and validating data types.
Transformation: Converts raw data into structured formats suitable for database insertion.
Table Creation: Creates necessary tables in MySQL and PostgreSQL databases.
Data Insertion: Inserts transformed data into the respective tables.
Design Decisions
Class-Based Approach: The pipeline uses classes to encapsulate different aspects of the data processing workflow:
DataProvider: Manages data extraction from JSON files.
Category, Chain, Hotel: Data models representing the entities.
Transformer: Handles data validation and transformation.
Logging: Configured logging to capture errors and track the pipeline's execution.
Error Handling: Includes error handling for file operations, data validation, and database interactions.
Implementation Details
Classes and Functions
DataProvider:

__init__: Initializes with file paths.
extract_data: Reads JSON files and combines data into a DataFrame.
Category, Chain, Hotel:

Represents data models for categories, chains, and hotels.
Provides a string representation for debugging purposes.
Transformer:

__init__: Initializes with a DataFrame.
validate_data: Checks for missing columns and validates data.
validate_category_chain: Validates category and chain data.
validate_location: Validates location data.
transform: Transforms data into separate DataFrames and lists.
get_transformed_data: Returns transformed data in a dictionary format.
connect_to_mysql(): Establishes a connection to a MySQL database.

connect_to_postgresql(): Establishes a connection to a PostgreSQL database.

create_tables_mysql(cursor): Creates tables in MySQL.

create_tables_postgresql(cursor): Creates tables in PostgreSQL.

insert_data_mysql(cursor, data): Inserts data into MySQL tables.

insert_data_postgresql(cursor, data): Inserts data into PostgreSQL tables.

Main Function
main():
Orchestrates the data pipeline by:
Extracting data using DataProvider.
Transforming data using Transformer.
Connecting to MySQL and PostgreSQL databases.
Creating necessary tables.
Inserting transformed data into the databases.
Committing transactions and closing database connections.
Setup and Usage
pandas: For data manipulation and analysis.
json: This is part of the Python standard library, so no installation is required.
logging: This is also part of the Python standard library, so no installation is required.
mysql-connector-python: For connecting to MySQL databases.
psycopg2: For connecting to PostgreSQL databases.
pytest: For writing and running unit tests.
pyspark: For working with Spark.

Install required packages:
bash
Copy code
pip install pandas mysql-connector-python psycopg2 pytest pyspark

Requirements
Python Libraries:

pandas
json
google.colab
mysql-connector-python
psycopg2
logging
unittest
pyspark
Database:

MySQL
PostgreSQL

Configure Database Connections:

Update the connect_to_mysql and connect_to_postgresql functions with your database credentials.
Upload JSON Files:

Use the file upload feature in Google Colab to upload your JSON files.
Run the Script:

Execute the script in your Python environment.
Monitor Logs:

Check the logs for any errors or issues during execution.
