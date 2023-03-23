# Ez-Merc-Intergalactic
Capstone project created for Promineo Tech's Data Engineering program. Utilized AWS tools and services to create a pipeline to move, clean, store, and analyze data that was scraped and created using Python.

## 15 Step Process:
#### Step 1: Created Business Plan
Formed necessary steps to convey the various steps in the business process 
#### Step 2: Created Logical and Physical Model
Created visual models using Visual Paradigm
#### Step 3: Set up relational database instance
Created MS SQL Server database instance in AWS RDS
#### Step 4: Connected relational database to local SQL editor
Connected to RDS instance using DBeaver
#### Step 5: Formed schema in editor
Used DDL script to create tables within database instance
#### Step 6: Created data using Python
Utilized BeautifulSoup, Faker, and ChatGPT API to generate data then stored in CSV file using Boto3
#### Step 7: Stored data in database instance
Employed the DBeaver Import Wizard to store CSV files into their respective tables
#### Step 8: Created data lake
Data lake created using AWS Lake Formation, an Amazon S3 bucket, and Glue data catalog hosted on administrator account
#### Step 9: Transferred raw data to data lake
Used Glue ETL Job to transfer raw data from RDS instance to S3 data lake, filling Glue catalog
#### Step 10: Inspected data
Inspected data in Athena to check the data quality
#### Step 11: Cleaned data
Used Glue ETL Job to correct data issues and moved clean data into data lake
#### Step 12: Created data warehouse

#### Step 13: Set up schema
#### Step 14: Ingested clean data from data lake into data warehouse
#### Step 15: Visualized data from warehouse using analyzation software
