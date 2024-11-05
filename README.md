# DataWareHouse_RedShift_AWS_Project

## Project Overview:

A music streaming startup, 'Sparkify', currently has JSON logs for user activity, as well as JSON metadata for songs in the application. These JSON documents currently reside in S3.

The aim of the project is to build an ETL pipeline, which will extract the data from S3, stage the data in redshift, and subsequently transform the data into a set of fact & dimension tables in redshift, which can then be used for analysis of application usage.

From an analytics perspective, the 'Sparkify' team wishes to be able to find insights into which songs their users are listening to.

# Redshift Considerations
The schema design in redshift can heavily influence the query performance associated. Some relevant areas for query performance are;

1. Defining how redshift distributes data across nodes.
2. Defining the sort keys, which can determine ordering and speed up joins.
3. Definining foreign key and primarty key constraints.

# Files

Before you run any of these files, please note that the Redshift cluster should be running & IAM role has to be created so that you will have Host & ARN details ready. 

> **create_tables.py** - Drop and recreates tables
> 
> **dwh.cfg** - Configure Redshift cluster
> 
> **etl.py** - Copy data to staging tables and insert into star schema of fact and dimension tables
> 
> **sql_queries** - 
> 
>    1. Creating and dropping staging and star schema tables
>         
>    2. Copy JSON data from S3 to Redshift staging tables
>    
>    3. Insert data from staging tables to star schema fact and dimension tables
