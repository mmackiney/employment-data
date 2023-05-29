# AV Challenge

## Purpose
The purpose of this project is to give our customer the data they need to analyze employee data.

## Structure
This project is structured into 3 parts. 

### Step 1: Data Syncing (Fivetran) 
The first step creates a data connector between AWS S3 and Snowflake. Data is then synced between the csv files in the S3 bucket and the corresponding Snowflake tables. AWS S3 was simply used out of convenience, but any other file/object storage service could be used in its place and easily modified in the Fivetran code.

### Step 2: Data Modeling (Snowflake)
The second step models the raw data in Snowflake for efficient queries and transformations.

### Step 3: Data Transformation (dbt)
The third and final step uses dbt to transform data into tables that can easily be used for visualizations and analysis.

## What You Need
To use this project, you will need AWS, Fivetran, and Snowflake credentials.
