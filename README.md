# Udacity Data Engineering with AWS Nanodegree project - Cloud Data Warehouse

## 1. Project description
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The project aims to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

<center>
<img style="float: center;height:450px;" src="images/sparkify-s3-to-redshift-etl.png"><br><br>
Sparkify S3 to Redshift ETL
</center>

## 2. Schema for Song Play Analysis

### Fact Table
- **songplays** - records in event data associated with song plays i.e. records with page "NextSong"
 + *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*
  
### Dimension Tables
- **users** - users in the app
 + *user_id, first_name, last_name, gender, level*
- **songs** - songs in music database
 + *song_id, title, artist_id, year, duration*
- **artists** - artists in music database
 + *artist_id, name, location, lattitude, longitude*
- **time** - timestamps of records in songplays broken down into specific units
 + *start_time, hour, day, week, month, year, weekday*

## 3. Files

**`Cloud DWH project.ipynb`** This notebook describes **8 STEPS** of the project.
- STEP 0: Save the AWS Access key
- STEP 1: Create a new IAM Role¶
- STEP 2: Create a Redshift Cluster
- STEP 3: Open an incoming TCP port to access the cluster endpoint
- STEP 4: Run `create_tables.py` to create staging, fact, and dimension tables in Redshift database
- STEP 5: Run `etl.py` to load data from S3 to staging tables on Redshift and from staging tables to analytics tables on Redshift.
- STEP 6: Run the analytic queries and compare the results to the expected results.
- STEP 7: Clean up resources
 
**`create_tables.py`** This python file is used to drop tables if they exist and create tables.

**`dwh.cfg`** This file contains the following data:
- Configuration of Redshift cluster
- role ARN - Allow Redshift able to access S3 bucket (ReadOnly)
- S3 links for **3 datasets** that reside in S3
- The pair of access key ID and secret

**`etl.py`** This python file is used to load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

**`sql_queries.py`** This python file is used to define SQL statements, which will be imported into **`dwh.cfg`** and **`etl.py`**