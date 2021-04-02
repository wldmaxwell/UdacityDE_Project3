
**Project Three: Data Warehouse**

 

**Overview**

This project builds an ETL pipeline on AWS Cloud for a music streaming service called Sparkify by extracting data from JSON files in S3, staging the data in Redshift, and transforming the data into a set of dimensional tables. This allows the Sparkify analytics team to analyze the song and user data to answer questions like “What day of the week are users listening the most?”

**Technologies used**



*   SQL -
    *   Used for dropping, creating, inserting, copying JSON files, and running test queries 
*   Python 
    *   Boto3 - Amazon SDK
        *   Used to create IAM Role, Redshift Cluster, and run SQL Queries 
*   Redshift
    *   Data Warehouse used to Stage data from S3 and create dimension tables for analytics team
*   S3 
    *   Storage service that holds event and song data JSON files

**How to Run**



*   AWS
    *   Creating_Warehouse.ipynb
        *   Used to Create IAM Role, Redshift Cluster
    *   Create an IAM user 
        *   With permissions to use Redshift
    *   Create IAM role for Redshift with AmazonS3ReadOnlyAccess rights
        *   Need the ARN
    *   Create the Redshift cluster
        *   Get the Endpoint ---- also known as Host
*   Fill in the dwh.cfg with your info
    *   Key, Secret, Host, ARN, etc
*   Run create_tables.py
    *   Drops and Creates Tables
*   Run etl.py
    *   Copies and Inserts data into tables
*   Test_queries.ipynb
    *   Jupyter Notebook used to run test queries

**Information About Dataset**



*   s3://udacity-dend/song_data
    *   31 JSON files
*   **Song_Data Example**






![alt_text](images/Song_Data.png "image_tooltip")




*   s3://udacity-dend/log_data
    *   14897 JSON files
*   **Log_Data Example**






![alt_text](images/Log_Data.png "image_tooltip")




*   Staging_events_table
    *   8056 rows
*   Staging_songs_table
    *   14896 rows
*   User_table
    *   104 Unique Users
*   Song_table
    *   14896 Unique Songs
*   Artist_table
    *   10025 Unique Artist
*   Songplay table
    *   9957 song plays

**Queries Examples from Test_Queries.ipynb**

1. Give me the artist, song title and song's length of the top 15 songs by duration.



*   **Query:** 

		SELECT a.artist_name, s.title, s.duration 
		     
		FROM artist a

		JOIN song s on (a.artist_id = s.artist_id)

		ORDER BY s.duration DESC limit 15;



*   **Query Result Example First Row:** Jean Grae, Chapter One: Destiny, 2709

2. Give me the total number of listens each day of the week ordered in Descending Order.
	
*   **Query:**

		SELECT t.weekday, count(s.songplay_id) as number_of_listens_each_day
	
		FROM time t
	
		JOIN songplay s  on (t.start_time = s.start_time)

		GROUP BY t.weekday
	
		ORDER BY number_of_listens_each_day DESC;



*   **Query Result Example First Row:** 5, 1966
    *   Users listen the most on Saturdays

	

**Database Info and Tables**


*   **Redshift Cluster Info:** 
    *   DWH_CLUSTER_TYPE=multi-node
    *   DWH_NUM_NODES=4
    *   DWH_NODE_TYPE=dc2.large


![alt_text](images/Staging_Tables.png "image_tooltip")



![alt_text](images/Analytics_Tables.png "image_tooltip")

