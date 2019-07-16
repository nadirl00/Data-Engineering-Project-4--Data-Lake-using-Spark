Project summary:
----------------
A music streaming startup, Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project implements an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables that will be used by their analytics teams to discover insights into their users listening habits.



How it works:
-------------
Since the analytics team is priamrily intereted in discovering insights into the song play behaviours of their users, we implement a star schema with songplays at its centre as a fact table since it is the metric we wish to analyze and add four dimensional tables: songs, users, artists and time. This structure is optimized for queries on song play analysis.

**dl.cfg**: the configuration file that contains the parameters needed to connect to connect to an S3 bucket to read or write data to it.

**etl.py**: performs the ETL in three steps. 
First the song data is loaded from the source S3 bucket into our Spark session. It is then transformed into song and artist tables and they are written to a destination S3 buckets  using parquet files.
Then the log data is loaded from the source S3 bucket into our Spark session. This data is used to create the user and time tables and they are stored in our destination S3 bucket.
Finally, the log data is joined with the song data by reading the songtable from the destination S3 bucket to create the songplays table (since it requires song_id and artist_id, both missing from the 
log JSON files.

How to use the scripts:
-----------------------

Prerequisite to run the code:
dl.cfg must be edited with your AWS credentials in order to read from and write to an S3 bucket.

etl.py must be edited to add the address of the destination S3 bucket.

The code was run on Amazon EMR cluster configured as follows:
-`Release:` emr-5.20.0 or later
-`Applications:` Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
-`Instance type:` m3.xlarge
-`Number of instance:` 3
-`EC2 key pair:` Proceed without an EC2 key pair or feel free to use one if you'd like

To execute the ETL to read the data, process it and store the analytics tables onto an S3:
python etl.py

