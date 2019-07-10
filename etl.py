import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')


#read the credentials from dl.cfg
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
"""
This function creates and instance of SparkSession.
"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
"""
This function receives a spark object, the address of the S3 bucket containing the song data. It processes the json files to extract the song and artist data and stores the resulting dataframes as parquet files in the S3 bucket provided in output_data parameter.

"""
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = df = spark.read.json(song_data)

    # extract columns to create songs table
    # remove the rows with duplicate song ids
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').where(df.song_id !='').dropDuplicates(['song_id'])
    
    print("creating song_table with %s rows." % songs_table.count())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet(output_data + "/songs_table/songs_table.parquet")

    # extract columns to create artists table
    # remove rows with duplicate artist ids
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude').where(df.artist_id !='').dropDuplicates(['artist_id'])
    
    print("creating artists_table with %s rows." % artists_table.count())
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artists_table/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
"""
This function receives a spark object, the address of the S3 bucket containing the log data. It processes the json files to build the user and time tables. It then uses the output_data parameter to read the merge the song table with the log table to build the songplays table. The resulting are saved as parquet files in the S3 bucket provided in output_data parameter.

"""
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df =  spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df.page=='NextSong']

    # extract columns for users table  
    # sort the rows in descending order using the timestamp column to ensure the most recent data  
    # comes first, select the required columns, filter out the users with no user ids, drop the 
    # duplicates to keep only the most recent entries first in order to ensure the level data is 
    # up to date
    
    users_table =  df.sort(df.ts.desc()).select('userId','firstName', 'lastName', 'gender','level').where(df.userId!='').dropDuplicates(['userId'])
    
    print("creating users_table with %s rows." % users_table.count())
 
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users_table/users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x / 1000.0),IntegerType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn('start_time', from_unixtime(df.timestamp))
    
    # extract columns to create time table
    # get the timestamps, drop the duplicates and extract the required columns.
    time_table = df.dropDuplicates(['start_time']).select('start_time').withColumn('hour', hour(df.start_time)).withColumn('day', date_format(df.start_time,'d')).withColumn('week', weekofyear(df.start_time)).withColumn('month', month(df.start_time)).withColumn('year', year(df.start_time)).withColumn('weekday', date_format(df.start_time,'u'))

    
    print("creating time_table with %s rows." % time_table.count())
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet(output_data + "/time_table/time_table.parquet")

    # read in song data from the destination S3 bucket to use for songplays table
    sdf = spark.read.parquet(output_data + "/songs_table/songs_table.parquet")
    
    # do a left join on both song title to add song id and artist id to the songplays table
    song_df = df.join(sdf, df.song == sdf.title, how='left')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.select('start_time','userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location','userAgent')
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id()).withColumn('month', month(df.start_time)).withColumn('year', year(df.start_time))

    print("creating songplays_table with %s rows." % songplays_table.count())
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month']).parquet(output_data + "/songplays_table/songplays_table.parquet")


def main():
    spark = create_spark_session()
    
    #local test dataset
    #song_data = "data/song_data/*/*/*/*.json"
    #log_data = "data/log_files/*.json"
    #output_data = "output"
    
    #test on dataset subset
    #song_data = "s3a://udacity-dend/song-data/A/A/*/*.json"
    #log_data = "s3a://udacity-dend/log-data/*/*/*.json"
    #ouput_data = "s3a://dend-p4-data-lake"

    #full dataset
    song_data = "data/song_data/*/*/*/*.json"
    log_data = "s3a://udacity-dend/log-data/*/*/*.json"
    
    #make sure you update the output_data parameter with the proper S3 bucket address
    ouput_data = "s3a://dend-p4-data-lake"

    process_song_data(spark, song_data, output_data)    
    process_log_data(spark, log_data, output_data)
   

if __name__ == "__main__":
    main()
