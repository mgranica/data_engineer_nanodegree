import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,  dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"
    song_path = os.path.join(input_data, song_data)
    
    # read song data file
    df = spark.read.json(song_path)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.createOrReplaceTempView('song_table')
    songs_outpath = os.path.join(output_data, "song_table.parquet")
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_outpath, 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'\
    ).withColumnRenamed('artist_name', 'name')\
    .withColumnRenamed('artist_location', 'location')\
    .withColumnRenamed('artist_latitude', 'latitude')\
    .withColumnRenamed('artist_longitude', 'longitude').dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.createOrReplaceTempView("artist_table")
    artists_outpath = os.path.join(output_data, "artist_table.parquet")
    artists_table.write.parquet(artists_outpath, "overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data ="log_data/*.json"
    log_path = os.path.join(input_data, log_data)

    # read log data file
    df = spark.read.json(log_path)
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").orderBy("userId").dropDuplicates()
    
    # write users table to parquet files
    users_table.createOrReplaceTempView("users_table")
    users_outpath = os.path.join(output_data, "users_table.parquet")
    users_table.write.parquet(users_outpath, "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    #get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d"))
    #df = df.withColumn("start_time", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select("start_time"\
                          ,hour("start_time").alias("hour")\
                          ,dayofmonth("start_time").alias("day")\
                          ,weekofyear("start_time").alias("week")\
                          ,month("start_time").alias("month")\
                          ,year("start_time").alias("year")\
                          ,dayofweek("start_time").alias("weekday")).orderBy("start_time") 
    
    # write time table to parquet files partitioned by year and month
    time_table.createOrReplaceTempView("time_table")
    time_outpath = os.path.join(output_data, "time_table.parquet")
    time_table.write.parquet(time_outpath, "overwrite")

    # read in song data to use for songplays table
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"
    song_path = os.path.join(input_data, song_data)
    
    # read song data file
    df_song = spark.read.json(song_path) 
    
    cond = [df.song==df_song.title, df.artist==df_song.artist_name, df.length==df_song.duration]
    df_join = df.join(df_song, cond, "left_outer")\
                    .withColumn("songplay_id", monotonically_increasing_id())\
                    .withColumn("start_time", get_timestamp("ts"))\
                    .withColumnRenamed("userId", "user_id")\
                    .withColumnRenamed("sessionId", "session_id")\
                    .withColumnRenamed("userAgent", "user_agent")\

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_join.select("songplay_id", 
                              "start_time", 
                              "user_id", 
                              "level", 
                              "song_id", 
                              "artist_id", 
                              "session_id", 
                              "location", 
                              "user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.createOrReplaceTempView("songplays_table")
    songplays_outpath = os.path.join(output_data, "songplays_table.parquet")
    songplays_table.write.parquet(songplays_outpath, "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://sparkify23feb/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
