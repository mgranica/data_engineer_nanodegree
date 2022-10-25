import configparser
import os


# CONFIG
config = configparser.ConfigParser()
filepath = os.path.join('dwh_cluster','dwh.cfg')
config.read(filepath)

# AWS
KEY              = config.get('AWS','KEY')
SECRET           = config.get('AWS','SECRET')
DB_IAM_ROLE_NAME = config.get("CLUSTER", "DB_IAM_ROLE_NAME")
# S3
LOG_DATA         = config.get("S3", "LOG_DATA")
LOG_JSONPATH     = config.get("S3", "LOG_JSONPATH")
SONG_DATA        = config.get("S3", "SONG_DATA")
REGION_NAME      = config.get("AWS", "REGION_NAME")

region_name = "us-west-2"

iam = boto3.client('iam',aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name=REGION_NAME
                   )
# Get the IAM role ARN
roleArn = iam.get_role(RoleName=DB_IAM_ROLE_NAME)['Role']['Arn']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplay"
user_table_drop =           "DROP TABLE IF EXISTS user"
song_table_drop =           "DROP TABLE IF EXISTS song"
artist_table_drop =         "DROP TABLE IF EXISTS artist"
time_table_drop =           "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(artist VARCHAR,
                                            auth VARCHAR,
                                            firstName VARCHAR,
                                            gender VARCHAR,
                                            ItemInSession INT,
                                            lastName VARCHAR,
                                            lenght NUMERIC,
                                            level VARCHAR,
                                            location VARCHAR,
                                            method VARCHAR,
                                            page VARCHAR,
                                            registration NUMERIC,
                                            sessionid INT,
                                            song VARCHAR,
                                            status INT,
                                            ts TIMESTAMP,
                                            user/Agent VARCHAR,
                                            userId INT
                                            )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(num_songs IDENTITY(0,1), 
                                            artist_id VARCHAR, 
                                            artist_latitude NUMERIC, 
                                            artist_longitude NUMERIC, 
                                            artist_location VARCHAR, 
                                            artist_name VARCHAR, 
                                            song_id VARCHAR, 
                                            title VARCHAR, 
                                            duration NUMERIC, 
                                            year NUMERIC
                                            )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay(songplay_id IDENTITY(0,1), 
                                        start_time TIMESTAMP, 
                                        user_id VARCHAR, 
                                        level VARCHAR, 
                                        song_id VARCHAR, 
                                        artist_id VARCHAR, 
                                        session_id INT, 
                                        location VARCHAR, 
                                        user_agent VARCHAR)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user(user_id VARCHAR, 
                                    first_name VARCHAR, 
                                    last_name VARCHAR, 
                                    gender VARCHAR, 
                                    level VARCHAR)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song(song_id VARCHAR, 
                                    title VARCHAR, 
                                    artist_id VARCHAR, 
                                    year NUMERIC, 
                                    duration NUMERIC)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(artist_id VARCHAR, 
                                        name VARCHAR, 
                                        location VARCHAR, 
                                        lattitude NUMERIC, 
                                        longitude NUMERIC)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP, 
                                    hour INT, 
                                    day INT, 
                                    week INT, 
                                    month INT, 
                                    year INT, 
                                    weekday NUMERIC)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    gzip delimiter ';' 
    compupdate off 
    region 'us-west-2'
    format as json {};
""").format(LOG_DATA, DB_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""").format(SONG_DATA, DB_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT to_timestamp(se.ts) as start_time
        , se.userid as user_id
        , se.level
        , ss.song_id
        , ss.artist_id
        , se.session_id
        , se.location
        , se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss 
        ON (se.song = ss.title)
    
""")

user_table_insert = ("""
    INSERT INTO user (user_id, first_name, last_name, gender, level )
    SELECT userId    as user_id
          ,firstName as first_name
          ,lastName  as last_name
          ,gender
          ,level
    FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT song_id
          ,title
          ,artist_id
          ,year
          ,duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, lattitude, longitude)
    SELECT artist_id
          ,artist_name      as name
          ,artist_location  as location
          ,artist_latitude  as latitude
          ,artist_longitude as longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT to_timestamp(se.ts)             as start_time
          ,EXTRACT(hour FROM start_time)   as hour
          ,EXTRACT(day FROM start_time)    as day
          ,EXTRACT(week FROM start_time)   as week
          ,EXTRACT(month FROM start_time)  as month
          ,EXTRACT(year FROM start_time)   as year
          ,EXTRACT(ISODOW FROM start_time) as weekday
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
