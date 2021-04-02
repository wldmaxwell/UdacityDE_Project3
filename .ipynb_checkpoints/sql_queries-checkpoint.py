import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"

staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

songplay_table_drop = "DROP TABLE IF EXISTS songplay"

user_table_drop = "DROP TABLE IF EXISTS users"

song_table_drop = "DROP TABLE IF EXISTS song"

artist_table_drop = "DROP TABLE IF EXISTS artist"

time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Resources
# --------------------------------

# Choosing the Best Distribution Style
# https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html

#https://aws.amazon.com/about-aws/whats-new/2019/03/amazon-redshift-now-automatically-picks-the-best-distribution-#st/#:~:text=Amazon%20Redshift%20can%20now%20automatically,size%20of%20the%20table%20data.&text=When%20using%20CREATE%20TABLE%2C%20if,choose%20the%20appropriate%20distribution%20style.

# Creating Tables in Redshift with Identity Column
# https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

# Using Bigint insead of int
# https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html

# Does Redshift need Primary Keys
# https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html




#STAGING TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id bigint Identity (0,1) NOT NULL,
        artist varchar                 NULL,
        auth varchar                   NULL,
        first_name varchar             NULL,
        gender varchar                 NULL,
        item_in_session integer        NULL,
        last_name varchar              NULL,
        length decimal                 NULL,
        level varchar                  NULL,
        location varchar               NULL,
        method varchar                 NULL,
        page varchar                   NULL,
        registration varchar           NULL,
        session_id integer             NOT NULL sortkey distkey,
        song varchar                   NULL,
        status integer                 NULL,
        ts bigint                      NOT NULL,
        user_agent varchar             NULL,
        user_id integer                NULL
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id varchar          NOT NULL sortkey distkey,
        artist_latitude decimal    NULL,
        artist_longitude decimal   NULL,
        artist_location varchar    NULL,
        artist_name varchar        NULL,
        duration decimal           NULL,
        num_songs integer          NULL,
        song_id varchar            NOT NULL,
        title varchar              NULL,
        year integer               NULL
        )
""")


# DIMENSION AND FACT TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id integer Identity (0, 1)  NOT NULL sortkey,
        start_time timestamp             NOT NULL,
        user_id integer                  NOT NULL distkey,
        level varchar                    NOT NULL,
        song_id varchar                  NOT NULL,
        artist_id varchar                NOT NULL,
        session_id integer               NOT NULL,
        location varchar                     NULL,
        user_agent varchar                   NULL
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer       NOT NULL sortkey distkey,
        first_name varchar    NULL,
        last_name varchar     NULL,
        gender varchar        NULL,
        level varchar         NULL
    ) 
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id varchar         NOT NULL sortkey,
        title varchar           NULL,
        artist_id varchar       NULL,
        year integer            NULL,
        duration decimal        NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id varchar     NOT NULL sortkey,
        artist_name varchar       NULL,
        location varchar          NULL,
        latitude decimal         NULL,
        longitude decimal         NULL
    )
""")

time_table_create = ("""
   CREATE TABLE IF NOT EXISTS time (
       start_time timestamp     NOT NULL sortkey,
       hour integer                 NULL,
       day integer                  NULL,
       week integer                 NULL,
       month integer                NULL,
       year integer                 NULL,
       weekday integer              NULL
   )
   
""")

# STAGING TABLES

# Copy JSON
# https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
# https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2'
""").format(SONG_DATA, ARN)

# FINAL TABLES

# Redshift Epochs and Timestamps
#https://www.fernandomc.com/posts/redshift-epochs-and-timestamps/
# ts is in Miliseconds so divide by 1000 to convert to seconds

songplay_table_insert = ("""
INSERT INTO songplay ( 
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
SELECT 
    DISTINCT timestamp 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM 
    staging_events AS e
JOIN staging_songs AS s ON (e.artist = s.artist_name)
WHERE e.page = 'NextSong';
    
""")

user_table_insert = ("""
INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
        )
SELECT 
        DISTINCT user_id,
        first_name,
        last_name,
        gender,
        level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song (
        song_id,
        title,
        artist_id,
        year,
        duration
        )
SELECT 
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
    
""")

artist_table_insert = ("""
INSERT INTO artist (
        artist_id,
        artist_name,
        location,
        latitude,
        longitude)
SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
       start_time,
       hour,
       day,
       week,
       month,
       year,
       weekday)
SELECT
    DISTINCT timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
    EXTRACT(hour from start_time) as hour,
    Extract(day from start_time) as day,
    EXTRACT(week from start_time) as week,
    EXTRACT(month from start_time) as month,
    EXTRACT(year from start_time) as year,
    EXTRACT(weekday from start_time) as weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,  user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
