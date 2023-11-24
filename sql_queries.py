import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# added variables to get configs
ROLE_ARN = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
REGION_NAME = config.get('S3', 'REGION_NAME')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id BIGINT IDENTITY(0,1) NOT NULL,
        artist VARCHAR NULL,
        auth VARCHAR NULL,
        first_name VARCHAR NULL,
        gender VARCHAR NULL,
        item_in_session VARCHAR NULL,
        last_name VARCHAR NULL,
        length VARCHAR NULL,
        level VARCHAR NULL,
        location VARCHAR NULL,
        method VARCHAR NULL,
        page VARCHAR NULL,
        registration VARCHAR NULL,
        session_id INTEGER NULL,
        song VARCHAR NULL,
        status INTEGER NULL,
        ts BIGINT NULL,
        user_agent VARCHAR NULL,
        user_id INTEGER NULL
        )
        DISTSTYLE EVEN;
    """)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        staging_song_id BIGINT IDENTITY(0,1),
        num_songs INTEGER NOT NULL,
        artist_id VARCHAR NULL,
        artist_latitude FLOAT NULL,                      
        artist_longitude FLOAT NULL,
        artist_location VARCHAR NULL,
        artist_name VARCHAR NULL,
        song_id VARCHAR NULL,
        title VARCHAR NULL,
        duration FLOAT NULL,
        year INTEGER NULL
        )
        DISTSTYLE EVEN;
    """)

# CREATE FINAL TABLES
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR(50) NOT NULL,
        level VARCHAR(20) NOT NULL,
        song_id VARCHAR(50) NOT NULL,
        artist_id VARCHAR(50) NOT NULL,
        session_id VARCHAR(50) NOT NULL,
        location VARCHAR(100) NULL,
        user_agent VARCHAR(255) NULL
    ) DISTKEY (user_id)
      SORTKEY (user_id, start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL,
        first_name VARCHAR(50) NULL,
        last_name VARCHAR(50) NULL,
        gender VARCHAR(20) NULL,
        level VARCHAR(20) NULL
    ) DISTKEY (user_id)
      SORTKEY (user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(50) NOT NULL,
        title VARCHAR(200) NOT NULL,
        artist_id VARCHAR(50) NOT NULL,
        year INTEGER NOT NULL,
        duration DECIMAL(9) NOT NULL
    ) DISTKEY (song_id)
      SORTKEY (song_id, year);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(50) NOT NULL,
        name VARCHAR(500) NULL,
        location VARCHAR(200) NULL,
        latitude DECIMAL(9) NULL,
        longitude DECIMAL(9) NULL
    ) DISTKEY (artist_id)
      SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL,
        hour SMALLINT NULL,
        day SMALLINT NULL,
        week SMALLINT NULL,
        month SMALLINT NULL,
        year SMALLINT NULL,
        weekday SMALLINT NULL
    ) DISTKEY (start_time)
      SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT as JSON {}
    REGION {};
""").format(LOG_DATA, ROLE_ARN, LOG_JSONPATH, REGION_NAME)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT as JSON 'auto'
    REGION {};
""").format(SONG_DATA, ROLE_ARN, REGION_NAME)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id,
                                        location, user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + t1.ts/1000 * INTERVAL '1 second'   AS start_time,
            t1.user_id AS user_id,
            t1.level AS level,
            t2.song_id AS song_id,
            t2.artist_id AS artist_id,
            t1.session_id AS session_id,
            t1.location AS location,
            t1.user_agent AS user_agent
    FROM staging_events AS t1
    JOIN staging_songs AS t2
        ON t1.artist = t2.artist_name
        AND t1.song = t2.title
    WHERE page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT t1.user_id AS user_id,
            t1.first_name AS first_name,
            t1.last_name AS last_name,
            t1.gender AS gender,
            t1.level AS level
    FROM staging_events AS t1
    WHERE t1.user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT t1.song_id AS song_id,
        t1.title AS title,
        t1.artist_id AS artist_id,
        t1.year AS year,
        t1.duration AS duration
    FROM staging_songs AS t1
    WHERE t1.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT t1.artist_id AS artist_id,
            t1.artist_name AS name,
            t1.artist_location AS location,
            t1.artist_latitude AS latitude,
            t1.artist_longitude AS longitude
    FROM staging_songs AS t1
    WHERE t1.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + t1.ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(week FROM start_time) AS weekday
    FROM staging_events AS t1
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
