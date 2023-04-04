import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
artist          TEXT       ,
auth            VARCHAR(20),
firstName       VARCHAR(25),
gender          VARCHAR(5) ,
itemInSession   INTEGER    ,
lastName        VARCHAR(25),
length          NUMERIC    ,
level           VARCHAR(10),
location        TEXT       ,
method          VARCHAR(10),
page            VARCHAR(20),
registration    NUMERIC    ,
sessionId       INTEGER    ,
song            TEXT       ,
status          INTEGER    ,
ts              BIGINT     ,
userAgent       TEXT       ,
userId          TEXT
)
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
song_id             TEXT   ,
num_songs           INTEGER,
title               TEXT   ,
artist_name         TEXT   ,
artist_latitude     NUMERIC,
year                INTEGER,
duration            NUMERIC,
artist_id           TEXT   ,
artist_longitude    NUMERIC,
artist_location     TEXT      
)
""")

songplay_table_create = ("""CREATE TABLE songplay (
songplay_id     BIGINT         IDENTITY(0,1) PRIMARY KEY,
start_time      TIMESTAMP      NOT NULL      SORTKEY    ,
user_id         TEXT           NOT NULL      DISTKEY    ,
level           VARCHAR(10)                             ,
song_id         TEXT                                    ,
artist_id       TEXT                                    ,
session_id      INTEGER                                 ,
location        TEXT                                    ,
user_agent      TEXT           
)
""")

user_table_create = ("""CREATE TABLE users (
user_id          TEXT           NOT NULL PRIMARY KEY SORTKEY,
first_name       VARCHAR(25)                                ,
last_name        VARCHAR(25)                                ,
gender           VARCHAR(5)                                 ,
level            VARCHAR(10)     
)
""")

song_table_create = ("""CREATE TABLE songs (
song_id         TEXT           NOT NULL PRIMARY KEY SORTKEY,
title           TEXT                                       ,
artist_id       TEXT                                DISTKEY,
year            INTEGER                                    ,
duration        NUMERIC        
)
""")

artist_table_create = ("""CREATE TABLE artists (
artist_id           TEXT           NOT NULL PRIMARY KEY SORTKEY,
name                TEXT                                       ,
location            TEXT                                       ,
latitude            NUMERIC                                    ,
longitude           NUMERIC        
)
""")

time_table_create = ("""CREATE TABLE time (
start_time      TIMESTAMP      NOT NULL PRIMARY KEY SORTKEY,
hour            SMALLINT                                   ,
day             SMALLINT                                   ,
week            SMALLINT                                   ,
month           SMALLINT                                   ,
year            SMALLINT                                   ,
weekday         SMALLINT    
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2' JSON {}
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2' JSON 'auto';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') AS ts_convert, ts FROM staging_events)
SELECT DISTINCT t.ts_convert    AS   start_time,
                e.userId        AS   user_id   ,
                e.level                        ,
                s.song_id                      ,
                s.artist_id                    ,
                e.sessionId     AS   session_id,
                e.location                     ,
                e.userAgent     AS   user_agent 
FROM staging_events e JOIN staging_songs s ON (e.artist = s.artist_name) JOIN temp_time t ON t.ts = e.ts
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId        AS   user_id   ,
                firstName     AS   first_name,
                lastname      AS   last_name ,
                gender                       ,
                level                        
FROM staging_events 
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id  ,
                title    ,
                artist_id,
                year     ,
                duration                      
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id                         ,
                artist_name         AS   name     ,
                artist_location     AS   location ,
                artist_latitude     AS   latitude ,
                artist_longitude    AS   longitude
FROM staging_songs 
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') AS ts FROM staging_events)
SELECT DISTINCT                           t.ts   ,
       EXTRACT(hour    FROM t.ts)    AS   hour   ,
       EXTRACT(day     FROM t.ts)    AS   day    ,
       EXTRACT(week    FROM t.ts)    AS   week   ,
       EXTRACT(month   FROM t.ts)    AS   month  ,
       EXTRACT(year    FROM t.ts)    AS   year   ,
       EXTRACT(weekday FROM t.ts)    AS   weekday      
FROM temp_time t JOIN songplay s ON t.ts = s.start_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
