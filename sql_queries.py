import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE table IF NOT EXISTS staging_events 
(artist          TEXT,
auth             TEXT,
first_name       TEXT,
gender           TEXT,
item_in_session  int,
last_name        TEXT,
length           float,
level            TEXT,
location         TEXT,
method           TEXT,
page             TEXT,
registration     BIGINT,
session_id       integer,
song             TEXT,
status           integer,
ts               bigint,
user_agent       TEXT,
user_id          integer
);

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            TEXT,
num_songs          INTEGER,
title              TEXT,
artist_name        TEXT,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          TEXT,
artist_longitude   FLOAT,
artist_location    TEXT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
songplay_id  INTEGER IDENTITY (0, 1) PRIMARY KEY sortkey,
start_time   TIMESTAMP NOT NULL, 
user_id      INTEGER NOT NULL distkey, 
level        TEXT, 
song_id      TEXT NOT NULL, 
artist_id    TEXT NOT NULL, 
session_id   INTEGER, 
location     TEXT, 
user_agent   TEXT
)diststyle key;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users(
user_id     INTEGER PRIMARY KEY sortkey, 
first_name  TEXT,
last_name   TEXT, 
gender      TEXT, 
level       TEXT
)diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs(
song_id   TEXT PRIMARY KEY sortkey, 
title     TEXT, 
artist_id TEXT distkey, 
year      INTEGER, 
duration  FLOAT
)diststyle key;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist(
artist_id   TEXT PRIMARY KEY sortkey,
name        TEXT,
location    TEXT,
lattitude   FLOAT4, 
longitude   FLOAT4
)diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time(
start_time  TIMESTAMP PRIMARY KEY, 
hour        INTEGER, 
day         INTEGER,
week        INTEGER,
month       INTEGER,
year        INTEGER DISTKEY,
weekday     INTEGER
) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {}
IAM_ROLE {}
compupdate off
region 'us-west-2'
format as JSON {}
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs from {}
iam_role {}
compupdate off
region 'us-west-2'
format as JSON 'auto'
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
      )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
from staging_events e
JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
where e.page='NexTSong'
""")

user_table_insert = ("""
INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
SELECT distinct (user_id) user_id,
        first_name,
        last_name,
        gender,
        level
FROM (select user_id,
            first_name,
            last_name,
            gender,
            level
          from staging_events
         where user_id is not Null) as temp
 group by user_id, first_name, last_name, gender, level
 order by user_id;
""")

song_table_insert = ("""
INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
SELECT distinct(song_id) as song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO dim_artist(artist_id, name, location, lattitude, longitude)
SELECT distinct(artist_id) as artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT  start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(weekday from start_time)
        FROM fact_songplays      
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
