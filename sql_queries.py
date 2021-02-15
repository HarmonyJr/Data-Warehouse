import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE  IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(event_id INT IDENTITY(0,1), artist_name VARCHAR, auth VARCHAR, 
                                                             user_first_name VARCHAR, user_gender CHAR, 
                                                             item_in_session INTEGER, user_last_name VARCHAR, 
                                                             song_length DOUBLE PRECISION, user_level VARCHAR, 
                                                             location VARCHAR, method VARCHAR, page VARCHAR, 
                                                             registration VARCHAR, session_id BIGINT, song_title VARCHAR, 
                                                             status INTEGER, ts VARCHAR, user_agent TEXT, user_id INT, 
                                                             PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(songplay_id INT IDENTITY(0,1), artist_id VARCHAR, latitude float, 
                                                            longitude float, location VARCHAR, artist_name VARCHAR, 
                                                            song_id VARCHAR, title VARCHAR, duration NUMERIC, year INT, 
                                                            PRIMARY KEY (songplay_id))
""")

songplay_table_create = ("""CREATE TABLE songplays(songplay_id INT IDENTITY(0,1), start_time TIMESTAMP NOT NULL, user_id INT NOT NULL, 
                                                   level VARCHAR, song_id VARCHAR NOT NULL, artist_id VARCHAR NOT NULL, 
                                                   session_id BIGINT, location VARCHAR, user_agent TEXT, 
                                                   PRIMARY KEY (songplay_id))
""")

user_table_create = ("""CREATE TABLE users(user_id INT NOT NULL, first_name VARCHAR, last_name VARCHAR, 
                                           gender CHAR, level VARCHAR, 
                                           PRIMARY KEY (user_id))
""")

song_table_create = ("""CREATE TABLE songs(song_id VARCHAR NOT NULL, title VARCHAR, artist_id VARCHAR NOT NULL, 
                                           year INT, duration NUMERIC,
                                           PRIMARY KEY (song_id))
""")

artist_table_create = ("""CREATE TABLE artists(artist_id VARCHAR NOT NULL, name VARCHAR NOT NULL, location VARCHAR, 
                                               latitude float, longitude float, 
                                               PRIMARY KEY (artist_id))

""")

time_table_create = ("""CREATE TABLE time(start_time TIMESTAMP, hour INT, day INT, week INT, month INT, year INT, weekday INT, 
                                          PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
                          credentials 'aws_iam_role={}'
                          json {}
                          region 'us-west-2'
                          timeformat as 'epochmillisecs';       
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))
    
staging_songs_copy = ("""copy staging_songs from {}
                         credentials 'aws_iam_role={}'
                         json 'auto'
                         region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id,location, user_agent) 
                            SELECT (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') As start_time, se.user_Id, se.user_level,
                            ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
                            FROM staging_events se LEFT JOIN staging_songs ss ON se.song_title = ss.title
                            WHERE ss.song_id IS NOT NULL and ss.artist_id IS NOT NULL;
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id, user_first_name, user_last_name, user_gender, user_level
                        FROM staging_events WHERE user_id IS NOT NULL and page ='NextSong';
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration 
                        FROM staging_songs ss WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT distinct ss.artist_id, ss.artist_name, ss.location, ss.latitude, ss.longitude
                          FROM staging_songs ss WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
                        SELECT distinct start_time, EXTRACT(HOUR FROM start_time) As hour, EXTRACT(DAY FROM start_time) 
                        As day, EXTRACT(WEEK FROM start_time) As week, EXTRACT(MONTH FROM start_time) As month, 
                        EXTRACT(YEAR FROM start_time) As year, EXTRACT(DOW FROM start_time) As weekday                              
                        FROM (SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time FROM staging_events)
                               
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
