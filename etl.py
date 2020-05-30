import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description:  Process a json file containing song data and insert it
    into the song and artist tables.
    Inputs:
        - cur: database cursor object
        - filepath: file path to an individual song file
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [df.values[0][i] for i in [7, 8, 0, 9, 6]]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = [df.values[0][i] for i in [0, 4, 2, 1, 3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description:  Process a json file containing user activity data, and insert
    it into the user and time tables.
    Inputs:
        - cur: database cursor object
        - filepath: path to individual log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'].str.contains('NextSong', na=False)]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts'].dt
    time_data = []
    for i, j in df.iterrows():
        time_data.append([str(t.time[i]), t.hour[i], t.day[i], t.week[i],
                          t.month[i], t.year[i], t.weekday[i]])
    column_labels = ['timestamp', 'hour', 'day', 'week',
                     'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    # insert timestamp records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function will walk a directory tree and file all json files.
    It then iterates over the files and processes them.
    Inputs:
        - cur: the cursor object
        - conn the db connection
        - filepath: root directory to be traversed for files
        - func: specifies whether the files contain song or log data
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
        user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
