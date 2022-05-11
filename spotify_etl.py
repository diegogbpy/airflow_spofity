"""
Algorítimo baseado no vídeo e exemplo de Karolina Sowinska (YouTube)
	- Crédito especial para ela.
"""

import psycopg2 as db
import pandas as pd
import requests
import json
import datetime
import spotipy.util as util


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    """
    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")
    """
    return True


def run_spotify_etl():

    
    user_id = ""  # Spotify username

    token = ""  # Spotify API token



    # Extract part of the ETL process

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50", headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Prepare a dictionary in order to turn it into a pandas dataframe below
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")

    # Format timestamp to Postgres
    song_df['played_at'] = song_df['played_at'].map(lambda x: x.replace('T', '-'))
    song_df['played_at'] = song_df['played_at'].map(lambda x: x.replace('Z', ''))
    song_df['played_at'] = song_df['played_at'].map(lambda x: str(x)[:-4])
    song_df['timestamp'] = pd.to_datetime(song_df['timestamp'], format='%m-%d-%Y')

    # Load
    parameters = {
        "host": "",
        "database": "",
        "user": "",
        "password": ""
    }

    def connect(par):
        conn = None
        try:
            print('Connecting to the PostgreSQL database...')
            conn = db.connect(**par)
        except (Exception, db.DatabaseError) as error:
            print(error)
            sys.exit(1)
        return conn

    def single_insert(conn, insert_req):
        cursor = conn.cursor()
        try:
            cursor.execute(insert_req)
            conn.commit()
        except (Exception, db.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    # Connecting to the database
    conn = connect(parameters)

    # Inserting each row
    for i in song_df.index:
        query = """INSERT into minha_lista(song_name,artist_name,played_at,timestamp) values(%s %s %s %s);
        """ % (song_df['song_name'], song_df['artist_name'], song_df['played_at'], song_df['timestamp'])

        single_insert(conn, query)

    # Close the connection
    conn.close()

    print("Close database successfully")

