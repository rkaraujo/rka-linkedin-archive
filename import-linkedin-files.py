import argparse
import pandas as pd
from langdetect import detect
from sqlalchemy import create_engine
from sqlalchemy.sql import text

MESSAGES_FILE = 'messages.csv'
CONNECTIONS_FILE = 'Connections.csv'


def save_to_db(df_to_save, table_name):
    print("Saving to database")
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost/rka_linkedin_archive')
    df_to_save.to_sql(table_name, con=engine, if_exists='replace', index=True)

    # index column - naming it id and making it primary key
    with engine.connect() as connection:
        connection.execute(text(f'ALTER TABLE {table_name} RENAME COLUMN index TO id;'))
        connection.execute(text(f'ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name}_id PRIMARY KEY (id);'))
        connection.commit()


def is_english(content):
    try:
        return detect(content) == 'en'
    except:
        # some errors happen I believe when there is no text, for example, only an emoji
        return False


def rename_columns_snakecase(df_to_rename):
    map_rename = {}  # saving index as id
    for col in df_to_rename.columns:
        map_rename[col] = col.lower().replace(' ', '_')
    df_to_rename.rename(columns=map_rename, inplace=True)


def read_messages_file(skip_english_detection):
    print("Reading messages file")

    # headers = ['CONVERSATION ID', 'CONVERSATION TITLE', 'FROM', 'SENDER PROFILE URL', 'TO', 'RECIPIENT PROFILE URLS', 'DATE', 'SUBJECT', 'CONTENT', 'FOLDER']
    df_msgs = pd.read_csv(MESSAGES_FILE)

    # remove null content
    df_msgs = df_msgs.dropna(subset=['CONTENT'])

    # convert date to datetime
    df_msgs['DATE'] = pd.to_datetime(df_msgs['DATE'])

    # check if language is English of the content
    if not skip_english_detection:
        df_msgs['IS_ENGLISH'] = df_msgs['CONTENT'].apply(is_english)

    # format the content a little bit adding new lines
    df_msgs['CONTENT'] = (df_msgs['CONTENT'].str.replace(r'\s{2,}', '\n')
                          .str.replace('. ', '.\n')
                          .str.replace('! ', '!\n')
                          .str.replace('? ', '?\n')
                          .str.replace('; ', ';\n'))

    rename_columns_snakecase(df_msgs)

    return df_msgs


def read_connections_file():
    print("Reading connections file")

    # First Name,Last Name,URL,Email Address,Company,Position,Connected On
    df_conns = pd.read_csv(CONNECTIONS_FILE, skiprows=3)

    # remove null content
    df_conns = df_conns.dropna(subset=['First Name'])

    # convert to datetime
    df_conns['Connected On'] = pd.to_datetime(df_conns['Connected On'], format='%d %b %Y')

    rename_columns_snakecase(df_conns)

    return df_conns


if __name__ == '__main__':
    """
    This script reads an exported LinkedIn csv file and saves the data to a database
    """

    # English content detection can be skipped, it runs kinda slow
    parser = argparse.ArgumentParser(description='LinkedIn messages file reader')
    parser.add_argument('-sed', '--skip-english-detection', action='store_true', help='Skip English detection for the content')
    args = parser.parse_args()

    df = read_messages_file(args.skip_english_detection)
    save_to_db(df, 'messages')

    df = read_connections_file()
    save_to_db(df, 'connections')
