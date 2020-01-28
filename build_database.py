from models import Account, Channel, ChatUser, Keyword, Message, Notification
import sqlalchemy as db
import csv
from datetime import datetime
import sys
import os
import logging
import yaml
from sqlalchemy.orm import sessionmaker
logging.getLogger().setLevel(logging.INFO)

Session = None
session = None
engine = None
Config = None

def init_db():
    global session, engine
    logging.info('{}: Initializing the database'.format(sys._getframe().f_code.co_name))
    Account.metadata.create_all(engine)
    ChatUser.metadata.create_all(engine)
    Channel.metadata.create_all(engine)
    Message.metadata.create_all(engine)
    Keyword.metadata.create_all(engine)
    Notification.metadata.create_all(engine)
    session.close()


"""
    Lets setup the channels to monitor in the database
"""
def init_data():
    global session, engine
    session = Session()
    init_add_account()
    init_add_channels()
    init_add_keywords()
    session.close()

def init_add_account():
    global session, engine, Config
    logging.info('{}: Adding bot account'.format(sys._getframe().f_code.co_name))

    BOT_ACCOUNTS = [
        Account(
            account_id=Config["initial"]["account"]["id"],
            account_api_id=Config["initial"]["account"]["api_id"],
            account_api_hash=Config["initial"]["account"]["api_hash"],
            account_is_bot=False,
            account_is_verified=False,
            account_is_restricted=False,
            account_first_name=Config["initial"]["account"]["first_name"],
            account_last_name=Config["initial"]["account"]["last_name"],
            account_user_name=Config["initial"]["account"]["username"],
            account_phone=Config["initial"]["account"]["phone"],
            account_is_enabled=True,
            account_tlogin=datetime.now(),
            account_tcreate=datetime.now(),
            account_tmodified=datetime.now())
    ]
    for account in BOT_ACCOUNTS:
        session.add(account)
    session.commit()

def init_add_channels():
    global session, engine

    # Lets get the first account
    account = session.query(Account).first()

    CHANNELS = []
    if Config["initial"]["channel"]["use_initial"]:
        CHANNELS.append({
            'channel_name': Config["initial"]["channel"]["name"],
            'channel_id': Config["initial"]["channel"]["id"],
            'channel_url': Config["initial"]["channel"]["url"],
            'channel_is_private': Config["initial"]["channel"]["is_private"]
        })

    # Lets import the CSV with the channel list
    with open('initial/channels.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                print(f'Adding channel {row[0]} => {row[1]}')
                CHANNELS.append({
                    'channel_name': row[0],
                     'channel_url': row[1]
                                 })
            line_count += 1

        logging.info('Inserted {} channels to database'.format(line_count))

    for channel in CHANNELS:
        logging.info('{}: Adding channel {} to database'.format(sys._getframe().f_code.co_name, channel['channel_name']))

        channel_url = channel['channel_url'] if 'channel_url' in channel else None
        channel_id = channel['channel_id'] if 'channel_id' in channel else None
        channel_is_group = channel['channel_is_group'] if 'channel_is_group' in channel else False
        channel_is_private = channel['channel_is_private'] if 'channel_is_private' in channel else False

        session.add(Channel(
            channel_name=channel['channel_name'],
            channel_url=channel_url,
            channel_id=channel_id,
            account_id=account.account_id,
            channel_tcreate=datetime.now(),
            channel_is_group=channel_is_group,
            channel_is_private=channel_is_private
        ))

    # session.add(ChatUser(
    #     chat_user_id = -1,
    #     chat_user_is_bot = 0,
    #     chat_user_is_verified = 0,
    #     chat_user_is_restricted = 0,
    #     chat_user_first_name = "Channel",
    #     chat_user_last_name = "User", 
    #     chat_user_name = "Channel",
    #     chat_user_phone = 0
    # ))

    session.commit()

# ==============================
# The keywords we want to spy on
# ==============================
def init_add_keywords():
    global session, engine

    KEYWORDS = []
    KEYWORDS.append({
                    'keyword_description': "NULL",
                    'keyword_regex': "NULL"})

    # Lets import the CSV with the keywork list
    with open('initial/keywords.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                print(f'Adding keyword {row[0]} => {row[1]}')
                KEYWORDS.append({
                    'keyword_description': row[0],
                    'keyword_regex': row[1]
                                 })
            line_count += 1

        logging.info('Inserted {} keywords to database'.format(line_count))

    for keyword in KEYWORDS:
        logging.info('{}: Adding keyword {} to the database'.format(sys._getframe().f_code.co_name, keyword['keyword_description']))

        session.add(Keyword(
            keyword_description=keyword['keyword_description'],
            keyword_regex=keyword['keyword_regex'],
            keyword_tmodified=datetime.now(),
            keyword_tcreate=datetime.now()
        ))
    session.commit()

def initialize_db(config):
    global session, engine, Session, Config
    Config = config

    MYSQL_CONNECTOR_STRING = 'mysql+mysqlconnector://{}:{}@{}:{}'.format(config["database"]["sql"]["username"],
                                                                         config["database"]["sql"]["password"],
                                                                         config["database"]["sql"]["hostname"],
                                                                         config["database"]["sql"]["port"])

    engine = db.create_engine(MYSQL_CONNECTOR_STRING)#, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    # Character set is to suppoer emojis. This requires additional MariaDB configuration.
    session.execute("CREATE DATABASE IF NOT EXISTS {} CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci';".format(config["database"]["sql"]["database"]))
    session.close()
    engine = db.create_engine('{}/{}'.format(MYSQL_CONNECTOR_STRING, config["database"]["sql"]["database"])) # ?charset=utf8mb4
    Session = sessionmaker(bind=engine)
    session = None
    session = Session()
    session.execute('SET NAMES "utf8mb4" COLLATE "utf8mb4_unicode_ci"')

    init_db()
    init_data()


if __name__ == '__main__':
    config = None
    with open("config.yaml", "r") as conf_file:
        config = yaml.load(conf_file, Loader=yaml.FullLoader)
    initialize_db(config)
