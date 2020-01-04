from models import Account, Channel, ChatUser, Keyword, Message, Monitor, Notification
import sqlalchemy as db
import csv
from datetime import datetime
import sys
import os
import logging
from sqlalchemy.orm import sessionmaker
logging.getLogger().setLevel(logging.INFO)

Session = None
session = None
SERVER_MODE = None
engine = None
Config = None

"""
This script will build our your database for you
"""

def init_db():
    global session, SERVER_MODE, engine
    logging.info('{}: Initializing the database'.format(sys._getframe().f_code.co_name))
    Account.metadata.create_all(engine)
    ChatUser.metadata.create_all(engine)
    Channel.metadata.create_all(engine)
    Message.metadata.create_all(engine)
    Keyword.metadata.create_all(engine)
    Monitor.metadata.create_all(engine)
    Notification.metadata.create_all(engine)
    session.close()


"""
    Lets setup the channels to monitor in the database
"""
def init_data():
    global session, SERVER_MODE, engine
    session = Session()
    init_add_account()
    init_add_channels()
    init_add_keywords()
    init_add_monitors()
    session.close()

def init_add_account():
    global session, SERVER_MODE, engine, Config
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
    global session, SERVER_MODE, engine

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

    session.commit()

# ==============================
# The keywords we want to spy on
# ==============================
def init_add_keywords():
    global session, SERVER_MODE, engine

    # TODO: Add keywords from csv

    KEYWORDS = []

    for keyword in KEYWORDS:
        logging.info('{}: Adding keyword {} to the database'.format(sys._getframe().f_code.co_name, keyword['keyword_description']))

        session.add(Keyword(
            keyword_description=keyword['keyword_description'],
            keyword_regex=keyword['keyword_regex'],
            keyword_tmodified=datetime.now(),
            keyword_tcreate=datetime.now()
        ))
    session.commit()


# ======================================
# Lets add the channels we want to watch
# ======================================
def init_add_monitors():
    global session, SERVER_MODE, engine
    # Lets assign them all
    accounts = session.query(Account).all()
    channels = session.query(Channel).all()
    account_index = 0
    channel_count = 0

    for channel in channels:
        account = accounts[account_index]
        logging.info('{}: Adding monitoring to channel {} with account_id {} to the database'.format(sys._getframe().f_code.co_name, channel.channel_name, account.account_id))
        session.add(Monitor(
            channel_id=channel.id,
            account_id=account.account_id,
            monitor_tcreate=datetime.now(),
            monitor_tmodified=datetime.now()
        ))
        channel_count += 1
        if channel_count > 500:
            account_index += 1
            channel_count = 0
    session.commit()


def initialize_db(config):
    global session, SERVER_MODE, engine, Session, Config
    Config = config

    MYSQL_CONNECTOR_STRING = 'mysql+mysqlconnector://{}:{}@{}:{}'.format(config["database"]["sql"]["username"],
                                                                         config["database"]["sql"]["password"],
                                                                         config["database"]["sql"]["hostname"],
                                                                         config["database"]["sql"]["port"])

    engine = db.create_engine(MYSQL_CONNECTOR_STRING)#, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute("CREATE DATABASE {} CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci';".format(config["database"]["sql"]["database"]))
    session.close()
    engine = db.create_engine('{}/{}?charset=utf8mb4'.format(MYSQL_CONNECTOR_STRING, config["database"]["sql"]["database"]))
    Session = sessionmaker(bind=engine)
    session = None
    session = Session()

    # A hack to support unicode for emojis
    session.execute('SET NAMES "utf8mb4" COLLATE "utf8mb4_unicode_ci"')
    session.execute('ALTER DATABASE {} CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;'.format(config["database"]["sql"]["database"]))
    session.execute('commit')

    init_db()
    init_data()


if __name__ == '__main__':
    initialize_db()
