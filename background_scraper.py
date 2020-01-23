import asyncio
import logging
import re
import sqlalchemy as db
import sys
from datetime import datetime
from join_types import JoinTypes
from models import ChatUser, Message, Notification
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from telethon import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import Message as TGMessage, PeerChannel, PeerChat, PeerUser
from urlextract import URLExtract

class BackgroundScraper:
    @staticmethod
    def make_scraper(conn_str, account_monitor):
        BackgroundScraper(conn_str, account_monitor)

    def __init__(self, conn_str, account_monitor):
        # Setup Logging
        self.logger = logging.getLogger("{}-scraper".format(account_monitor.account.account_id))
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_logger = logging.FileHandler("logs/{}-scraper.log".format(account_monitor.account.account_id))
        file_logger.setLevel(logging.INFO)
        file_logger.setFormatter(formatter)
        self.logger.addHandler(file_logger)
        console_logger = logging.StreamHandler()
        console_logger.setLevel(logging.INFO)
        console_logger.setFormatter(formatter)
        self.logger.addHandler(console_logger)

        # SQL Setup
        engine = db.create_engine(conn_str)
        self.Session = sessionmaker(bind=engine)

        # References to parent monitor
        self.monitor = account_monitor
        self.channels_to_join = account_monitor.channels_to_join
        self.channels_to_scrape = account_monitor.channels_to_scrape

        self.logger.info('{}: Background scraper is now active.'.format(sys._getframe().f_code.co_name))
        asyncio.run(self.scrape())

    async def scrape(self):
        # Telegram service login
        self.logger.info('{}: Logging in with account # {}'.format(sys._getframe().f_code.co_name, self.monitor.account.account_phone))
        session_file = 'session/scrape-' + self.monitor.account.account_phone.replace('+', '')
        self.client = TelegramClient(session_file, self.monitor.account.account_api_id, self.monitor.account.account_api_hash)
        self.tg_user = None
        await self.client.connect()
        if not await self.client.is_user_authorized():
            self.logger.critical('{}: Could not use client token to log in!'.format(sys._getframe().f_code.co_name))
            await self.client.send_code_request(self.monitor.account.account_phone)
            self.tg_user = await self.client.sign_in(self.monitor.account.account_phone, input('Enter code: '))

        # Start the scraper
        while True:
            # Blocking call
            channel = self.channels_to_scrape.get()
            await self.scrape_channel(log_time=channel[0], channel_id=channel[1])

    def check_for_channels(self, message_raw):
        potential_urls = URLExtract().find_urls(message_raw, True)
        for url in potential_urls:
            if "t.me" in url:
                # TODO: Check for the join URL in the database
                if len(self.monitor.channel_list) < 500:
                    if "joinchat" in url:
                        self.logger.info("{}: Identified new private channel: {}".format(sys._getframe().f_code.co_name, url))
                        self.channels_to_join.put((JoinTypes.PRIVATE, url))
                    else:
                        self.logger.info("{}: Identified new public channel: {}".format(sys._getframe().f_code.co_name, url))
                        self.channels_to_join.put((JoinTypes.GROUP, url))
                        
    async def get_user_by_id(self, user_id=None):
        if user_id > 0:
            u = await self.client.get_input_entity(PeerUser(user_id=user_id))
            user = await self.client(GetFullUserRequest(u))
            return {
                'username': user.user.username,
                'first_name': user.user.first_name,
                'last_name': user.user.last_name,
                'is_verified': user.user.verified,
                'is_bot': user.user.bot,
                'is_restricted': user.user.restricted,
                'phone': user.user.phone,
            }
        else:
            data = await self.client.get_entity(PeerChannel(user_id))
            return {
                'username': data.title,
                'first_name': "Channel",
                'last_name': "User",
                'is_verified': False,
                'is_bot': False,
                'is_restricted': False,
                'phone': None,
            }
    
    async def scrape_channel(self, log_time, channel_id):
        data = await self.client.get_entity(channel_id)
        count = 1
        async for message in self.client.iter_messages(data):
            if isinstance(message, TGMessage):
                await self.log_message(message, message.sender_id, channel_id, log_time)
                self.logger.info("{}: Logging message number {}".format(sys._getframe().f_code.co_name, count))
                count = count + 1
            else:
                self.logger.info("{}: A non-message was encountered: {}".format(sys._getframe().f_code.co_name, type(message)))
            await asyncio.sleep(4)

    async def log_message(self, message, sender_id, channel_id, log_time):
        # TODO: Support more then only SQL
        message_text = message.message

        # Lets set the meta data
        is_mention = message.mentioned
        is_scheduled = message.from_scheduled
        is_fwd = False if message.fwd_from is None else True
        is_reply = False if message.reply_to_msg_id is None else True
        is_bot = False if message.via_bot_id is None else True

        if self.monitor.should_crawl:
            self.check_for_channels(str(message))
        
        if isinstance(message.to_id, PeerChannel):
            is_channel = True
            is_group = False
            is_private = False
        elif isinstance(message.to_id, PeerChat):
            is_channel = False
            is_group = True
            is_private = False
        else:
            is_channel = False
            is_group = False
            is_private = False

        o = await self.get_user_by_id(sender_id)

        session = self.Session()
        if not bool(session.query(ChatUser).filter_by(chat_user_id=sender_id).all()):
            self.logger.info("{}: Message user not in DB.".format(sys._getframe().f_code.co_name))
            session.add(ChatUser(
                chat_user_id=sender_id,
                chat_user_is_channel=False if sender_id > 0 else True,
                chat_user_is_bot=o['is_bot'],
                chat_user_is_verified=o['is_verified'],
                chat_user_is_restricted=o['is_restricted'],
                chat_user_first_name=o['first_name'],
                chat_user_last_name=o['last_name'],
                chat_user_name=o['username'],
                chat_user_phone=o['phone'],
                chat_user_tlogin=log_time,
                chat_user_tmodified=log_time
            ))
            session.flush()

        # Add message
        msg = Message(
            chat_user_id=sender_id,
            account_id=self.monitor.account.account_id,
            channel_id=channel_id,
            message_text=message_text,
            message_is_mention=is_mention,
            message_is_scheduled=is_scheduled,
            message_is_fwd=is_fwd,
            message_is_reply=is_reply,
            message_is_bot=is_bot,
            message_is_group=is_group,
            message_is_private=is_private,
            message_is_channel=is_channel,
            message_channel_size=None,
            message_tcreate=datetime.now()
        )
        session.add(msg)
        session.flush()

        session.add(Notification(
            keyword_id=1,
            message_id=msg.message_id,
            channel_id=channel_id,
            account_id=self.monitor.account.account_id,
            chat_user_id=sender_id
        ))

        try:
            session.commit()
        except IntegrityError as integ_err:
            self.logger.error("{}: An IntegrityError has been encountered: {}".format(sys._getframe().f_code.co_name, str(integ_err)))
        finally:
            session.close()