from models import Account, Channel, ChatUser, Keyword, Message, Notification
import sqlalchemy as db
from datetime import datetime, timedelta
from random import randrange
import build_database
import sys
import os
import logging
import json
import re
import asyncio
import time
from telethon import utils
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, InterfaceError, ProgrammingError
from telethon.tl.functions.users import GetFullUserRequest
from telethon import TelegramClient, events
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.errors.rpcerrorlist import FloodWaitError, ChannelPrivateError, UserAlreadyParticipantError, ChatAdminRequiredError, InviteHashInvalidError, InviteHashEmptyError
from telethon.tl.functions.channels import  JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, ExportChatInviteRequest
import gspread
from oauth2client.service_account import ServiceAccountCredentials

r"""
    --------------------------------------------------
        ____      ____                              
       /  _/___  / __/___  _________ ___  ___  _____
       / // __ \/ /_/ __ \/ ___/ __ `__ \/ _ \/ ___/
     _/ // / / / __/ /_/ / /  / / / / / /  __/ /    
    /___/_/ /_/_/  \____/_/  /_/ /_/ /_/\___/_/
    
    --------------------------------------------------
    by @paulpierre 11-26-2019
    https://github.com/paulpierre/informer
"""


# Lets set the logging level
logging.getLogger().setLevel(logging.INFO)


class TGInformer:

    def __init__(self, account_id, config):
        # ------------------
        # Instance variables
        # ------------------
        self.keyword_list = []
        self.channel_list = []
        self.channel_meta = {}
        self.bot_task = None
        self.KEYWORD_REFRESH_WAIT = 15 * 60
        self.MIN_CHANNEL_JOIN_WAIT = 30
        self.MAX_CHANNEL_JOIN_WAIT = 120
        self.bot_uptime = 0
        self.should_crawl = config["crawling"]["enabled"]

        # --------------
        # Display banner
        # --------------
        print(r"""
            ____      ____                              
           /  _/___  / __/___  _________ ___  ___  _____
           / // __ \/ /_/ __ \/ ___/ __ `__ \/ _ \/ ___/
         _/ // / / / __/ /_/ / /  / / / / / /  __/ /    
        /___/_/ /_/_/  \____/_/  /_/ /_/ /_/\___/_/     

        by @paulpierre 11-26-2019
        """)

        # Initialize database
        self.MYSQL_CONNECTOR_STRING = 'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(config["database"]["sql"]["username"],
                                                                                     config["database"]["sql"]["password"],
                                                                                     config["database"]["sql"]["hostname"],
                                                                                     config["database"]["sql"]["port"],
                                                                                     config["database"]["sql"]["database"])
        self.engine = db.create_engine(self.MYSQL_CONNECTOR_STRING)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        try:
            self.account = self.session.query(Account).filter_by(account_id=account_id).first()
        except ProgrammingError:
            logging.error('Database is not set up, setting it up')
            build_database.initialize_db(config)
            self.account = self.session.query(Account).filter_by(account_id=account_id).first()

        if not self.account:
            logging.error('Invalid account_id {} for bot instance'.format(account_id))
            sys.exit(0)

        # Notification Methods
        self.telegram_enabled = config["notification"]["telegram"]["enabled"]
        self.g_enabled = config["notification"]["google_sheets"]["enabled"]
        self.sql_enabled = config["notification"]["sql"]["enabled"]
        self.elastic_enabled = config["notification"]["elastic_search"]["enabled"]
        self.json_enabled = config["notification"]["json"]["enabled"]

        # Initialize Google Sheet
        if self.g_enabled:
            scope = [ 'https://www.googleapis.com/auth/spreadsheets',
                      'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(config["notification"]["google_sheets"]["credentials_path"], scope)
            self.gsheet = gspread.authorize(creds)
            self.sheet = self.gsheet.open(config["notification"]["google_sheets"]["sheet_name"]).sheet1
        else:
            self.gsheet = None
            self.sheet = None

        # Set the channel we want to send alerts to
        self.monitor_channel = config["notification"]["telegram"]["channel_id"]

        # Telegram service login
        logging.info('Logging in with account # {}'.format(self.account.account_phone))
        session_file = 'session/' + self.account.account_phone.replace('+', '')
        self.client = TelegramClient(session_file, self.account.account_api_id, self.account.account_api_hash)
        self.tg_user = None


    async def client_login(self):
        # TODO: automate authcode with the Burner API
        await self.client.connect()
        if not await self.client.is_user_authorized():
            logging.info('Client is currently not logged in, please sign in!')
            self.client.send_code_request(self.account.account_phone)
            self.tg_user = self.client.sign_in(self.account.account_phone, input('Enter code: '))

    # =============
    # Get all users
    # =============
    def get_channel_all_users(self, channel_id):
        # TODO: this function is not complete
        channel = self.client.get_entity(PeerChat(channel_id))
        users = self.client.get_participants(channel)
        print('total users: {}'.format(users.total))
        for user in users:
            if user.username is not None and not user.is_self:
                print(utils.get_display_name(user), user.username, user.id, user.bot, user.verified, user.restricted, user.first_name, user.last_name, user.phone, user.is_self)

    # =====================
    # Get # of participants
    # =====================
    async def get_channel_user_count(self, channel):
        data = await self.client.get_entity(PeerChannel(-channel))
        try:
            users = await self.client.get_participants(data)
            return users.total
        except ChatAdminRequiredError:
            return 0

    # =======================
    # Get channel by group ID
    # =======================
    async def get_channel_info_by_group_id(self, id):
        channel = await self.client.get_entity(PeerChat(id))

        return {
            'channel_id': channel.id,
            'channel_title': channel.title,
            'is_broadcast': False,
            'is_mega_group': False,
            'channel_access_hash': None,
        }

    # ==========================
    # Get channel by channel URL
    # ==========================
    async def get_channel_info_by_url(self, url):
        logging.info('{}: Getting channel info with url: {}'.format(sys._getframe().f_code.co_name, url))
        channel_hash = utils.parse_username(url)[0]

        # Test if we can get entity by channel hash
        try:
            channel = await self.client.get_entity(channel_hash)
        except ValueError:
            logging.info('{}: Not a valid telegram URL: {}'.format(sys._getframe().f_code.co_name, url))
            return False
        except FloodWaitError as e:
            logging.info('{}: Got a flood wait error for: {}'.format(sys._getframe().f_code.co_name, url))
            # TODO: Doesn't this crash?
            await asyncio.sleep(e.seconds * 2)

        return {
            'channel_id': channel.id,
            'channel_title': channel.title,
            'is_broadcast': channel.broadcast,
            'is_mega_group': channel.megagroup,
            'channel_access_hash': channel.access_hash,
        }

    # ===================
    # Get user info by ID
    # ===================
    async def get_user_by_id(self, user_id=None):
        try:
            u = await self.client.get_input_entity(PeerUser(user_id=user_id))
            user = await self.client(GetFullUserRequest(u))

            logging.info('{}: User ID {} has data:\n {}\n\n'.format(sys._getframe().f_code.co_name, user_id, user))

            return {
                'username': user.user.username,
                'first_name': user.user.first_name,
                'last_name': user.user.last_name,
                'is_verified': user.user.verified,
                'is_bot': user.user.bot,
                'is_restricted': user.user.restricted,
                'phone': user.user.phone,
                'is_valid_user': True
            }
        except TypeError:
            logging.info("{}: Message was not sent by a user... giving a null user".format(sys._getframe().f_code.co_name))
            return {
                'username': "Channel",
                'first_name': "Channel",
                'last_name': "User",
                'is_verified': False,
                'is_bot': False,
                'is_restricted': False,
                'phone': None,
                'is_valid_user': False
            }

    # =====================================
    # Check for invite links in the channel
    # =====================================
    async def check_for_channels(self, message_text, forward_from):
        private_channels = re.findall(r"https:\/\/t.me\/joinchat\/([a-zA-z0-9]|.*)+", message_text)
        for url in private_channels:
            if len(self.channel_list) != 500:
                logging.info("{}: Attempting to join private channel: {}".format(sys._getframe().f_code.co_name, url))
                await self.join_private_channel(url)
            else:
                # TODO: handle adding multiple accounts
                pass
        groups = re.findall(r"https:\/\/t.me\/([a-zA-z0-9]|.*)+", message_text)
        for url in groups:
            if url in private_channels:
                continue
            if len(self.channel_list) != 500:
                logging.info("{}: Attempting to join group: {}".format(sys._getframe().f_code.co_name, url))
                await self.join_group(url)
            else:
                # TODO: handle adding multiple accounts
                pass
        if forward_from:
            logging.info("{}: Attempting to join group from forward: {}".format(sys._getframe().f_code.co_name, forward_from.channel_id))
            await self.join_group(forward_from.channel_id)
    
    async def join_group(self, url):
        try:
            await self.client(JoinChannelRequest(channel=await self.client.get_entity(url)))
            sec = randrange(self.MIN_CHANNEL_JOIN_WAIT, self.MAX_CHANNEL_JOIN_WAIT)
            logging.info('sleeping for {} seconds'.format(sec))
            await asyncio.sleep(sec)
        except FloodWaitError as e:
            logging.info('Received FloodWaitError, waiting for {} seconds..'.format(e.seconds))
            await asyncio.sleep(e.seconds * 2)
        except ChannelPrivateError as e:
            logging.info('Channel is private or we were banned bc we didnt respond to bot')
        except InviteHashInvalidError:
            logging.info('Failed to join an invalid chat link')
        except InviteHashEmptyError:
            logging.info('The invite hash was empty')

    async def join_private_channel(self, url):
        channel_hash = url.replace('https://t.me/joinchat/', '')
        try:
            await self.client(ImportChatInviteRequest(hash=channel_hash))
            sec = randrange(self.MIN_CHANNEL_JOIN_WAIT, self.MAX_CHANNEL_JOIN_WAIT)
            logging.info('sleeping for {} seconds'.format(sec))
            await asyncio.sleep(sec)
        except FloodWaitError as e:
            logging.info('Received FloodWaitError, waiting for {} seconds..'.format(e.seconds))
            await asyncio.sleep(e.seconds * 2)
        except ChannelPrivateError as e:
            logging.info('Channel is private or we were banned bc we didnt respond to bot')
        except UserAlreadyParticipantError as e:
            logging.info('Already in channel, skipping')
        except InviteHashInvalidError:
            logging.info('Failed to join an invalid chat link')
        except InviteHashEmptyError:
            logging.info('The invite hash was empty')

    # ==============================
    # Initialize keywords to monitor
    # ==============================
    def init_keywords(self):
        keywords = self.session.query(Keyword).filter_by(keyword_is_enabled=True).all()

        for keyword in keywords:
            self.keyword_list.append({
                'id': keyword.keyword_id,
                'name': keyword.keyword_description,
                'regex': keyword.keyword_regex
            })
        logging.info('{}: Monitoring keywords: {}'.format(sys._getframe().f_code.co_name, json.dumps(self.keyword_list, indent=4)))

    # ===========================
    # Initialize channels to join
    # ===========================
    async def init_monitor_channels(self):

        # Let's start listening
        # pylint: disable=unused-variable
        @self.client.on(events.NewMessage)
        async def message_event_handler(event):
            await self.filter_message(event)

        # Update the channel data in DB
        current_channels = []
        # Lets iterate through all the open chat channels we have
        async for dialog in self.client.iter_dialogs():
            channel_id = dialog.id
            # As long as it is not a chat with ourselves
            if not dialog.is_user:
                # Certain channels have a prefix of 100, lets remove that
                if str(abs(channel_id))[:3] == '100':
                    channel_id = int(str(abs(channel_id))[3:])
                # Lets add it to the current list of channels we're in
                current_channels.append(channel_id)
                self.channel_list.append(channel_id)
                logging.info('id: {} name: {}'.format(dialog.id, dialog.name))
                # Is it in the DB?
                self.session = self.Session()
                channel_obj = self.session.query(Channel).filter_by(channel_id=channel_id).first()
                if not channel_obj:
                    self.session.add(Channel(
                        channel_id = channel_id,
                        channel_name = dialog.name,
                        channel_title = dialog.name,
                        channel_url = None,
                        account_id = self.account.account_id,
                        channel_is_mega_group = dialog.entity.megagroup,
                        channel_is_group = True,
                        channel_is_private = dialog.entity.restricted,
                        channel_is_broadcast = dialog.entity.broadcast,
                        channel_access_hash = dialog.entity.access_hash,
                        channel_size = 0,
                        channel_is_enabled = True,
                        channel_tcreate = datetime.now(),
                    ))
                    try:
                        self.session.commit()
                    except IntegrityError:
                        self.session.rollback()
                    except InterfaceError:
                        pass
                self.session.close()

        logging.info('{}: ### Current channels {}'.format(sys._getframe().f_code.co_name, json.dumps(current_channels)))

        # Get the list of channels to monitor
        self.session = self.Session()
        channels_for_account = self.session.query(Channel).filter_by(account_id=self.account.account_id).all()

        channels_to_monitor = []
        for monitor in channels_for_account:
            channel_data = {
                'channel_id': monitor.channel_id,
                'channel_name': monitor.channel_name,
                'channel_title': monitor.channel_title,
                'channel_url': monitor.channel_url,
                'account_id': monitor.account_id,
                'channel_is_megagroup': monitor.channel_is_mega_group,
                'channel_is_group': monitor.channel_is_group,
                'channel_is_private': monitor.channel_is_private,
                'channel_is_broadcast': monitor.channel_is_broadcast,
                'channel_access_hash': monitor.channel_access_hash,
                'channel_size': monitor.channel_size,
                'channel_is_enabled': monitor.channel_is_enabled,
                'channel_tcreate': monitor.channel_tcreate
            }

            if monitor.channel_is_enabled is True:
                channels_to_monitor.append(channel_data)
        self.session.close()

        for channel in channels_to_monitor:
            self.session = self.Session()
            channel_obj = self.session.query(Channel).filter_by(channel_id=channel['channel_id']).first()
            # Is the channel populated
            if channel['channel_id']:
                self.channel_list.append(channel['channel_id'])
                logging.info('Adding channel {} to monitoring w/ ID: {} hash: {}'.format(channel['channel_name'], channel['channel_id'], channel['channel_access_hash']))

                self.channel_meta[channel['channel_id']] = {
                    'channel_id': channel['channel_id'],
                    'channel_title': channel['channel_title'],
                    'channel_url': channel['channel_url'],
                    'channel_size': 0,
                    'channel_texpire': datetime.now() + timedelta(hours=3)
                }
            else:
                if channel['channel_url'] and '/joinchat/' not in channel['channel_url']:
                    o = await self.get_channel_info_by_url(channel['channel_url'])
                    if o is False:
                        logging.error('Invalid channel URL: {}'.format(channel['channel_url']))
                        # TODO: Remove it from the channel DB, or disable it
                        continue
                    logging.info('{}: ### Successfully identified {}'.format(sys._getframe().f_code.co_name, channel['channel_name']))
                elif channel['channel_is_group']:
                    o = await self.get_channel_info_by_group_id(channel['channel_id'])
                    logging.info('{}: ### Successfully identified {}'.format(sys._getframe().f_code.co_name, channel['channel_name']))
                else:
                    logging.info('{}: Unable to indentify channel {}'.format(sys._getframe().f_code.co_name, channel['channel_name']))
                    # TODO: Remove it form the channel DB, or disable it
                    continue

                channel_obj.channel_id = o['channel_id']
                channel_obj.channel_title = o['channel_title']
                channel_obj.channel_is_broadcast = o['is_broadcast']
                channel_obj.channel_is_mega_group = o['is_mega_group']
                channel_obj.channel_access_hash = o['channel_access_hash']
                self.channel_meta[o['channel_id']] = {
                    'channel_id': o['channel_id'],
                    'channel_title': o['channel_title'],
                    'channel_url': channel['channel_url'],
                    'channel_size': 0,
                    'channel_texpire':datetime.now() + timedelta(hours=3)
                }

            channel_is_private = True if (channel['channel_is_private'] or (channel['channel_url'] and '/joinchat/' in channel['channel_url'])) else False
            # TODO: Make this a function
            # Join if public channel and we're not in it
            if channel['channel_is_group'] is False and channel_is_private is False and channel['channel_id'] not in current_channels:
                logging.info('{}: Joining channel: {} => {}'.format(sys._getframe().f_code.co_name, channel['channel_id'], channel['channel_name']))
                try:
                    await self.client(JoinChannelRequest(channel=await self.client.get_entity(channel['channel_url'])))
                    sec = randrange(self.MIN_CHANNEL_JOIN_WAIT, self.MAX_CHANNEL_JOIN_WAIT)
                    logging.info('sleeping for {} seconds'.format(sec))
                    await asyncio.sleep(sec)
                except FloodWaitError as e:
                    logging.info('Received FloodWaitError, waiting for {} seconds..'.format(e.seconds))
                    await asyncio.sleep(e.seconds * 2)
                except ChannelPrivateError as e:
                    logging.info('Channel is private or we were banned bc we didnt respond to bot')
                    channel['channel_is_enabled'] = False

            # Join if private channel and we're not in it
            elif channel_is_private and channel['channel_id'] not in current_channels:
                channel_obj.channel_is_private = True
                logging.info('{}: Joining private channel: {} => {}'.format(sys._getframe().f_code.co_name, channel['channel_id'], channel['channel_name']))
                # Join private channel with secret hash
                channel_hash = channel['channel_url'].replace('https://t.me/joinchat/', '')
                try:
                    await self.client(ImportChatInviteRequest(hash=channel_hash))
                    sec = randrange(self.MIN_CHANNEL_JOIN_WAIT, self.MAX_CHANNEL_JOIN_WAIT)
                    logging.info('sleeping for {} seconds'.format(sec))
                    await asyncio.sleep(sec)
                except FloodWaitError as e:
                    logging.info('Received FloodWaitError, waiting for {} seconds..'.format(e.seconds))
                    await asyncio.sleep(e.seconds * 2)
                except ChannelPrivateError as e:
                    logging.info('Channel is private or we were banned bc we didnt respond to bot')
                    channel['channel_is_enabled'] = False
                except UserAlreadyParticipantError as e:
                    logging.info('Already in channel, skipping')
                    self.session.close()
                    continue

            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
            except InterfaceError:
                pass
            self.session.close()

        logging.info('{}: Monitoring channels: {}'.format(sys._getframe().f_code.co_name, json.dumps(self.channel_list, indent=4)))
        logging.info('Channel METADATA: {}'.format(self.channel_meta))


    # ===========================
    # Filter the incoming message
    # ===========================
    async def filter_message(self, event):
        # If this is a channel, grab the channel ID
        if isinstance(event.message.to_id, PeerChannel):
            channel_id = event.message.to_id.channel_id
        # If this is a group chat, grab the chat ID
        elif isinstance(event.message.to_id, PeerChat):
            channel_id = event.message.chat_id
        else:
            # Message comes neither from a channel or chat, lets skip
            return

        # Channel values from the API are signed ints, lets get ABS for consistency
        channel_id = abs(channel_id)

        message = event.raw_text

        if channel_id in self.channel_list:
            if len(self.keyword_list) != 0:
                for keyword in self.keyword_list:
                    if re.search(keyword['regex'], message, re.IGNORECASE):
                        logging.info(
                            'Filtering: {}\n\nEvent raw text: {} \n\n Data: {}'.format(channel_id, event.raw_text, event))
                        await self.send_notification(message_obj=event.message, event=event, sender_id=event.message.sender_id, channel_id=channel_id, keyword=keyword['name'], keyword_id=keyword['id'])
            else:
                await self.send_notification(message_obj=event.message, event=event, sender_id=event.message.sender_id, channel_id=channel_id)
        
        await event.message.mark_read()

    # ====================
    # Handle notifications
    # ====================
    async def send_notification(self, sender_id=None, event=None, channel_id=None, keyword=None, keyword_id=1, message_obj=None):
        message_text = message_obj.message

        # Lets set the meta data
        is_mention = message_obj.mentioned
        is_scheduled = message_obj.from_scheduled
        is_fwd = False if message_obj.fwd_from is None else True
        is_reply = False if message_obj.reply_to_msg_id is None else True
        is_bot = False if message_obj.via_bot_id is None else True

        if self.should_crawl:
            await self.check_for_channels(message_text, message_obj.fwd_from)

        if isinstance(message_obj.to_id, PeerChannel):
            is_channel = True
            is_group = False
            is_private = False
        elif isinstance(message_obj.to_id, PeerChat):
            is_channel = False
            is_group = True
            is_private = False
        else:
            is_channel = False
            is_group = False
            is_private = False

        # We track the channel size and set it to expire after sometime, if it does we update the participant size
        if channel_id in self.channel_meta and self.channel_meta[channel_id]['channel_size'] == 0 or datetime.now() > self.channel_meta[channel_id]['channel_texpire']:
            logging.info('refreshing the channel information')
            channel_size = await self.get_channel_user_count(channel_id)
        else:
            channel_size = self.channel_meta[channel_id]['channel_size']

        # Lets get who sent the message
        sender = await event.get_sender()
        sender_username = sender.username

        channel_id = abs(channel_id)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if self.telegram_enabled:
            # Set the message for the notification we're about to send in our monitor channel
            message = '⚠️ "{}" mentioned by {} in => "{}" url: {}\n\n Message:\n"{}\ntimestamp: {}'.format(keyword, sender_username, self.channel_meta[channel_id]['channel_title'], self.channel_meta[channel_id]['channel_url'], message_text,timestamp)
            logging.info('{} Sending notification {}'.format(sys._getframe().f_code.co_name, message))
            await self.client.send_message(self.monitor_channel, message)

        if self.g_enabled:
            self.sheet.append_row([
                sender_id,
                sender_username,
                channel_id,
                self.channel_meta[channel_id]['channel_title'],
                self.channel_meta[channel_id]['channel_url'],
                keyword,
                message_text,
                is_mention,
                is_scheduled,
                is_fwd,
                is_reply,
                is_bot,
                is_channel,
                is_group,
                is_private,
                channel_size,
                timestamp
            ])

        if self.sql_enabled:
            o = await self.get_user_by_id(sender_id)

            self.session = self.Session()
            if not bool(self.session.query(ChatUser).filter_by(chat_user_id=sender_id if o['is_valid_user'] else -1).all()):
                logging.info("Message user already in DB.")
                self.session.add(ChatUser(
                    chat_user_id=sender_id if o['is_valid_user'] else -1,
                    chat_user_is_bot=o['is_bot'],
                    chat_user_is_verified=o['is_verified'],
                    chat_user_is_restricted=o['is_restricted'],
                    chat_user_first_name=o['first_name'],
                    chat_user_last_name=o['last_name'],
                    chat_user_name=o['username'],
                    chat_user_phone=o['phone'],
                    chat_user_tlogin=datetime.now(),
                    chat_user_tmodified=datetime.now()
                ))

            # Add message
            msg = Message(
                chat_user_id=sender_id if o['is_valid_user'] else -1,
                account_id=self.account.account_id,
                channel_id=channel_id,
                keyword_id=keyword_id,
                message_text=message_text,
                message_is_mention=is_mention,
                message_is_scheduled=is_scheduled,
                message_is_fwd=is_fwd,
                message_is_reply=is_reply,
                message_is_bot=is_bot,
                message_is_group=is_group,
                message_is_private=is_private,
                message_is_channel=is_channel,
                message_channel_size=channel_size,
                message_tcreate=datetime.now()
            )
            self.session.add(msg)
            self.session.flush()

            message_id = msg.message_id

            self.session.add(Notification(
                keyword_id=keyword_id,
                message_id=message_id,
                channel_id=channel_id,
                account_id=self.account.account_id,
                chat_user_id=sender_id if o['is_valid_user'] else -1
            ))

            # -----------
            # Write to DB
            # -----------
            try:
                self.session.commit()
            except IntegrityError as integ_err:
                logging.error("{}: An IntegrityError has been encountered.\n{}".format(sys._getframe().f_code.co_name, str(integ_err)))
            self.session.close()

        # TODO: Add json and Elastic search outputs


    async def update_keyword_list(self):
        # ------------------------------
        # Lets update keywords in memory
        # ------------------------------
        # TODO: functionality to poll the DB for new keywords and refresh in memory
        logging.info('### updating keyword_list')
        pass

    # ===========================
    # Loop we run while we listen
    # ===========================
    async def bot_interval(self):
        self.init_keywords()
        await self.client_login()
        await self.init_monitor_channels()
        while True:
            logging.info('### Running bot interval')
            await self.update_keyword_list()
            await asyncio.sleep(self.KEYWORD_REFRESH_WAIT)

    def stop_bot_interval(self):
        self.bot_task.cancel()

    # ===========================
    # Initialize connection to TG
    # ===========================
    def init(self):
        loop = asyncio.get_event_loop()
        self.bot_task = loop.create_task(self.bot_interval())

        with self.client:
            self.client.run_until_disconnected()
            try:
                loop.run_until_complete(self.bot_task)
            except asyncio.CancelledError:
                logging.info('### Async cancelled')
                pass