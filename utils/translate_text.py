import json
from models import Message
import os
import cld3
import requests
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import uuid
import yaml

config = None
with open("../config.yaml", "r") as conf_file:
    config = yaml.load(conf_file, Loader=yaml.FullLoader)

# Constants
MYSQL_CONNECTOR_STRING = 'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(config["config"]["database"]["sql"]["username"],
                                                                        config["config"]["database"]["sql"]["password"],
                                                                        config["config"]["database"]["sql"]["hostname"],
                                                                        config["config"]["database"]["sql"]["port"],
                                                                        config["config"]["database"]["sql"]["database"])
AZURE_TRANSLATE_KEY = config["config"]["api"]["azure_translate"]["key"]
AZURE_TRANSLATE_ENDPOINT = config["config"]["api"]["azure_translate"]["endpoint"]
AZURE_TRANSLATE_PATH = "/translate?api-version=3.0"
AZURE_TRANSLATE_PARAMS = "&to=en"
AZURE_TRANSLATE_URI = AZURE_TRANSLATE_ENDPOINT + AZURE_TRANSLATE_PATH + AZURE_TRANSLATE_PARAMS
AZURE_TRANSLATE_HEADERS = { 
    'Ocp-Apim-Subscription-Key': AZURE_TRANSLATE_KEY,
    'Content-type': 'application/json',
    'X-ClientTraceId': str(uuid.uuid4())
}

# Prepare the database
engine = sqlalchemy.create_engine(MYSQL_CONNECTOR_STRING)
session_maker = sessionmaker(bind=engine)
session = session_maker()

# Fetch all messages in the DB
character_count = 2228966
messages = session.query(Message).filter_by(message_translated=None).all()
for message in messages:
    if cld3.get_language(message.message_text).language == "en":
        message.message_translated = response[0]["translations"][0]["text"]
        message.message_language = response[0]["detectedLanguage"]["language"]
        print("Message ID {} is already in English.".format(message.message_id))
        session.commit()
        continue

    request_body = [{
        'text': message.message_text
    }]

    request = requests.post(AZURE_TRANSLATE_URI, headers=AZURE_TRANSLATE_HEADERS, json=request_body)
    if request.status_code != 200:
        print("Message ID {} failed to translate.".format(message.message_id))
        continue

    response = request.json()
    character_count += len(response[0]["translations"][0]["text"])
    print("Translating message ID {}. Total characters translated: {}".format(message.message_id, character_count))
    message.message_translated = response[0]["translations"][0]["text"]
    message.message_language = response[0]["detectedLanguage"]["language"]
    session.commit()

print("Done.")