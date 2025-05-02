from dotenv import load_dotenv
from utils.mongo import Mongo

import os

load_dotenv()

class ReePublicMongo(Mongo):

    @staticmethod
    def db_connection():
        connection = super(ReePublicMongo, ReePublicMongo).db_connection(os.getenv("REE_PUBLIC_MONGO_DATABASE"))

        return connection