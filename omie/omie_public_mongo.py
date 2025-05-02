from dotenv import load_dotenv
from utils.mongo import Mongo

import os

load_dotenv()

class OmiePublicMongo(Mongo):

    @staticmethod
    def db_connection():
        connection = super(OmiePublicMongo, OmiePublicMongo).db_connection(os.getenv("OMIE_PUBLIC_MONGO_DATABASE"))

        return connection