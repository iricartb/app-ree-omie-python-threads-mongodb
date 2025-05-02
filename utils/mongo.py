from dotenv import load_dotenv

import os
import pymongo

load_dotenv()

class Mongo:

    connection = None

    @staticmethod
    def db_connection(database_name):
        if (Mongo.connection is None):
            credentials = ""
            if (len(os.getenv("MONGO_USER")) > 0):
                credentials += os.getenv("MONGO_USER")

                if (len(os.getenv("MONGO_PASSWORD")) > 0):
                    credentials += ":" + os.getenv('MONGO_PASSWORD')

                credentials += "@"

            server_address = os.getenv('MONGO_HOSTNAME')
            if (len(os.getenv("MONGO_PORT")) > 0):
                server_address += ":" + os.getenv("MONGO_PORT")

            url = f"{os.getenv('MONGO_PROTOCOL')}://{credentials}{server_address}"

            Mongo.connection = pymongo.MongoClient(url)[database_name]

        return Mongo.connection

    @staticmethod
    def db_close():
        if (Mongo.connection is not None):
            Mongo.connection.client.close()
            Mongo.connection = None