from omie.omie_public_mongo import OmiePublicMongo
from utils.helpers import Helpers

from datetime import datetime, timedelta
from pymongo import ASCENDING
from utils.log import Log

import calendar
import os
import queue
import requests
import shutil
import threading

class OmiePublicFiles:
    # Download folder
    DOWNLOAD_FOLDER = "downloads/omie"

    # Transform hourly to quarterhourly
    TRANSFORM_HOURLY_TO_QUARTERHOURLY = True
    
    # Transform quarterhourly to hourly
    TRANSFORM_QUARTERHOURLY_TO_HOURLY = True

    # Collection quarterhourly suffix
    COLLECTION_QUARTERHOURLY_SUFFIX = "qh"

    # Process files
    PROCESS_FILES = [
        # OMIE_MD / OMIE_MI1..OMIE_MI3
        { "file" : "MD",  "collection" : "files_markets_breakdown" },
        { "file" : "MI1", "collection" : "files_markets_breakdown" },
        { "file" : "MI2", "collection" : "files_markets_breakdown" },
        { "file" : "MI3", "collection" : "files_markets_breakdown" }
    ]

    # Collections & indexes
    COLLECTION_INDEXES = [
        { "collection" : "files",                      "indexes" : [ "file", "type" ] },

        { "collection" : "files_markets_breakdown",    "indexes" : [ "market", "date", "hour", "country" ] },
        { "collection" : "files_markets",              "indexes" : [ "market", "date", "country" ] },

        { "collection" : "files_markets_breakdown_qh", "indexes" : [ "market", "date", "hour", "quarter", "country" ] },
        { "collection" : "files_markets_qh",           "indexes" : [ "market", "date", "country" ] }
	]

    # Logger
    logger = Log.get_logger(os.getenv("OMIE_PUBLIC_LOG_NAME"))

    ##########################################################################################################################
    ##  CREATE COLLECTIONS INDEXES                                                                                          ##
    ##########################################################################################################################
    @staticmethod
    def create_collections_indexes():
        try:
            db_connection = OmiePublicMongo.db_connection()

            for collection_data in OmiePublicFiles.COLLECTION_INDEXES:
                collection_update = db_connection[collection_data["collection"]]

                collection_update.create_index([(index, ASCENDING) for index in collection_data["indexes"]], unique=True)

                if (collection_data["collection"] != "files"):
                    collection_update.create_index([("file_id", ASCENDING)])

            OmiePublicMongo.db_close()

        except Exception as e:
            OmiePublicFiles.logger.error(e)

    ##########################################################################################################################
    ##  DOWNLOAD FILES                                                                                                      ##
    ##########################################################################################################################
    @staticmethod
    def download_files(start_date, end_date):
        try:
            download_files_results = []

            if (start_date <= end_date):
                # Create folder to download the files
                if not os.path.exists(OmiePublicFiles.DOWNLOAD_FOLDER):
                    os.makedirs(OmiePublicFiles.DOWNLOAD_FOLDER)

                OmiePublicFiles.logger.info(f"[ + ] Descargando ficheros de OMIE sobre las fechas {datetime.strptime(start_date, '%Y-%m-%d').strftime('%d/%m/%Y')} y {datetime.strptime(end_date, '%Y-%m-%d').strftime('%d/%m/%Y')}...")

                threads_num = os.getenv("OMIE_PUBLIC_THREADS_NUM")

                if (threads_num is not None):
                    threads_num = int(threads_num)
                else:
                    threads_num = 1
                
                if (threads_num > 1):
                    # Create queue tasks
                    queue_tasks = queue.Queue()

                    for delta in range((datetime.strptime(end_date, "%Y-%m-%d").date() - datetime.strptime(start_date, "%Y-%m-%d").date()).days + 1):
                        current_date = datetime.strptime(start_date, "%Y-%m-%d").date() + timedelta(days=delta)

                        for process_file_data in OmiePublicFiles.PROCESS_FILES:
                            queue_tasks.put(process_file_data["file"] + "_" + current_date.strftime("%Y%m%d"))

                    # Create threads
                    threads = []

                    for _ in range(threads_num):
                        thread = threading.Thread(target=OmiePublicFiles.thread_download_file, args=(queue_tasks, download_files_results))
                        thread.start()
                        threads.append(thread)

                    # Wait all threads
                    for thread in threads:
                        thread.join()
                else:
                    for delta in range((datetime.strptime(end_date, "%Y-%m-%d").date() - datetime.strptime(start_date, "%Y-%m-%d").date()).days + 1):
                        current_date = datetime.strptime(start_date, "%Y-%m-%d").date() + timedelta(days=delta)

                        for process_file_data in OmiePublicFiles.PROCESS_FILES:
                            file_name = OmiePublicFiles.download_file(process_file_data["file"] + "_" + current_date.strftime("%Y%m%d"))

                            if file_name:
                            	download_files_results.append(file_name)

            if (len(download_files_results) > 0):
                download_files_results = sorted(download_files_results)
                
                OmiePublicFiles.logger.info("[ - ] Listado de ficheros resultantes:")
                for download_file in download_files_results:
                    OmiePublicFiles.logger.info(f"[ - ] {download_file}")

            return download_files_results

        except Exception as e:
            OmiePublicFiles.logger.error(e)

            return []

    ##########################################################################################################################
    ##  THREAD DOWNLOAD FILE                                                                                                ##
    ##########################################################################################################################
    @staticmethod
    def thread_download_file(queue_tasks, download_files_results):
        while not queue_tasks.empty():
            task = queue_tasks.get()

            file_name = OmiePublicFiles.download_file(task)

            if file_name:
                download_files_results.append(file_name)

    ##########################################################################################################################
    ##  DOWNLOAD FILE                                                                                                       ##
    ##########################################################################################################################
    @staticmethod
    def download_file(file_name):
        file_write = False

        OmiePublicFiles.logger.info(f"[ - ] Descargando el fichero {file_name}...")

        file_name_split = file_name.split("_")

        if (len(file_name_split) > 1):
            file_type = file_name_split[0]
            file_date = file_name_split[1]

            for version in range(10):
                file_url = ""

                if (file_type[:2] in [ "MD", "MI" ]):
                    file_url = f"{os.getenv('OMIE_PUBLIC_URL_PROTOCOL')}://{os.getenv('OMIE_PUBLIC_URL_HOSTNAME')}/{os.getenv('OMIE_PUBLIC_URL_ENDPOINT')}?parents%5B0%5D={os.getenv('OMIE_PUBLIC_URL_PARENTS_' + file_type[:2])}&filename={os.getenv('OMIE_PUBLIC_URL_FILENAME_' + file_type[:2]) + '_' + file_date}{'0' + file_type[2:] if len(file_type) >= 3 else ''}.{version + 1}"

                if file_url:
                    file_response = requests.get(file_url)

                    file_path = os.path.join(OmiePublicFiles.DOWNLOAD_FOLDER, file_name)

                    if (file_response.status_code == 200):
                        if file_response.content:
                            with open(file_path, "wb") as file:
                                file.write(file_response.content)
                                file_write = True

                    if file_write:
                        break

        if not file_write:
            file_name = ""

        return file_name

    ##########################################################################################################################
    ##  PROCESS FILES                                                                                                       ##
    ##########################################################################################################################
    @staticmethod
    def process_files(files):
        try:
            if (len(files) > 0):
                OmiePublicFiles.logger.info(f"[ + ] Procesando ficheros...")

                threads_num = os.getenv("OMIE_PUBLIC_THREADS_NUM")

                if (threads_num is not None):
                    threads_num = int(threads_num)
                else:
                    threads_num = 1
                
                if (threads_num > 1):
                    # Create queue tasks
                    queue_tasks = queue.Queue()

                    for file in files:
                        queue_tasks.put(os.path.join(OmiePublicFiles.DOWNLOAD_FOLDER, file))

                    # Create threads
                    threads = []

                    for _ in range(threads_num):
                        thread = threading.Thread(target=OmiePublicFiles.thread_process_file, args=(queue_tasks,))
                        thread.start()
                        threads.append(thread)

                    # Wait all threads
                    for thread in threads:
                        thread.join()
                else:
                    for file in files:
                        OmiePublicFiles.process_file(os.path.join(OmiePublicFiles.DOWNLOAD_FOLDER, file))

        except Exception as e:
            OmiePublicFiles.logger.error(e)

    ##########################################################################################################################
    ##  THREAD PROCESS FILE                                                                                                 ##
    ##########################################################################################################################
    @staticmethod
    def thread_process_file(queue_tasks):
        while not queue_tasks.empty():
            task = queue_tasks.get()

            OmiePublicFiles.process_file(task)

    ##########################################################################################################################
    ##  PROCESS FILE                                                                                                        ##
    ##########################################################################################################################
    @staticmethod
    def process_file(file_path):
        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        file_type = file_name_split[0]

        process_file_item = next((process_file_data for process_file_data in OmiePublicFiles.PROCESS_FILES if process_file_data["file"] == file_name_split[0]), None)

        if process_file_item:
            try:
                OmiePublicFiles.logger.info(f"[ - ] Procesando el fichero {file_name}...")

                # Save mongo file information
                db_connection = OmiePublicMongo.db_connection()

                collection = db_connection["files"]

                condition = {
                    "file"   : file_name,
                    "type"   : file_type
                }

                document = {
                    "file"      : file_name,
                    "type"      : file_type,
                    "processed" : datetime.utcnow(),
                    "timestamp" : datetime.utcnow()
                }

                operation = {
                    "$set": document
                }

                operation_result = collection.update_one(condition, operation, upsert=True)

                if operation_result.upserted_id:
                    file_id = operation_result.upserted_id  
                else:
                    file_id = collection.find_one(condition)["_id"]

                # Call dynamic process function
                function = getattr(OmiePublicFiles, "process_files_" + file_type + "_handler")

                function(file_id, file_path, file_type, process_file_item["collection"])

            except Exception as e:
                OmiePublicFiles.logger.error(e)

    ##########################################################################################################################
    ##  TRANSFORM TIME DOCUMENTS                                                                                            ##
    ##########################################################################################################################
    @staticmethod
    def transform_time_documents(collection_name, documents, time, time_fields, update_fields):
        # Transform hourly to quarterhourly
        if ((OmiePublicFiles.TRANSFORM_HOURLY_TO_QUARTERHOURLY) and (time == "hourly")):
            collection_name += "_" + OmiePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            documents_quarterhourly = Helpers.transform_hourly_to_quarterhourly(documents, time_fields)

            Helpers.bulk_update(OmiePublicMongo.db_connection(), collection_name, documents_quarterhourly, update_fields)
        
        # Transform quarterhourly to hourly
        elif ((OmiePublicFiles.TRANSFORM_QUARTERHOURLY_TO_HOURLY) and (time == "quarterhourly")):
            collection_name = collection_name.replace("_" + OmiePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX, "")

            documents_hourly = Helpers.transform_quarterhourly_to_hourly(documents, time_fields)

            Helpers.bulk_update(OmiePublicMongo.db_connection(), collection_name, documents_hourly, update_fields)

    ##########################################################################################################################
    ##  DYNAMIC FUNCTIONS HANDLERS: The following parameters are injected: file_id, file_path, file_type, collection_name   ##
    ##########################################################################################################################

    # = MD =
    @staticmethod
    def process_files_MD_handler(file_id, file_path, file_type, collection_name):
        OmiePublicFiles.process_files_markets(file_id, file_path, file_type, collection_name)

    # = MI1 =
    @staticmethod
    def process_files_MI1_handler(file_id, file_path, file_type, collection_name):
        OmiePublicFiles.process_files_markets(file_id, file_path, file_type, collection_name)

    # = MI2 =
    @staticmethod
    def process_files_MI2_handler(file_id, file_path, file_type, collection_name):
        OmiePublicFiles.process_files_markets(file_id, file_path, file_type, collection_name)

    # = MI3 =
    @staticmethod
    def process_files_MI3_handler(file_id, file_path, file_type, collection_name):
        OmiePublicFiles.process_files_markets(file_id, file_path, file_type, collection_name)

    ##########################################################################################################################
    ##  DYNAMIC FUNCTIONS MANAGEMENT: Management of dynamic funcions                                                        ##
    ##########################################################################################################################
    
    # = Markets =
    @staticmethod
    def process_files_markets(file_id, file_path, file_type, collection_name):
        time = "hourly"

        offset = 0
        num_line = 0
        documents = []

        with open(file_path, "r") as file:
            for line in file:
                if ((num_line > 0) and (len(line) > 3)):                    
                    line_split = line.split(";")

                    if (len(line_split) > 7):
                        time = "quarterhourly"
                        offset = 1

                    for country_offset in range(2):
                        document = {
                            "file_id"         : file_id,
                            "market"          : file_type,
                            "date"            : line_split[0] + "-" + line_split[1] + "-" + line_split[2],
                            "hour"            : int(line_split[3]),
                            "country"         : "PT" if country_offset == 0 else "ES",
                            "price"           : float(line_split[4 + offset + country_offset]),
                            "timestamp"       : datetime.utcnow()
                        }

                        if (time == "quarterhourly"):
                            document["quarter"] = int(line_split[4])

                        documents.append(document)

                num_line += 1

        update_fields_condition = [ "market", "date", "hour", "country" ]

        if (time == "quarterhourly"):
            collection_name += "_" + OmiePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            update_fields_condition.append("quarter")

        Helpers.bulk_update(OmiePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

        OmiePublicFiles.process_files_markets_daily(file_id, collection_name, time)

    # = Markets daily =
    @staticmethod
    def process_files_markets_daily(file_id, collection_name, time):
        db_connection = OmiePublicMongo.db_connection()

        collection = db_connection[collection_name]

        documents_file = collection.find({"file_id": file_id})

        documents = {}

        if (time == "hourly"):
            for document in documents_file:
                key = (document["date"], document["country"])
                
                if (key not in documents):
                    documents[key] = {
                        "file_id"         : document["file_id"],
                        "market"          : document["market"],
                        "date"            : document["date"],
                        "country"         : document["country"],
                        **{f"H{hour:02d}" : 0 for hour in range(1, 26)},
                        "avg_price"       : 0,
                        "avg_count"       : 0
                    }

                documents[key][f"H{document['hour']:02d}"] += document["price"]
                documents[key]["avg_price"] += document["price"]
                documents[key]["avg_count"] += 1

        elif (time == "quarterhourly"):
            for document in documents_file:
                key = (document["date"], document["country"])
                
                if (key not in documents):
                    documents[key] = {
                        "file_id"         : document["file_id"],
                        "market"          : document["market"],
                        "date"            : document["date"],
                        "country"         : document["country"],
                        **{f"H{hour:02d}" : {f"QH{quarter:02d}": 0 for quarter in range(1, 5)} for hour in range(1, 26)},
                        "avg_price"       : 0,
                        "avg_count"       : 0
                    }

                documents[key][f"H{document['hour']:02d}"][f"QH{document['quarter']:02d}"] += document["price"]
                documents[key]["avg_price"] += document["price"]
                documents[key]["avg_count"] += 1

        if (len(documents) > 0):
            # Calculate average
            for key in documents.keys():
                if (documents[key]["avg_count"] != 0):
                    documents[key]["avg_price"] = round(documents[key]["avg_price"] / documents[key].pop("avg_count"), 2)

            documents = documents.values()

        update_fields_condition = [ "market", "date", "country" ]

        collection_name = collection_name.replace("_breakdown", "")

        Helpers.bulk_update(OmiePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

        # Transform time (quarterhourly to hourly or hourly to quarterhourly)
        time_fields = [ "H" ]

        OmiePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)