from ree.ree_public_api import ReePublicApi
from ree.ree_public_mongo import ReePublicMongo
from utils.helpers import Helpers

from datetime import datetime
from pymongo import ASCENDING
from utils.log import Log

import calendar
import os
import queue
import requests
import shutil
import threading
import zipfile

class ReePublicFiles:
    # API liquidations
    API_LIQUIDATIONS = [ 
                         "A1", "A2", "A3", "A4", "A5", "A6", 
                         "C1", "C2", "C3", "C4", "C5", "C6" 
                       ]

    # API files
    API_FILES = [ "liquicomun" ]

    # Download folder
    DOWNLOAD_FOLDER = "downloads/ree"

    # Transform hourly to quarterhourly
    TRANSFORM_HOURLY_TO_QUARTERHOURLY = True
    
    # Transform quarterhourly to hourly
    TRANSFORM_QUARTERHOURLY_TO_HOURLY = True

    # Collection quarterhourly suffix
    COLLECTION_QUARTERHOURLY_SUFFIX = "qh"

    # Num hourly fields
    NUM_HOURLY_FIELDS = 30

    # Process files
    PROCESS_FILES = [
        # Codsvbaj / Codsvsub
        { "file" : "codsvbaj", "collection" : "files_codsvbaj" },
        { "file" : "codsvsub", "collection" : "files_codsvsub" },

        # Compodem
        { "file" : "compodem", "collection" : "files_compodem_breakdown" },

        # Losses subsystem (PENINSULA)
        { "file" : "perd20TD",   "collection" : "files_losses" },
        { "file" : "perd30TD",   "collection" : "files_losses" },
        { "file" : "perd61TD",   "collection" : "files_losses" },
        { "file" : "perd62TD",   "collection" : "files_losses" },
        { "file" : "perd63TD",   "collection" : "files_losses" },
        { "file" : "perd64TD",   "collection" : "files_losses" },
        { "file" : "perd30TDVE", "collection" : "files_losses" },
        { "file" : "perd61TDVE", "collection" : "files_losses" },

        # Losses subsystem (MELILLA, CEUTA, CANARIAS, BALEARES)
        { "file" : "Sperd20TD",   "collection" : "files_losses" },
        { "file" : "Sperd30TD",   "collection" : "files_losses" },
        { "file" : "Sperd61TD",   "collection" : "files_losses" },
        { "file" : "Sperd62TD",   "collection" : "files_losses" },
        { "file" : "Sperd63TD",   "collection" : "files_losses" },
        { "file" : "Sperd64TD",   "collection" : "files_losses" },
        { "file" : "Sperd30TDVE", "collection" : "files_losses" },
        { "file" : "Sperd61TDVE", "collection" : "files_losses" },

        # Periods subsystem (PENINSULA)
        { "file" : "petar3P", "collection" : "files_periods" },
        { "file" : "petar6P", "collection" : "files_periods" },

        # Periods subsystem (MELILLA, CEUTA, CANARIAS, BALEARES)
        { "file" : "Spetar3P", "collection" : "files_periods" },
        { "file" : "Spetar6P", "collection" : "files_periods" },

        # Scdsvdem
        { "file" : "Scdsvdem", "collection" : "files_scdsvdem" },

        # SphdemDD / SphvenDD
        { "file" : "SphdemDD", "collection" : "files_sphdem_dd" },
        { "file" : "SphvenDD", "collection" : "files_sphven_dd" },

        # Sprpcap subsystem (MELILLA, CEUTA, CANARIAS, BALEARES)
        { "file" : "Sprpcap20TD",   "collection" : "files_sprpcap" },
        { "file" : "Sprpcap30TD",   "collection" : "files_sprpcap" },
        { "file" : "Sprpcap61TD",   "collection" : "files_sprpcap" },
        { "file" : "Sprpcap62TD",   "collection" : "files_sprpcap" },
        { "file" : "Sprpcap63TD",   "collection" : "files_sprpcap" },
        { "file" : "Sprpcap64TD",   "collection" : "files_sprpcap" },
        { "file" : "Sprpcap30TDVE", "collection" : "files_sprpcap" },
        { "file" : "Sprpcap61TDVE", "collection" : "files_sprpcap" }
    ]

    # Collections & indexes
    COLLECTION_INDEXES = [
        { "collection" : "files",                       "indexes" : [ "zip", "file", "liquidation" ] },

        { "collection" : "files_codsvbaj",              "indexes" : [ "liquidation", "date" ] },
        { "collection" : "files_codsvsub",              "indexes" : [ "liquidation", "date" ] },
        { "collection" : "files_compodem_breakdown",    "indexes" : [ "liquidation", "date", "hour", "segment", "type_demand" ] },
        { "collection" : "files_compodem",              "indexes" : [ "liquidation", "date", "segment", "type_demand" ] },
        { "collection" : "files_losses",                "indexes" : [ "liquidation", "date", "tariff_id", "subsystem_id" ] },
        { "collection" : "files_periods",               "indexes" : [ "liquidation", "date", "tariff_id", "subsystem_id" ] },
        { "collection" : "files_scdsvdem",              "indexes" : [ "liquidation", "date" ] },
        { "collection" : "files_sphdem_dd",             "indexes" : [ "liquidation", "date", "subsystem_id" ] },
        { "collection" : "files_sphven_dd",             "indexes" : [ "liquidation", "date", "subsystem_id" ] },
        { "collection" : "files_sprpcap",               "indexes" : [ "liquidation", "date", "tariff_id", "subsystem_id" ] },

        { "collection" : "files_codsvbaj_qh",           "indexes" : [ "liquidation", "date" ] },
        { "collection" : "files_codsvsub_qh",           "indexes" : [ "liquidation", "date" ] },
        { "collection" : "files_compodem_breakdown_qh", "indexes" : [ "liquidation", "date", "hour", "quarter", "segment", "type_demand" ] },
        { "collection" : "files_compodem_qh",           "indexes" : [ "liquidation", "date", "segment", "type_demand" ] },
        { "collection" : "files_losses_qh",             "indexes" : [ "liquidation", "date", "tariff_id", "subsystem_id" ] },
        { "collection" : "files_periods_qh",            "indexes" : [ "liquidation", "date", "tariff_id", "subsystem_id" ] },
        { "collection" : "files_scdsvdem_qh",           "indexes" : [ "liquidation", "date" ] },
        { "collection" : "files_sphdem_dd_qh",          "indexes" : [ "liquidation", "date", "subsystem_id" ] },
        { "collection" : "files_sphven_dd_qh",          "indexes" : [ "liquidation", "date", "subsystem_id" ] },
        { "collection" : "files_sprpcap_qh",            "indexes" : [ "liquidation", "date", "tariff_id", "subsystem_id" ] }
    ]
    
    # Logger
    logger = Log.get_logger(os.getenv("REE_PUBLIC_LOG_NAME"))

    ##########################################################################################################################
    ##  CREATE COLLECTIONS INDEXES                                                                                          ##
    ##########################################################################################################################
    @staticmethod
    def create_collections_indexes():
        try:
            db_connection = ReePublicMongo.db_connection()

            for collection_data in ReePublicFiles.COLLECTION_INDEXES:
                collection_update = db_connection[collection_data["collection"]]

                collection_update.create_index([(index, ASCENDING) for index in collection_data["indexes"]], unique=True)

                if (collection_data["collection"] != "files"):
                    collection_update.create_index([("file_id", ASCENDING)])

            ReePublicMongo.db_close()

        except Exception as e:
            ReePublicFiles.logger.error(e)

    ##########################################################################################################################
    ##  DOWNLOAD FILES                                                                                                      ##
    ##########################################################################################################################
    @staticmethod
    def download_files(start_date, end_date, liquidations = API_LIQUIDATIONS, files = API_FILES):
        try:
            download_files_results = []

            if ((start_date <= end_date) and (len(liquidations) > 0) and (len(files) > 0)):

                # Liquidation files to download
                liquidation_files = []

                for liquidation in liquidations:
                    for file in files:
                        liquidation_files.append(liquidation + "_" + file)

                # Create folder to download the files
                if not os.path.exists(ReePublicFiles.DOWNLOAD_FOLDER):
                    os.makedirs(ReePublicFiles.DOWNLOAD_FOLDER)

                # API call
                ReePublicFiles.logger.info(f"[ + ] Realizando la llamada API a REE sobre las fechas {datetime.strptime(start_date, '%Y-%m-%d').strftime('%d/%m/%Y')} y {datetime.strptime(end_date, '%Y-%m-%d').strftime('%d/%m/%Y')}...")
                api_response = ReePublicApi.call(start_date, end_date)

                # API response management
                if (len(liquidation_files) > 0):
                    if ("archives" in api_response):
                        for api_file in api_response["archives"]:
                            if (("name" in api_file) and ("archive_type" in api_file) and ("download" in api_file) and ("date_times" in api_file) and (len(api_file["date_times"]) >= 2)):
                                if (api_file["name"] in liquidation_files):
                                    if ("url" in api_file["download"]):
                                        publication_date = "0000-00-00"

                                        if (("publication_date" in api_file) and (len(api_file["publication_date"]) >= 1)):
                                            publication_date = api_file["publication_date"][0]

                                        file_start_date = api_file["date_times"][0][:4] + api_file["date_times"][0][5:7]
                                        file_end_date = api_file["date_times"][1][:4] + api_file["date_times"][1][5:7]

                                        if (file_start_date != file_end_date):
                                            file_name = api_file["name"] + "_" + file_start_date + "_" + file_end_date + "." + api_file["archive_type"]
                                        else:
                                            file_name = api_file["name"] + "_" + file_start_date + "." + api_file["archive_type"]

                                        ReePublicFiles.logger.info(f"[ - ] Descargando el fichero {file_name}...")

                                        file_url = os.getenv("REE_PUBLIC_API_PROTOCOL") + "://" + os.getenv("REE_PUBLIC_API_HOSTNAME") + api_file["download"]["url"]
                                        
                                        file_response = requests.get(file_url)

                                        file_path = os.path.join(ReePublicFiles.DOWNLOAD_FOLDER, file_name)

                                        if (file_response.status_code == 200):
                                            file_write = False
                                            with open(file_path, "wb") as file:
                                                file.write(file_response.content)
                                                file_write = True

                                            if file_write:
                                                file_extract = False
                                                if (api_file["archive_type"] == "zip"):
                                                    with zipfile.ZipFile(file_path, "r") as file:
                                                        zip_files = file.namelist()
                                                        
                                                        zip_grouped = False
                                                        for zip_file in zip_files:
                                                            if zip_file.endswith(".zip"):
                                                                zip_grouped = True
                                                                break 

                                                        if zip_grouped:
                                                            ReePublicFiles.logger.info(f"[ - ] Descomprimiendo el fichero agrupado {file_name}...")
                                                            file.extractall(ReePublicFiles.DOWNLOAD_FOLDER)
                                                            
                                                            for zip_file in zip_files:
                                                                download_files_results.append(zip_file)

                                                            file_extract = True
                                                        
                                                if file_extract:
                                                    os.remove(file_path)
                                                else:
                                                    download_files_results.append(file_name)

            if (len(download_files_results) > 0):
                download_files_results = sorted(download_files_results)

                ReePublicFiles.logger.info("[ - ] Listado de ficheros resultantes:")
                for download_file in download_files_results:
                    ReePublicFiles.logger.info(f"[ - ] {download_file}")

            return download_files_results

        except Exception as e:
            ReePublicFiles.logger.error(e)

            return []

    ##########################################################################################################################
    ##  EXTRACT FILES                                                                                                       ##
    ##########################################################################################################################
    @staticmethod
    def extract_files(files):
        try:
            extract_files_results = []

            if (len(files) > 0):

                ReePublicFiles.logger.info(f"[ + ] Descomprimiendo ficheros...")

                for file in files:
                    if file.endswith(".zip"):
                        file_path = os.path.join(ReePublicFiles.DOWNLOAD_FOLDER, file)

                        with zipfile.ZipFile(file_path, "r") as zip_file:
                            # Create folder to extract zip file
                            folder_name = os.path.splitext(file)[0]
                            folder_path = os.path.join(ReePublicFiles.DOWNLOAD_FOLDER, folder_name)

                            if not os.path.exists(folder_path):
                                os.makedirs(folder_path)

                            ReePublicFiles.logger.info(f"[ - ] Descomprimiendo el fichero {file} en la carpeta {folder_name}...")
                            zip_file.extractall(folder_path)
                            extract_files_results.append(folder_name)

            if (len(extract_files_results) > 0):
                extract_files_results = sorted(extract_files_results)

                ReePublicFiles.logger.info("[ - ] Listado de carpetas resultantes:")
                for extract_folder in extract_files_results:
                    ReePublicFiles.logger.info(f"[ - ] {extract_folder}")

            return extract_files_results

        except Exception as e:
            ReePublicFiles.logger.error(e)

            return []

    ##########################################################################################################################
    ##  PROCESS FOLDERS                                                                                                     ##
    ##########################################################################################################################
    @staticmethod
    def process_folders(folders):
        try:
            if (len(folders) > 0):
                ReePublicFiles.logger.info(f"[ + ] Procesando carpetas...")

                for folder in folders:
                    ReePublicFiles.logger.info(f"[ - ] Procesando la carpeta {folder}...")

                    folder_path = os.path.join(ReePublicFiles.DOWNLOAD_FOLDER, folder)

                    if os.path.exists(folder_path):
                        files = sorted(os.listdir(folder_path), key=str.lower)

                        threads_num = os.getenv("REE_PUBLIC_THREADS_NUM")

                        if (threads_num is not None):
                            threads_num = int(threads_num)
                        else:
                            threads_num = 1

                        if (threads_num > 1):
                            # Create queue tasks
                            queue_tasks = queue.Queue()

                            for file in files:
                                queue_tasks.put(os.path.join(folder_path, file))

                            # Create threads
                            threads = []

                            for _ in range(threads_num):
                                thread = threading.Thread(target=ReePublicFiles.thread_process_file, args=(queue_tasks,))
                                thread.start()
                                threads.append(thread)

                            # Wait all threads
                            for thread in threads:
                                thread.join()
                        else:
                            for file in files:
                                ReePublicFiles.process_file(os.path.join(folder_path, file))

                        shutil.rmtree(folder_path)

            ReePublicMongo.db_close()

        except Exception as e:
            ReePublicFiles.logger.error(e)

    ##########################################################################################################################
    ##  THREAD PROCESS FILE                                                                                                 ##
    ##########################################################################################################################
    @staticmethod
    def thread_process_file(queue_tasks):
        while not queue_tasks.empty():
            task = queue_tasks.get()

            ReePublicFiles.process_file(task)

    ##########################################################################################################################
    ##  PROCESS FILE                                                                                                        ##
    ##########################################################################################################################
    @staticmethod
    def process_file(file_path):
        file_name = os.path.basename(file_path)

        folder = os.path.basename(os.path.dirname(file_path))

        file_name_split = file_name.split("_")

        if (len(file_name_split) > 1):
            liquidation = file_name_split[0]

            process_file_name = file_name_split[1]

            process_file_item = next((process_file_data for process_file_data in ReePublicFiles.PROCESS_FILES if process_file_data["file"] == process_file_name), None)

            if process_file_item:
                try:
                    ReePublicFiles.logger.info(f"[ - ] Procesando el fichero {file_name}...")

                    # Save mongo file information
                    db_connection = ReePublicMongo.db_connection()

                    collection = db_connection["files"]

                    condition = {
                        "zip"         : folder + ".zip",
                        "file"        : file_name,
                        "liquidation" : liquidation
                    }

                    document = {
                        "zip"         : folder + ".zip",
                        "file"        : file_name,
                        "liquidation" : liquidation,
                        "processed"   : datetime.utcnow(),
                        "timestamp"   : datetime.utcnow()
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
                    function = getattr(ReePublicFiles, "process_files_" + process_file_name + "_handler")

                    function(file_id, file_path, liquidation, process_file_item["collection"])
                
                except Exception as e:
                    ReePublicFiles.logger.error(e)

    ##########################################################################################################################
    ##  TRANSFORM TIME DOCUMENTS                                                                                            ##
    ##########################################################################################################################
    @staticmethod
    def transform_time_documents(collection_name, documents, time, time_fields, update_fields):
        # Transform hourly to quarterhourly
        if ((ReePublicFiles.TRANSFORM_HOURLY_TO_QUARTERHOURLY) and (time == "hourly")):
            collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            documents_quarterhourly = Helpers.transform_hourly_to_quarterhourly(documents, time_fields)

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents_quarterhourly, update_fields)
        
        # Transform quarterhourly to hourly
        elif ((ReePublicFiles.TRANSFORM_QUARTERHOURLY_TO_HOURLY) and (time == "quarterhourly")):
            collection_name = collection_name.replace("_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX, "")

            documents_hourly = Helpers.transform_quarterhourly_to_hourly(documents, time_fields)

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents_hourly, update_fields)

    ##########################################################################################################################
    ##  DYNAMIC FUNCTIONS HANDLERS: The following parameters are injected: file_id, file_path, liquidation, collection_name ##
    ##########################################################################################################################

    # = Codsvbaj =
    @staticmethod
    def process_files_codsvbaj_handler(file_id, file_path, liquidation, collection_name):
        sign = -1

        ReePublicFiles.process_files_codsv(file_id, file_path, liquidation, collection_name, sign)

    # = Codsvsub =
    @staticmethod
    def process_files_codsvsub_handler(file_id, file_path, liquidation, collection_name):
        sign = 1

        ReePublicFiles.process_files_codsv(file_id, file_path, liquidation, collection_name, sign)

    # = Compodem =
    @staticmethod
    def process_files_compodem_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_compodem(file_id, file_path, liquidation, collection_name)

    # = perd20TD =
    @staticmethod
    def process_files_perd20TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = perd30TD =
    @staticmethod
    def process_files_perd30TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = perd61TD =
    @staticmethod
    def process_files_perd61TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = perd62TD =
    @staticmethod
    def process_files_perd62TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = perd63TD =
    @staticmethod
    def process_files_perd63TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = perd64TD =
    @staticmethod
    def process_files_perd64TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = perd30TDVE =
    @staticmethod
    def process_files_perd30TDVE_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = perd61TDVE =
    @staticmethod
    def process_files_perd61TDVE_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name)

    # = Sperd20TD =
    @staticmethod
    def process_files_Sperd20TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = Sperd30TD =
    @staticmethod
    def process_files_Sperd30TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = Sperd61TD =
    @staticmethod
    def process_files_Sperd61TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = Sperd62TD =
    @staticmethod
    def process_files_Sperd62TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = Sperd63TD =
    @staticmethod
    def process_files_Sperd63TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = Sperd64TD =
    @staticmethod
    def process_files_Sperd64TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = Sperd30TDVE =
    @staticmethod
    def process_files_Sperd30TDVE_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = Sperd61TDVE =
    @staticmethod
    def process_files_Sperd61TDVE_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_losses_subsystem(file_id, file_path, liquidation, collection_name)

    # = petar3P =
    @staticmethod
    def process_files_petar3P_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_periods(file_id, file_path, liquidation, collection_name)

    # = petar6P =
    @staticmethod
    def process_files_petar6P_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_periods(file_id, file_path, liquidation, collection_name)

    # = Spetar3P =
    @staticmethod
    def process_files_Spetar3P_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_periods_subsystem(file_id, file_path, liquidation, collection_name)

    # = Spetar6P =
    @staticmethod
    def process_files_Spetar6P_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_periods_subsystem(file_id, file_path, liquidation, collection_name)

    # = Scdsvdem =
    def process_files_Scdsvdem_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_scdsvdem(file_id, file_path, liquidation, collection_name)

    # = SphdemDD =
    @staticmethod
    def process_files_SphdemDD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sphdem_dd(file_id, file_path, liquidation, collection_name)

    # = SphvenDD =
    @staticmethod
    def process_files_SphvenDD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sphven_dd(file_id, file_path, liquidation, collection_name)

    # = Sprpcap20TD =
    @staticmethod
    def process_files_Sprpcap20TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    # = Sprpcap30TD =
    @staticmethod
    def process_files_Sprpcap30TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    # = Sprpcap61TD =
    @staticmethod
    def process_files_Sprpcap61TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    # = Sprpcap62TD =
    @staticmethod
    def process_files_Sprpcap62TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    # = Sprpcap63TD =
    @staticmethod
    def process_files_Sprpcap63TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    # = Sprpcap64TD =
    @staticmethod
    def process_files_Sprpcap64TD_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    # = Sprpcap30TDVE =
    @staticmethod
    def process_files_Sprpcap30TDVE_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    # = Sprpcap61TDVE =
    @staticmethod
    def process_files_Sprpcap61TDVE_handler(file_id, file_path, liquidation, collection_name):
        ReePublicFiles.process_files_sprpcap(file_id, file_path, liquidation, collection_name)

    ##########################################################################################################################
    ##  DYNAMIC FUNCTIONS MANAGEMENT: Management of dynamic funcions                                                        ##
    ##########################################################################################################################
    
    # = Codsv =
    @staticmethod
    def process_files_codsv(file_id, file_path, liquidation, collection_name, sign):
        time = "hourly"

        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        file_date = ""

        if (len(file_name_split) > 2):
            file_date = datetime.strptime(file_name_split[2], "%Y%m%d").strftime("%Y-%m")

        if file_date:
            num_line = 0
            documents = []

            with open(file_path, "r") as file:
                for line in file:
                    if ((num_line > 1) and (len(line) > 3)):
                        line_split = line.split(";")

                        date_split = line_split[0].split(" ")

                        document = {
                            "file_id"      : file_id,
                            "liquidation"  : liquidation,
                            "date"         : file_date + "-" + date_split[1],
                            "sign"         : sign,
                            "timestamp"    : datetime.utcnow()
                        }

                        if (len(line_split) <= ReePublicFiles.NUM_HOURLY_FIELDS):
                            for hour in range(1, 26):
                                if line_split[hour]:
                                    document[f"H{hour:02d}"] = float(line_split[hour])
                                else:
                                    document[f"H{hour:02d}"] = 0
                        else:
                            time = "quarterhourly"
                            
                            for hour in range(4, 101, 4):
                                document[f"H{hour // 4:02d}"] = {
                                    "QH01" : float(line_split[hour - 3]) if line_split[hour - 3] else 0,
                                    "QH02" : float(line_split[hour - 2]) if line_split[hour - 2] else 0,
                                    "QH03" : float(line_split[hour - 1]) if line_split[hour - 1] else 0,
                                    "QH04" : float(line_split[hour]) if line_split[hour] else 0,
                                }

                        documents.append(document)

                    num_line += 1

            update_fields_condition = [ "liquidation", "date" ]

            if (time == "quarterhourly"):
                collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

            # Transform time (quarterhourly to hourly or hourly to quarterhourly)
            time_fields = [ "H" ]

            ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)

    # = Compodem =
    @staticmethod
    def process_files_compodem(file_id, file_path, liquidation, collection_name):
        time = "hourly"

        offset = 0
        num_line = 0
        documents = []

        with open(file_path, "r") as file:
            for line in file:
                if ((num_line > 1) and (len(line) > 3)):
                    line_split = line.split(";")

                    if (len(line_split) > 8):
                        time = "quarterhourly"
                        offset = 1

                    document = {
                        "file_id"         : file_id,
                        "liquidation"     : liquidation,
                        "date"            : datetime.strptime(line_split[0], "%d/%m/%Y").strftime("%Y-%m-%d"),
                        "hour"            : int(line_split[1]),
                        "segment"         : line_split[2 + offset],
                        "type_demand"     : line_split[3 + offset],
                        "cost"            : float(line_split[4 + offset]),
                        "cost_eur_mwh_bc" : float(line_split[6 + offset]),
                        "demand_mwh_bc"   : float(line_split[5 + offset]),
                        "timestamp"       : datetime.utcnow()
                    }

                    if (time == "quarterhourly"):
                        document["quarter"] = int(line_split[2])

                    documents.append(document)

                num_line += 1

        update_fields_condition = [ "liquidation", "date", "hour", "segment", "type_demand" ]

        if (time == "quarterhourly"):
            collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX
            
            update_fields_condition.append("quarter")

        Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

        ReePublicFiles.process_files_compodem_daily(file_id, collection_name, time)

    # = Compodem daily =
    @staticmethod
    def process_files_compodem_daily(file_id, collection_name, time):
        db_connection = ReePublicMongo.db_connection()

        collection = db_connection[collection_name]

        documents_file = collection.find({"file_id": file_id})

        documents = {}

        if (time == "hourly"):
            for document in documents_file:
                key = (document["date"], document["segment"], document["type_demand"])
                
                if (key not in documents):
                    documents[key] = {
                        "file_id"                         : document["file_id"],
                        "liquidation"                     : document["liquidation"],
                        "date"                            : document["date"],
                        "segment"                         : document["segment"],
                        "type_demand"                     : document["type_demand"],
                        **{f"cost_h{hour:02d}"            : 0 for hour in range(1, 26)},
                        **{f"cost_eur_mwh_bc_h{hour:02d}" : 0 for hour in range(1, 26)},
                        **{f"demand_mwh_bc_h{hour:02d}"   : 0 for hour in range(1, 26)}
                    }

                documents[key][f"cost_h{document['hour']:02d}"]            += document["cost"]
                documents[key][f"cost_eur_mwh_bc_h{document['hour']:02d}"] += document["cost_eur_mwh_bc"]
                documents[key][f"demand_mwh_bc_h{document['hour']:02d}"]   += document["demand_mwh_bc"]
       
        elif (time == "quarterhourly"):
            for document in documents_file:
                key = (document["date"], document["segment"], document["type_demand"])
                
                if (key not in documents):
                    documents[key] = {
                        "file_id"                         : document["file_id"],
                        "liquidation"                     : document["liquidation"],
                        "date"                            : document["date"],
                        "segment"                         : document["segment"],
                        "type_demand"                     : document["type_demand"],
                        **{f"cost_h{hour:02d}"            : {f"QH{quarter:02d}": 0 for quarter in range(1, 5)} for hour in range(1, 26)},
                        **{f"cost_eur_mwh_bc_h{hour:02d}" : {f"QH{quarter:02d}": 0 for quarter in range(1, 5)} for hour in range(1, 26)},
                        **{f"demand_mwh_bc_h{hour:02d}"   : {f"QH{quarter:02d}": 0 for quarter in range(1, 5)} for hour in range(1, 26)}
                    }

                documents[key][f"cost_h{document['hour']:02d}"][f"QH{document['quarter']:02d}"]            += document["cost"]
                documents[key][f"cost_eur_mwh_bc_h{document['hour']:02d}"][f"QH{document['quarter']:02d}"] += document["cost_eur_mwh_bc"]
                documents[key][f"demand_mwh_bc_h{document['hour']:02d}"][f"QH{document['quarter']:02d}"]   += document["demand_mwh_bc"]

        if (len(documents) > 0):
            documents = documents.values()

        update_fields_condition = [ "liquidation", "date", "segment", "type_demand" ]

        collection_name = collection_name.replace("_breakdown", "")

        Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

        # Transform time (quarterhourly to hourly or hourly to quarterhourly)
        time_fields = [ "cost_h", "demand_mwh_bc_h", "cost_eur_mwh_bc_h" ]

        ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)

    # = Losses =
    @staticmethod
    def process_files_losses(file_id, file_path, liquidation, collection_name, subsystem_name = "PENINSULA"):
        time = "hourly"

        subsystem_id = Helpers.get_subsystem_id_from_name(subsystem_name)

        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        tariff_id   = 0
        tariff_name = ""
        file_date   = ""

        if (len(file_name_split) > 1):
            tariff_name = file_name_split[1]
            tariff_name = tariff_name.replace("Sperd", "").replace("perd", "")
            tariff_name = tariff_name[:1] + "." + tariff_name[1:]
          
            tariff_id = Helpers.get_tariff_id_from_name(tariff_name)

            if (len(file_name_split) == 4):
                file_date = datetime.strptime(file_name_split[2], "%Y%m%d").strftime("%Y-%m")

            elif (len(file_name_split) == 5):
                file_date = datetime.strptime(file_name_split[3], "%Y%m%d").strftime("%Y-%m")

        if ((tariff_id > 0) and (file_date)):
            num_line = 0
            documents = []

            with open(file_path, "r") as file:
                for line in file:
                    if ((num_line > 1) and (len(line) > 3)):
                        line_split = line.split(";")

                        date_split = line_split[0].split(" ")

                        document = {
                            "file_id"      : file_id,
                            "liquidation"  : liquidation,
                            "tariff"       : tariff_name,
                            "tariff_id"    : tariff_id,
                            "date"         : file_date + "-" + date_split[1],
                            "subsystem"    : subsystem_name,
                            "subsystem_id" : subsystem_id,
                            "timestamp"    : datetime.utcnow()
                        }

                        if (len(line_split) <= ReePublicFiles.NUM_HOURLY_FIELDS):
                            for hour in range(1, 26):
                                if line_split[hour]:
                                    document[f"H{hour:02d}"] = float(line_split[hour])
                                else:
                                    document[f"H{hour:02d}"] = 0
                        else:
                            time = "quarterhourly"
                            
                            for hour in range(4, 101, 4):
                                document[f"H{hour // 4:02d}"] = {
                                    "QH01" : float(line_split[hour - 3]) if line_split[hour - 3] else 0,
                                    "QH02" : float(line_split[hour - 2]) if line_split[hour - 2] else 0,
                                    "QH03" : float(line_split[hour - 1]) if line_split[hour - 1] else 0,
                                    "QH04" : float(line_split[hour]) if line_split[hour] else 0,
                                }

                        documents.append(document)

                    num_line += 1

            update_fields_condition = [ "liquidation", "date", "tariff_id", "subsystem_id" ]

            if (time == "quarterhourly"):
                collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

            # Transform time (quarterhourly to hourly or hourly to quarterhourly)
            time_fields = [ "H" ]

            ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)

    @staticmethod
    def process_files_losses_subsystem(file_id, file_path, liquidation, collection_name):
        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        if (len(file_name_split) == 5):
            subsystem_name = file_name_split[2]

            ReePublicFiles.process_files_losses(file_id, file_path, liquidation, collection_name, subsystem_name)

    # = Periods =
    @staticmethod
    def process_files_periods(file_id, file_path, liquidation, collection_name, subsystem_name = "PENINSULA"):
        time = "hourly"

        subsystem_id = Helpers.get_subsystem_id_from_name(subsystem_name)

        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        period_name = ""
        file_date   = ""

        if (len(file_name_split) > 1):
            period_name = file_name_split[1]
            period_name = period_name.replace("Spetar", "").replace("petar", "")

            if (len(file_name_split) == 4):
                file_date = datetime.strptime(file_name_split[2], "%Y%m%d").strftime("%Y-%m")

            elif (len(file_name_split) == 5):
                file_date = datetime.strptime(file_name_split[3], "%Y%m%d").strftime("%Y-%m")

        if ((period_name) and (file_date)):
            documents = []

            list_tariffs = Helpers.get_list_tariffs_from_period(period_name)

            for tariff_name in list_tariffs:
                num_line = 0

                tariff_id = Helpers.get_tariff_id_from_name(tariff_name)

                with open(file_path, "r") as file:
                    for line in file:
                        if ((num_line > 1) and (len(line) > 3)):
                            line_split = line.split(";")

                            date_split = line_split[0].split(" ")

                            document = {
                                "file_id"      : file_id,
                                "liquidation"  : liquidation,
                                "tariff"       : tariff_name,
                                "tariff_id"    : tariff_id,
                                "date"         : file_date + "-" + date_split[1],
                                "subsystem"    : subsystem_name,
                                "subsystem_id" : subsystem_id,
                                "timestamp"    : datetime.utcnow()
                            }

                            if (len(line_split) <= ReePublicFiles.NUM_HOURLY_FIELDS):
                                for hour in range(1, 26):
                                    if line_split[hour]:
                                        document[f"H{hour:02d}"] = int(line_split[hour])
                                    else:
                                        document[f"H{hour:02d}"] = 0
                            else:
                                time = "quarterhourly"

                                for hour in range(4, 101, 4):
                                    document[f"H{hour // 4:02d}"] = {
                                        "QH01" : int(line_split[hour - 3]) if line_split[hour - 3] else 0,
                                        "QH02" : int(line_split[hour - 2]) if line_split[hour - 2] else 0,
                                        "QH03" : int(line_split[hour - 1]) if line_split[hour - 1] else 0,
                                        "QH04" : int(line_split[hour]) if line_split[hour] else 0,
                                    }

                            documents.append(document)

                        num_line += 1

            update_fields_condition = [ "liquidation", "date", "tariff_id", "subsystem_id" ]

            if (time == "quarterhourly"):
                collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)   

            # Transform time (quarterhourly to hourly or hourly to quarterhourly)
            time_fields = [ "H" ]

            ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)

    @staticmethod
    def process_files_periods_subsystem(file_id, file_path, liquidation, collection_name):
        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        if (len(file_name_split) == 5):
            subsystem_name = file_name_split[2]

            ReePublicFiles.process_files_periods(file_id, file_path, liquidation, collection_name, subsystem_name)

    # = Scdsvdem =
    @staticmethod
    def process_files_scdsvdem(file_id, file_path, liquidation, collection_name):
        time = "hourly"

        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        file_date = ""

        if (len(file_name_split) > 2):
            file_date = datetime.strptime(file_name_split[2], "%Y%m%d").strftime("%Y-%m")

        if file_date:
            num_line = 0
            documents = []

            with open(file_path, "r") as file:
                for line in file:
                    if ((num_line > 1) and (len(line) > 3)):
                        line_split = line.split(";")

                        date_split = line_split[0].split(" ")

                        document = {
                            "file_id"      : file_id,
                            "liquidation"  : liquidation,
                            "date"         : file_date + "-" + date_split[1],
                            "timestamp"    : datetime.utcnow()
                        }

                        if (len(line_split) <= ReePublicFiles.NUM_HOURLY_FIELDS):
                            for hour in range(1, 26):
                                if line_split[hour]:
                                    document[f"H{hour:02d}"] = float(line_split[hour])
                                else:
                                    document[f"H{hour:02d}"] = 0
                        else:
                            time = "quarterhourly"

                            for hour in range(4, 101, 4):
                                document[f"H{hour // 4:02d}"] = {
                                    "QH01" : float(line_split[hour - 3]) if line_split[hour - 3] else 0,
                                    "QH02" : float(line_split[hour - 2]) if line_split[hour - 2] else 0,
                                    "QH03" : float(line_split[hour - 1]) if line_split[hour - 1] else 0,
                                    "QH04" : float(line_split[hour]) if line_split[hour] else 0,
                                }

                        documents.append(document)

                    num_line += 1

            update_fields_condition = [ "liquidation", "date" ]

            if (time == "quarterhourly"):
                collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

            # Transform time (quarterhourly to hourly or hourly to quarterhourly)
            time_fields = [ "H" ]

            ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)

    # = SphdemDD =
    @staticmethod
    def process_files_sphdem_dd(file_id, file_path, liquidation, collection_name):
        time = "hourly"

        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        subsystem_id   = 0
        subsystem_name = ""
        file_date      = ""

        if (len(file_name_split) > 2):
            subsystem_name = file_name_split[2]

            subsystem_id = Helpers.get_subsystem_id_from_name(subsystem_name)

            if (len(file_name_split) > 3):
                file_date = datetime.strptime(file_name_split[3], "%Y%m%d").strftime("%Y-%m")

        if ((subsystem_id > 0) and (file_date)):
            num_line = 0
            documents = []

            with open(file_path, "r") as file:
                for line in file:
                    if ((num_line > 1) and (len(line) > 3)):
                        line_split = line.split(";")

                        date_split = line_split[0].split(" ")

                        document = {
                            "file_id"      : file_id,
                            "liquidation"  : liquidation,
                            "date"         : file_date + "-" + date_split[1],
                            "subsystem"    : subsystem_name,
                            "subsystem_id" : subsystem_id,
                            "timestamp"    : datetime.utcnow()
                        }

                        if (len(line_split) <= ReePublicFiles.NUM_HOURLY_FIELDS):
                            for hour in range(1, 26):
                                if line_split[hour]:
                                    document[f"H{hour:02d}"] = float(line_split[hour])
                                else:
                                    document[f"H{hour:02d}"] = 0
                        else:
                            time = "quarterhourly"

                            for hour in range(4, 101, 4):
                                document[f"H{hour // 4:02d}"] = {
                                    "QH01" : float(line_split[hour - 3]) if line_split[hour - 3] else 0,
                                    "QH02" : float(line_split[hour - 2]) if line_split[hour - 2] else 0,
                                    "QH03" : float(line_split[hour - 1]) if line_split[hour - 1] else 0,
                                    "QH04" : float(line_split[hour]) if line_split[hour] else 0,
                                }

                        documents.append(document)

                    num_line += 1

            update_fields_condition = [ "liquidation", "date", "subsystem_id" ]

            if (time == "quarterhourly"):
                collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

            # Transform time (quarterhourly to hourly or hourly to quarterhourly)
            time_fields = [ "H" ]

            ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)

     # = SphvenDD =
    @staticmethod
    def process_files_sphven_dd(file_id, file_path, liquidation, collection_name):
        time = "hourly"

        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        subsystem_id   = 0
        subsystem_name = ""
        file_date      = ""

        if (len(file_name_split) > 2):
            subsystem_name = file_name_split[2]

            subsystem_id = Helpers.get_subsystem_id_from_name(subsystem_name)

            if (len(file_name_split) > 3):
                file_date = datetime.strptime(file_name_split[3], "%Y%m%d").strftime("%Y-%m")

        if ((subsystem_id > 0) and (file_date)):
            num_line = 0
            documents = []

            with open(file_path, "r") as file:
                for line in file:
                    if ((num_line > 1) and (len(line) > 3)):
                        line_split = line.split(";")

                        date_split = line_split[0].split(" ")

                        document = {
                            "file_id"      : file_id,
                            "liquidation"  : liquidation,
                            "date"         : file_date + "-" + date_split[1],
                            "subsystem"    : subsystem_name,
                            "subsystem_id" : subsystem_id,
                            "timestamp"    : datetime.utcnow()
                        }

                        if (len(line_split) <= ReePublicFiles.NUM_HOURLY_FIELDS):
                            for hour in range(1, 26):
                                if line_split[hour]:
                                    document[f"H{hour:02d}"] = float(line_split[hour])
                                else:
                                    document[f"H{hour:02d}"] = 0
                        else:
                            time = "quarterhourly"

                            for hour in range(4, 101, 4):
                                document[f"H{hour // 4:02d}"] = {
                                    "QH01" : float(line_split[hour - 3]) if line_split[hour - 3] else 0,
                                    "QH02" : float(line_split[hour - 2]) if line_split[hour - 2] else 0,
                                    "QH03" : float(line_split[hour - 1]) if line_split[hour - 1] else 0,
                                    "QH04" : float(line_split[hour]) if line_split[hour] else 0,
                                }

                        documents.append(document)

                    num_line += 1

            update_fields_condition = [ "liquidation", "date", "subsystem_id" ]

            if (time == "quarterhourly"):
                collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

            # Transform time (quarterhourly to hourly or hourly to quarterhourly)
            time_fields = [ "H" ]

            ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)

    # = Sprpcap =
    @staticmethod
    def process_files_sprpcap(file_id, file_path, liquidation, collection_name):
        time = "hourly"

        file_name = os.path.basename(file_path)

        file_name_split = file_name.split("_")

        tariff_id      = 0
        tariff_name    = ""
        subsystem_id   = 0
        subsystem_name = ""
        file_date      = ""

        if (len(file_name_split) > 1):
            tariff_name = file_name_split[1]
            tariff_name = tariff_name.replace("Sprpcap", "")
            tariff_name = tariff_name[:1] + "." + tariff_name[1:]

            tariff_id = Helpers.get_tariff_id_from_name(tariff_name)

            if (len(file_name_split) > 2):
                subsystem_name = file_name_split[2]

                subsystem_id = Helpers.get_subsystem_id_from_name(subsystem_name)

                if (len(file_name_split) > 3):
                    file_date = datetime.strptime(file_name_split[3], "%Y%m%d").strftime("%Y-%m")

        if ((tariff_id > 0) and (subsystem_id > 0) and (file_date)):
            num_line = 0
            documents = []

            with open(file_path, "r") as file:
                for line in file:
                    if ((num_line > 1) and (len(line) > 3)):
                        line_split = line.split(";")

                        date_split = line_split[0].split(" ")

                        document = {
                            "file_id"      : file_id,
                            "liquidation"  : liquidation,
                            "tariff"       : tariff_name,
                            "tariff_id"    : tariff_id,
                            "date"         : file_date + "-" + date_split[1],
                            "subsystem"    : subsystem_name,
                            "subsystem_id" : subsystem_id,
                            "timestamp"    : datetime.utcnow()
                        }

                        if (len(line_split) <= ReePublicFiles.NUM_HOURLY_FIELDS):
                            for hour in range(1, 26):
                                if line_split[hour]:
                                    document[f"H{hour:02d}"] = float(line_split[hour])
                                else:
                                    document[f"H{hour:02d}"] = 0
                        else:
                            time = "quarterhourly"

                            for hour in range(4, 101, 4):
                                document[f"H{hour // 4:02d}"] = {
                                    "QH01" : float(line_split[hour - 3]) if line_split[hour - 3] else 0,
                                    "QH02" : float(line_split[hour - 2]) if line_split[hour - 2] else 0,
                                    "QH03" : float(line_split[hour - 1]) if line_split[hour - 1] else 0,
                                    "QH04" : float(line_split[hour]) if line_split[hour] else 0,
                                }

                        documents.append(document)

                    num_line += 1

            update_fields_condition = [ "liquidation", "date", "tariff_id", "subsystem_id" ]

            if (time == "quarterhourly"):
                collection_name += "_" + ReePublicFiles.COLLECTION_QUARTERHOURLY_SUFFIX

            Helpers.bulk_update(ReePublicMongo.db_connection(), collection_name, documents, update_fields_condition)

            # Transform time (quarterhourly to hourly or hourly to quarterhourly)
            time_fields = [ "H" ]

            ReePublicFiles.transform_time_documents(collection_name, documents, time, time_fields, update_fields_condition)