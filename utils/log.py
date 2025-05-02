from datetime import datetime

import colorlog
import logging
import os

class Log:
    NOTICE = 25

    @staticmethod
    def get_logger(name: str):
        logger = None

        fmt = os.getenv("LOG_FORMAT", "%(asctime)s %(log_color)s%(name)s[%(levelname)s]: %(message)s")
        level = os.getenv("LOG_LEVEL", "INFO")
        file_folder_path = os.getenv("LOG_FILE_FOLDER_PATH")

        logging.addLevelName(Log.NOTICE, "NOTICE")
        
        formatter = colorlog.TTYColoredFormatter(
            fmt,
            datefmt = "%Y-%m-%d %H:%M:%S",
            reset = True,
            log_colors = {
                "NOTICE"   : "blue",
                "DEBUG"    : "green",
                "INFO"     : "white",
                "WARNING"  : "yellow",
                "ERROR"    : "red",
                "CRITICAL" : "red,bg_white",
            },
            secondary_log_colors = {},
            style = '%'
        )

        handler = colorlog.StreamHandler()
        handler.setFormatter(formatter)

        logger = colorlog.getLogger(name)
        logger.addHandler(handler)
        
        if file_folder_path:
            logger = Log.add_file_handler(logger, file_folder_path, os.getenv("LOG_FILE_ADD_TIMESTAMP"), level)
            
        logger.setLevel(level)

        return logger
           
    @staticmethod 
    def add_file_handler(logger: logging.Logger, file_path: str, timestamp: bool, level: str):
        
        if timestamp:
            log_file = f"{file_path}/{logger.name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
        else:
            log_file = f"{file_path}/{logger.name}.log"

        try:
            file_handler = logging.FileHandler(log_file, "w")
            
            formatter = logging.Formatter(
                fmt = "%(asctime)s %(name)s[%(levelname)s]: %(message)s",
                datefmt = "%Y-%m-%d %H:%M:%S"
            )

            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)

            logger.addHandler(file_handler)

            logger.info(f"Saving log to {file_path}")

        except FileNotFoundError as fnfe:
            logger.warning(f"Can't save log at {file_path}, this execution will have console output only: {fnfe.strerror}")
            
        return logger