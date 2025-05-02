from ree.ree_public_files import ReePublicFiles
from omie.omie_public_files import OmiePublicFiles

from datetime import datetime
from dateutil.relativedelta import relativedelta

def main():
    current_date = datetime.now()
    current_hour = current_date.hour

    ree_public_execute_hours = [ 8, 21 ]

    if (current_hour in ree_public_execute_hours):
        print(f"\r\n======================================== [ REE ] ========================================")
 
        # Create collection indexes
        ReePublicFiles.create_collections_indexes()

        # Set start_date the first day of the previous 12 months and end_date the last day after 3 months
        start_date = (current_date.replace(day = 1) - relativedelta(months = 12)).strftime("%Y-%m-%d")
        end_date   = (current_date.replace(day = 1) + relativedelta(months = 4, days = -1)).strftime("%Y-%m-%d")
        
        # REE download and process files
        files   = ReePublicFiles.download_files(start_date, end_date)
        folders = ReePublicFiles.extract_files(files)
        ReePublicFiles.process_folders(folders)

    omie_public_execute_hours = [ 8, 21 ]

    if (current_hour in omie_public_execute_hours):
        print(f"\r\n======================================= [ OMIE ] ========================================")

        # Create collection indexes
        OmiePublicFiles.create_collections_indexes()

        # Set start_date the first day of the previous 7 days and end_date is tomorrow
        start_date = (current_date - relativedelta(days = 7)).strftime("%Y-%m-%d")
        end_date   = (current_date + relativedelta(days = 1)).strftime("%Y-%m-%d")

        # OMIE download and process files
        files = OmiePublicFiles.download_files(start_date, end_date)
        OmiePublicFiles.process_files(files)

if __name__ == "__main__":
    main()