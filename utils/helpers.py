from pymongo import UpdateOne

class Helpers:

    @staticmethod
    def bulk_update(db_connection, collection_name, documents, update_fields_condition):
        operations = []

        for document in documents:
            condition = {field: document[field] for field in update_fields_condition}
            operation = {"$set": document}

            operations.append(UpdateOne(condition, operation, upsert=True))

        if operations:
            collection = db_connection[collection_name]

            collection.bulk_write(operations)

    @staticmethod
    def get_subsystem_id_from_name(subsystem_name):
        subsystem_id = 0

        if (subsystem_name == "MELILLA"):
            subsystem_id = 1
        elif (subsystem_name == "CEUTA"):
            subsystem_id = 2
        elif (subsystem_name == "CANARIAS"):
            subsystem_id = 3
        elif (subsystem_name == "BALEARES"):
            subsystem_id = 4

        return subsystem_id

    @staticmethod
    def get_tariff_id_from_name(tariff_name):
        tariff_id = 0

        if ((tariff_name == "20TD") or (tariff_name == "2.0TD")):
            tariff_id = 29
        elif ((tariff_name == "30TD") or (tariff_name == "3.0TD")):
            tariff_id = 30
        elif ((tariff_name == "61TD") or (tariff_name == "6.1TD")):
            tariff_id = 31
        elif ((tariff_name == "62TD") or (tariff_name == "6.2TD")):
            tariff_id = 32
        elif ((tariff_name == "63TD") or (tariff_name == "6.3TD")):
            tariff_id = 33
        elif ((tariff_name == "64TD") or (tariff_name == "6.4TD")):
            tariff_id = 34
        elif ((tariff_name == "30TDVE") or (tariff_name == "3.0TDVE")):
            tariff_id = 41
        elif ((tariff_name == "61TDVE") or (tariff_name == "6.1TDVE")):
            tariff_id = 42
            
        return tariff_id

    @staticmethod
    def get_list_tariffs_from_period(period_name):
        list_tariffs = []

        if (period_name == "3P"):
            list_tariffs = [ "2.0TD" ]
        elif (period_name == "6P"):
            list_tariffs = [ "3.0TD", "6.1TD", "6.2TD", "6.3TD", "6.4TD", "3.0TDVE", "6.1TDVE" ]

        return list_tariffs

    @staticmethod
    def transform_hourly_to_quarterhourly(documents, hourly_fields):
        for document in documents:
            for hourly_field in hourly_fields:
                for hour in range(1, 26):
                    document[f"{hourly_field}{hour:02d}"] = {
                        "QH01": document[f"{hourly_field}{hour:02d}"],
                        "QH02": document[f"{hourly_field}{hour:02d}"],
                        "QH03": document[f"{hourly_field}{hour:02d}"],
                        "QH04": document[f"{hourly_field}{hour:02d}"]
                    }

        return documents

    @staticmethod
    def transform_quarterhourly_to_hourly(documents, quarterhourly_fields):
        for document in documents:
            for quarterhourly_field in quarterhourly_fields:
                for hour in range(1, 26):
                    hourly_avg = round(((document[f"{quarterhourly_field}{hour:02d}"]["QH01"] + 
                                         document[f"{quarterhourly_field}{hour:02d}"]["QH02"] + 
                                         document[f"{quarterhourly_field}{hour:02d}"]["QH03"] + 
                                         document[f"{quarterhourly_field}{hour:02d}"]["QH04"]) / 4), 10)

                    document[f"{quarterhourly_field}{hour:02d}"] = hourly_avg

        return documents