from dotenv import load_dotenv

import os
import requests

load_dotenv()

class ReePublicApi:

    @staticmethod
    def call(start_date, end_date):
        call_results = []

        url = f"{os.getenv('REE_PUBLIC_API_PROTOCOL')}://{os.getenv('REE_PUBLIC_API_HOSTNAME')}/{os.getenv('REE_PUBLIC_API_ENDPOINT')}?date_type={os.getenv('REE_PUBLIC_API_DATE_TYPE')}&start_date={start_date}&end_date={end_date}&locale={os.getenv('REE_PUBLIC_API_LOCALE')}"

        api_response = requests.get(url, headers={"x-api-key": os.getenv("REE_PUBLIC_API_TOKEN")})

        if api_response.status_code == 200:
            call_results = api_response.json()

        return call_results