import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from fastapi import status, HTTPException

load_dotenv()


class OutagesOfSitesService(object):

    def __init__(self):
        super(OutagesOfSitesService, self).__init__()

    def download_data_from_api(self, url, file_name):
        headers = {'x-api-key': os.getenv('API_KEY'),
                   "Content-Type": "application/json"}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                outages = response.json()
                self.write_to_file(outages, file_name)
                return
            else:
                raise ValueError(f'Request failed with status code {response.status_code}')
        except requests.exceptions.RequestException as e:
            print(f'Request failed ({e}). Retrying in {retry_delay} seconds...')
            raise ValueError('Request failed after maximum number of retries')

    def write_to_file(self, data, file_name):
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=4))

    def read_file_with_pandas(self, file_path):
        return pd.read_json(file_path)

    def read_site_id_json_request(self, site_id_df):
        try:
            devices = site_id_df['devices']
        except KeyError as e:
            raise ValueError(f"Invalid site_id_df: {e}") from e

        return pd.DataFrame(devices, columns=['id', 'name'])

    def read_outages_response(self, outages_json_list):
        try:
            result = pd.DataFrame.from_dict(outages_json_list)
        except ValueError as e:
            raise ValueError(f"Invalid outages_json_list: {e}") from e

        try:
            result_json = json.dumps(result.to_dict(orient='list'))
        except ValueError as e:
            raise RuntimeError(f"Failed to convert result to JSON: {e}") from e

        return result_json

    def filter_outages_by_timestamp(self, outages_df, last_timestamp):
        if not isinstance(outages_df, pd.DataFrame):
            raise TypeError("outages_df must be a pandas DataFrame")
        try:
            filtered_outages = outages_df[outages_df['begin'] > last_timestamp]
        except KeyError as e:
            raise ValueError(f"Invalid outages_df: {e}") from e

        return filtered_outages

    def filter_site_id_by_not_null(self, site_df):
        return site_df[site_df['id'].notnull()]

    def concat_dataframes(self, cleaned_outages_df, cleaned_site_ids_df):
        if not isinstance(cleaned_outages_df, pd.DataFrame):
            raise TypeError("cleaned_outages_df must be a pandas DataFrame")
        if not isinstance(cleaned_site_ids_df, pd.DataFrame):
            raise TypeError("cleaned_site_ids_df must be a pandas DataFrame")

        try:
            merged_df = pd.merge(cleaned_outages_df, cleaned_site_ids_df)
        except ValueError as e:
            raise ValueError(f"Failed to merge dataframes: {e}") from e

        return merged_df

    def transformation_site_outages(self, outages_DF, site_id_DF, last_timestamp):
        outages_df = outages_DF.copy()
        site_id_df = pd.json_normalize(site_id_DF['devices'])

        try:
            filtered_outages = self.filter_outages_by_timestamp(outages_df, last_timestamp)
            extracted_site_id_df = self.filter_site_id_by_not_null(site_id_df)
            result_list = self.concat_dataframes(filtered_outages, extracted_site_id_df)
        except Exception as e:
            raise ValueError("Failed to transform data: {}".format(e))
        result_list.to_csv('result.csv', index=False)
        return {"status": status.HTTP_200_OK}

    def check_file_exists(self, file_path):
        if os.path.isfile(file_path):
            print('File exists')
        else:
            raise ValueError("file not exist")

    def send_outages(self):
        # Read the content of the JSON file
        list_of_payload  = pd.read_csv("result.csv").to_dict(orient="records")
        headers = {'x-api-key': os.getenv('API_KEY'),
                   "Content-Type": "application/json"}
        BASE_URL = os.getenv("BASE_URL", "https://api.krakenflex.systems/interview-tests-mock-api/v1/")
        response = requests.post(BASE_URL + 'site-outages/norwich-pear-tree', headers=headers, json=list_of_payload)
        return {"status": status.HTTP_200_OK}


 #  FOR PURE TESTING PURPOSE UNCOMMENT THOSE LINES AND JUST RUN 🙃
"""
if __name__ == '__main__':
    BASE_URL = os.getenv("BASE_URL", "https://api.krakenflex.systems/interview-tests-mock-api/v1/")
    service = OutagesOfSitesService()
    outages = service.download_data_from_api(BASE_URL + "outages", "outages.json")
    site_ids = service.download_data_from_api(BASE_URL + "site-info/norwich-pear-tree", "site_ids.json")
    outages_df = service.read_file_with_pandas(os.getcwd() + "/" + "outages.json")
    site_id_df = service.read_file_with_pandas(os.getcwd() + "/" + "site_ids.json")
    last_timestamp = "2022-01-01T00:00:00.000Z"
    result_list = service.transformation_site_outages(outages_df, site_id_df, last_timestamp)
    service.send_outages()
"""