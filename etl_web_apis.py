from fastapi import FastAPI, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import pandas as pd
import json
import os
import uvicorn
from OutagesOfSitesService import OutagesOfSitesService
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
service = OutagesOfSitesService()

BASE_URL = os.getenv("BASE_URL", "https://api.krakenflex.systems/interview-tests-mock-api/v1/")
@app.get("/")
def read_root():
    return {"Hello": "Kraken"}


@app.post("/download_data")
async def download_data_from_api(url_path: str, file_name: str):
    try:
        service.download_data_from_api(BASE_URL + url_path, file_name)
        return {"message": f"Data downloaded from {url_path} and saved as {file_name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/transform_data")
async def transform_data_from_file(outages_file_name: str, site_id_file_name: str,
                                   last_timestamp_string: Optional[str] = None):
    try:
        service.check_file_exists(os.getcwd() +"/" + outages_file_name)
        service.check_file_exists(os.getcwd() +"/" + site_id_file_name)
        outages_df = service.read_file_with_pandas(os.getcwd() + "/" + outages_file_name)
        site_id_df = service.read_file_with_pandas(os.getcwd() + "/" + site_id_file_name)
        last_timestamp = last_timestamp_string if last_timestamp_string else "2022-01-01T00:00:00.000Z"
        result_list = service.transformation_site_outages(outages_df, site_id_df, last_timestamp)
        return JSONResponse(content=result_list, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete_files")
async def delete_files():
    # Iterate over all files in the directory
    dir_path = os.path.dirname(os.path.realpath(__file__)) +"/"
    for filename in os.listdir(dir_path):
        if filename.endswith('.json'):
            # Delete the file
            os.remove(os.path.join(dir_path, filename))


@app.post("/send_outaged_site_ids")
async def send_outages_site_ids():
    service.send_outages()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
