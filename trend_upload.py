# ETL script
from pytrends.request import TrendReq
import pandas as pd
import datetime
from google.cloud import storage

credentials_path = '/home/rsa-key-20230529/Github/job-posting-api-388303-8ec7283e13da.json'
# 인증 정보 설정
client = storage.Client.from_service_account_json(credentials_path)
def collect_data():
    pytrends = TrendReq(hl='ko-KR', tz=540)
    kw_list = ['정보처리','인공지능','빅데이터','백준','프로그래머스']
    pytrends.build_payload(kw_list, timeframe='today 5-y', geo='KR')
    df = pytrends.interest_over_time()
    df.reset_index(inplace=True)
    df.drop(columns=['isPartial'], inplace=True)
    df.to_csv('result.csv', index=False)
def upload_data():
    project_id = 'jop-hosting-api'
    bucket_name = 'hale-posting-bucket'
    source_file_name = 'result.csv'
    ts = datetime.datetime.now()
    destination_blob_name = 'google_trend/result_%s.csv' % ts
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))
if __name__ == "__main__":
    collect_data()
    upload_data()