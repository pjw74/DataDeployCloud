import requests
import json
import csv
import env
from alarm import *
from datetime import datetime
from google.cloud import storage
from transform import preprocessing


def add_csv(filename, jobs):
    """
        현재 가진 job데이터를 전처리 후 csv에 입력
    """
    lst = []
    for ele in jobs:
        lst.append(preprocessing(ele))
    with open(filename, 'a', newline='', encoding='UTF-8-sig') as f:
        if lst:
            writer = csv.DictWriter(f, fieldnames=lst[0].keys())
            for data in lst:
                writer.writerow(data)


def add_json(filename, data):
    """
        현재 가진 job데이터를 json에 입력
    """
    with open(filename, 'r', encoding='UTF-8-sig') as f:
        k = json.load(f)
    lst = [] + k + data
    with open(filename, 'w', encoding='UTF-8-sig') as f:
        json.dump(lst, f, indent=2, ensure_ascii=False)


def upload_to_gcs(filename):
    credentials = env.GCS['CREDENTIAL_PATH']
    dir = env.PATH['DIR']

    # GCS 클라이언트 생성
    client = storage.Client.from_service_account_json(credentials)

    # 버킷 선택
    bucket = client.get_bucket(env.GCS['BUCKET_NAME'])

    # 로컬 파일 업로드
    blob = bucket.blob("recruit/"+filename)
    blob.upload_from_filename(dir+"job_postings/"+filename)

    print(filename + "업로드 완료!")


