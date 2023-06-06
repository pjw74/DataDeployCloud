import requests
import json
import csv
import env
from alarm import *
from datetime import datetime
from google.cloud import storage
from transform import preprocessing
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import Forbidden, BadRequest


def add_csv(filename, jobs):
    """
        현재 가진 job데이터를 전처리 후 csv에 입력
    """
    lst = []
    for ele in jobs:
        lst.append(preprocessing(ele))
    try:
        with open(filename, 'a', newline='', encoding='UTF-8-sig') as f:
            if lst:
                writer = csv.DictWriter(f, fieldnames=lst[0].keys())
                for data in lst:
                    writer.writerow(data)
    except FileNotFoundError:
        print(f"파일 탐색 실패: {filename}")
        return 0
    return 1


def add_json(filename, data):
    """
        현재 가진 job데이터를 json에 입력
    """
    try:
        with open(filename, 'r', encoding='UTF-8-sig') as f:
            k = json.load(f)
    except FileNotFoundError:
        print(f"파일 탐색 실패: {filename}")
        return 0
    lst = [] + k + data
    with open(filename, 'w', encoding='UTF-8-sig') as f:
        json.dump(lst, f, indent=2, ensure_ascii=False)
    return 1


def upload_to_gcs(filename):
    credentials = env.GCS['CREDENTIAL_PATH']
    dir = env.PATH['DIR']

    # GCS 클라이언트 생성
    try:
        client = storage.Client.from_service_account_json(credentials)
        # 버킷 선택
        bucket = client.get_bucket(env.GCS['BUCKET_NAME'])
        # 로컬 파일 업로드
        blob = bucket.blob("recruit/"+filename)
    except(FileNotFoundError):
        print('인증 파일이 없습니다.')
        return 4, credentials
    except(ValueError):
        print('저장 파일이 잘못되었습니다.')
        return 4, credentials
    except DefaultCredentialsError as e:
        print("인증 키에 문제가 있습니다.")
        print(e)
        return 3
    except NotFound:
        print(f"{env.GCS['BUCKET_NAME']}버킷을 찾을 수 없습니다.")
        return 3
    except (Forbidden, BadRequest):
        print('접근이 금지되었거나 요청이 잘못되었습니다.')
        return 3

    blob.upload_from_filename(dir+"job_postings/"+filename)

    print(filename + "업로드 완료!")
    return 0
