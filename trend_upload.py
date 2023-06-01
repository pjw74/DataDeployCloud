# ETL script
from io import StringIO
from pytrends.request import TrendReq
import pandas as pd
import datetime
from google.cloud import storage
import time

credentials_path = '/home/rsa-key-20230529/Github/job-posting-api-388303-0fa2808b9dcb.json'
# 인증 정보 설정
client = storage.Client.from_service_account_json(credentials_path)

# 전체 결과를 만들기 위한 DataFrame 리스트
df_list = []
# 검색어 리스트를 바꾼 횟수
cnt = 0

def merge_and_upload_data(df_list):

    """ 전체 결과를 만들기 위한 함수 """

    # DataFrame 리스트를 합치기
    merged_df = pd.concat(df_list, axis=1)

    # 결과 CSV 파일을 저장하기 위한 Cloud Storage 정보
    project_id = 'jop-hosting-api'
    bucket_name = 'hale-posting-bucket'
    ts = datetime.datetime.now()
    destination_blob_name = 'google_trend/result_%s.csv' % ts

    # Cloud Storage 클라이언트 및 버킷 생성
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # 결과 파일을 CSV식으로 저장 (Cloud Storage 버킷에 업로드)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(merged_df.to_csv(index=False), content_type='text/csv')
    print("google_trend uploaded to {}.".format(destination_blob_name))

def collect_data():

    """ 데이터 수집 함수 """

    pytrends = TrendReq(hl='ko-KR', tz=540)
    global cnt
    kw_list = ['정보처리', '인공지능', '빅데이터', '백준', '프로그래머스'] if cnt == 0 else ['부트캠프', '데이터베이스', '데이터 마이닝', '데이터 엔지니어', '클러스터링']
    pytrends.build_payload(kw_list, timeframe='today 5-y', geo='KR')
    df = pytrends.interest_over_time()
    df.reset_index(inplace=True)
    df.drop(columns=['isPartial'], inplace=True)
    if cnt > 0:
        df.drop(columns=['date'], inplace=True) # 두 번째 데이터 수집 시에는 'date' 열을 삭제

    # 결과 리스트에 DataFrame 추가
    df_list.append(df)
    # 검색어 리스트를 바꾼 횟수 증가
    cnt += 1

if __name__ == "__main__":
    # 두번의 데이터 수집 후 결과 저장
    collect_data() # 첫 번째 데이터 수집
    time.sleep(10) # 10초 대기(pytrends 제한사항)
    collect_data() # 두 번째 데이터 수집
    merge_and_upload_data(df_list) # 수집한 데이터를 합치고 저장
