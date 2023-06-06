from io import StringIO
from pytrends.request import TrendReq
import pandas as pd
import datetime
from google.cloud import storage
import time
from google.auth.exceptions import RefreshError
from google.cloud.exceptions import NotFound
import requests
import json

# 인증 정보 설정
credentials_path = '/home/rsa-key-20230529/Github/job-posting-api-388303-0fa2808b9dcb.json'
client = storage.Client.from_service_account_json(credentials_path)

# 전체 결과를 만들기 위한 DataFrame 리스트
df_list = []

cnt = 0

def send_slack_notification(message, num_data):
    """ Slack Incoming WebHooks를 사용하여 알림을 보내는 함수 """
    webhook_url = 'https://hooks.slack.com/services/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'
    slack_data = {'text': "{} ({}개 키워드 군)".format(message, num_data)}
    response = requests.post(webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise ValueError('Slack message sending failed: %s' % response.text)


def build_payload_and_retry(pytrends, keyword):
    """ PyTrends 객체를 초기화하고 요청에 대해 stable한 응답을 받을 때까지 retry """
    MAX_TRIES = 5
    delay = 10
    for i in range(MAX_TRIES):
        try:
            pytrends.build_payload(keyword, timeframe='today 5-y', geo='KR')
            time.sleep(delay) # 안정적인 응답을 위한 대기 시간 추가
            break
        except Exception as e:
            print(f"Failed to build payload, retrying in {delay} seconds, {MAX_TRIES - i - 1} tries left")
            time.sleep(delay)
    if i == MAX_TRIES - 1:
        raise ValueError("Failed to build payload after {MAX_TRIES} tries")

def collect_data():
    """ 데이터 수집 함수 """
    global cnt

    keyword_groups = [
        ['정보처리', '인공지능', '빅데이터', '백준'],
        ['부트캠프', '데이터베이스', '데이터 마이닝', '클러스터링'],
        ['데이터 엔지니어', '데이터 분석가', '데이터 사이언티스트']
    ]

    pytrends = TrendReq(hl='ko-KR', tz=540)

    for keywords in keyword_groups:
        try:
            build_payload_and_retry(pytrends, keywords)
            df = pytrends.interest_over_time()
            df.reset_index(inplace=True)
            df.drop(columns=['isPartial'], inplace=True)
            if cnt > 0:
                df.drop(columns=['date'], inplace=True)
            df_list.append(df)
            cnt += 1
        except Exception as e:
            print(f"An error occurred while collecting data for {keywords}")
            print(str(e))
            continue

def upload_file_to_gcs(file_path, bucket_name, destination_blob_name):
    """ 지정한 경로의 파일을 GCS 버킷에 업로드 """
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
    except NotFound:
        print(f"Bucket {bucket_name} not found")
        return
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_filename(file_path)
    except RefreshError:
        print("Google auth access token expired, retrying")
        upload_file_to_gcs(file_path, bucket_name, destination_blob_name)
    except Exception as e:
        print(f"An error occurred while uploading {file_path}")
        print(str(e))

def merge_and_upload_data(df_list, project_id, bucket_name):
    """ 전체 결과를 만들기 위한 함수 """

    try:
        # DataFrame 리스트를 합치기
        merged_df = pd.concat(df_list, axis=1)
        # 결과 CSV 파일을 저장하기 위한 Cloud Storage 정보
        ts = datetime.datetime.now()
        destination_blob_name = 'google_trend/result_{}.csv'.format(ts)
        # 결과 파일을 CSV식으로 저장 (Cloud Storage 버킷에 업로드)
        merged_df.to_csv(destination_blob_name, index=False)
        upload_file_to_gcs(destination_blob_name, bucket_name, destination_blob_name)
        print(f"DataFrame uploaded to {destination_blob_name} in {bucket_name}")

        # Slack에 알림 메시지 전송
        num_data = len(df_list)
        send_slack_notification("Google Trend 데이터 수집이 완료되었습니다.", num_data)

    except Exception as e:
        print(f"An error occurred while uploading to {bucket_name}")
        print(str(e))

if __name__ == "__main__":

    collect_data() # 세 번 데이터 수집
    bucket_name ='hale-posting-bucket'
    merge_and_upload_data(df_list, project_id='jop-hosting-api', bucket_name=bucket_name) # 수집한 데이터를 합치고 저장
