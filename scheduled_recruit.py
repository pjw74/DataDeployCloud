import requests
import json
import csv
import env
from datetime import datetime
from google.cloud import storage


def preprocessing(d):
    """
        API로부터 추출한 데이터에 대해 전처리 수행
    """
    data_refactored = {}

    data_refactored['position.title'] = d['position']['title']
    data_refactored['position.industry'] = d['position']['industry'].get('code', '999')
    data_refactored['position.location'] = d['position']['location'].get('code', '999')
    data_refactored['position.job-type'] = d['position']['job-type'].get('code', '999')
    data_refactored['position.job-mid-code'] = d['position']['job-mid-code'].get('code', '999')
    data_refactored['position.industry-keyword-code'] = d['position'].get('industry-keyword-code', '999').replace('|', ',')
    data_refactored['position.job-code-keyword-code'] = d['position'].get('job-code-keyword-code', '999')
    data_refactored['position.experience-level-code'] = d['position']['experience-level'].get('code', '999')
    data_refactored['position.experience-level-min'] = d['position']['experience-level']['min']
    data_refactored['position.experience-level-max'] = d['position']['experience-level']['max']
    data_refactored['position.required-education-level'] = d['position']['required-education-level'].get('code', '999')
    data_refactored['keyword'] = d.get('keyword', '')
    data_refactored['salary'] = d['salary'].get('code', '999')
    data_refactored['posting-timestamp'] = d['posting-timestamp']
    data_refactored['posting-date'] = d['posting-date']
    data_refactored['expiration-timestamp'] = d['expiration-timestamp']
    data_refactored['expiration-date'] = d['expiration-date']
    data_refactored['read-cnt'] = d['read-cnt']
    data_refactored['apply-cnt'] = d['apply-cnt']
    return data_refactored


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
        

def set_init(url, params, filename):
    """
        최초 설정 함수
        기본 패러미터에 대해 api 호출을 시행
        호출 데이터 파일 적재 후 총 페이지 수, 마지막 페이지 데이터 수 반환
    """
    print('호출시작!')
    # API request
    res = requests.get(url, params = params)
    data_json = json.loads(res.text)
    data = data_json['jobs']['job']
    
    if data_json['jobs']['total'] == '0':
        return 0, 0
    # calc page, last page
    page = int(data_json['jobs']['total']) // int(data_json['jobs']['count'])
    last_page = int(data_json['jobs']['total']) % int(data_json['jobs']['count'])
    
    # save json
    with open(filename+'.json', 'w', encoding='UTF-8-sig') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        
    # save CSV
    lst = []
    for job in data:
        lst.append(preprocessing(job))
    with open(filename+'.csv', 'w', newline='', encoding='UTF-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=lst[0].keys())
        writer.writeheader()
        for job in lst:
            writer.writerow(job)
    
    return page, last_page

def upload_to_gcs(filename):
    credentials = service_account.Credentials.from_service_account_file(env.GCS['CREDENTIAL_PATH'])

    # GCS 클라이언트 생성
    client = storage.Client(credentials=credentials)

    # 버킷 선택
    bucket = client.get_bucket(env.GCS['BUCKET_NAME'])

    # 로컬 파일 업로드
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)
    
    print(filename + "업로드 완료!")


def run():
    url = env.API_CONFIG['url']
    params = {'access-key' : env.API_CONFIG['key'], 'ind_cd' : '3', 'fields': 'posting-date,expiration-date,keyword-code,count', 'count':110, 'start':0}
    
    with open('time.txt', 'r') as f:
        before = f.readlines()[-1]
    
    ts = datetime.now()
    now = ts.strftime('%Y-%m-%d %H:%M:%S')
    filenow = ts.strftime('%Y-%m-%d__%H-%M-%S')
    
    params['published_min'] = before
    params['published_max'] = now
    
    pages, last_length = set_init(url, params, f"recruit/recruit_info{filenow}")
    
    if pages == 0 and last_length == 0:
        print("새로 업로드된 채용공고가 없습니다!")
        return
    
    for i in range(1, pages+1):
        if i == pages:
            params['count'] = last_length
        params['start'] = i
        
        res = requests.get(url, params=params)
        data_json = json.loads(res.text)
        data = data_json['jobs']['job']
        
        add_json(f"recruit/recruit_info{filenow}.json", data)
        add_csv(f"recruit/recruit_info{filenow}.csv", data)
        print("페이지 :", i, "저장 데이터 수 :", i * 110)
        
    upload_to_gcs(f"recruit/recruit_info{filenow}.json")
    upload_to_gcs(f"recruit/recruit_info{filenow}.csv")
    
    with open('time.txt', 'a') as f:
        f.write("\n" + now)
        

if __name__ == '__main__':
    run()