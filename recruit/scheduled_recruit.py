import requests
import json
import csv
import env
from alarm import *
from load import *
from transform import preprocessing
from datetime import datetime
from google.cloud import storage
        

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
    if int(data_json['jobs']['total']) <= 110:
        page = 0
        last_page = int(data_json['jobs']['total'])
    else:
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


def run():
    url = env.API_CONFIG['url']
    params = {'access-key' : env.API_CONFIG['key'], 'ind_cd' : '3', 'fields': 'posting-date,expiration-date,keyword-code,count', 'count':110, 'start':0}
    dir = env.PATH['DIR']
    with open(dir+'time.txt', 'r') as f:
        before = f.readlines()[-1]
    
    ts = datetime.now()
    now = ts.strftime('%Y-%m-%d %H:%M:%S')
    filenow = ts.strftime('%Y-%m-%d__%H-%M-%S')
    
    params['published_min'] = before
    params['published_max'] = now
    
    try:
        pages, last_length = set_init(url, params, f"{dir}job_postings/recruit_info{filenow}")
    except requests.exceptions.RequestException:
        print("api 접근에 실패했습니다.")
        send_to_slack(0, 0, 0, env.WEB_HOOK['slack'], 2)
        return

    if pages == 0 and last_length == 0:
        print("새로 업로드된 채용공고가 없습니다!")
        send_to_slack(before, now, 0, env.WEB_HOOK['slack'], 1)
        return
    
    saves = 0
    if pages == 0:
        saves = last_length
        print("페이지 :", 0, "저장 데이터 수 :", saves)
    else:
        saves = 110
        print("페이지 :", 0, "저장 데이터 수 :", saves)

    for i in range(1, pages+1):
        if i == pages:
            params['count'] = last_length
        params['start'] = i
        
        try:
            res = requests.get(url, params=params)
        except requests.exceptions.RequestException:
            print("api 접근에 실패했습니다.")
            send_to_slack(0, 0, 0, env.WEB_HOOK['slack'], 2)
            return

        data_json = json.loads(res.text)
        data = data_json['jobs']['job']
        saves += len(data)
        
        if not add_json(f"{dir}job_postings/recruit_info{filenow}.json", data):
            send_to_slack(0, 0, f"{dir}job_postings/recruit_info{filenow}.json", env.WEB_HOOK['slack'], 4)
            return
        if not add_csv(f"{dir}job_postings/recruit_info{filenow}.csv", data):
            send_to_slack(0, 0, f"{dir}job_postings/recruit_info{filenow}.csv", env.WEB_HOOK['slack'], 4)
            return
        print("페이지 :", i, "저장 데이터 수 :", saves)
    
    if upload_to_gcs(f"recruit_info{filenow}.json") != 0 or upload_to_gcs(f"recruit_info{filenow}.csv") != 0:
        send_to_slack(0, 0, 0, env.WEB_HOOK['slack'], 4)
        return
    
    send_to_slack(before, now, saves, env.WEB_HOOK['slack'], 1)
    
    with open(dir+'time.txt', 'a') as f:
        f.write("\n" + now)
        

if __name__ == '__main__':
    run()
