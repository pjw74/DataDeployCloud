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
