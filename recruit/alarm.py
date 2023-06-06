import json
import requests

def send_to_slack(before, after, saves, webhook, code):
    # 슬랙 웹훅 URL을 설정합니다.
    webhook_url = webhook

    # 보낼 메시지 내용을 설정합니다.
    # 연결 성공 시
    if code == 1:
        if saves == 0:
            message = "새로 업로드된 채용공고가 없습니다."
        else:
            message = f"{before}부터 {after}까지 총 {saves}개 데이터가 적재되었습니다!"
        title = "(테스트)채용공고 스케줄러 알림 :hyperfastparrot:"
    else:
        # api request error
        if code == 2:
            title = "api 호출 에러"
            message = "api 호출에 실패했습니다."
        # gcs upload error
        elif code == 3:
            title = "GCS 업로드 에러"
            message = "GCS 연결에 실패했습니다."
        # file save error
        elif code == 4:
            title = "파일 실행 실패"
            message = f"{saves}파일 실행에 실패했습니다."

    data = make_send_format(title, message)

    # POST 요청을 보냅니다.
    response = requests.post(webhook_url, data=data, headers={"Content-Type": "application/json"})

    # 응답을 확인합니다.
    if response.status_code == 200:
        print("메시지가 성공적으로 전송되었습니다.")
    else:
        print("메시지 전송에 실패했습니다. 응답 코드:", response.status_code)


def make_send_format(title, message):
    # 메시지를 보낼 때 필요한 데이터를 생성합니다.
    payload = {
        "attachments": [
            {
                "color": "#9733EE",
                "icon_emoji": ":satellite:",
                'fields':[
                    {
                        "title": title,
                        "value": message
                    }
                ]
            }
        ]
    }

    # JSON 형식으로 데이터를 변환합니다.
    return json.dumps(payload)
