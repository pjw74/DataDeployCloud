import requests
import json

def send_to_slack(before, after, saves, webhook):
    # 슬랙 웹훅 URL을 설정합니다.
    webhook_url = webhook

    # 보낼 메시지 내용을 설정합니다.
    if saves == 0:
        message = "새로 업로드된 채용공고가 없습니다."
    else:
        message = f"{before}부터 {after}까지 총 {saves}개 데이터가 적재되었습니다!"
    title = "(테스트)채용공고 스케줄러 알림 :hyperfastparrot:"

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
    data = json.dumps(payload)

    # POST 요청을 보냅니다.
    response = requests.post(webhook_url, data=data, headers={"Content-Type": "application/json"})

    # 응답을 확인합니다.
    if response.status_code == 200:
        print("메시지가 성공적으로 전송되었습니다.")
    else:
        print("메시지 전송에 실패했습니다. 응답 코드:", response.status_code)
