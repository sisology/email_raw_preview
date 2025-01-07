import pandas as pd
from bs4 import BeautifulSoup
import re
import requests

# Elasticsearch 서버 정보
url = "http://110.234.28.58:9200/air.france1@atlanticif.com/_search?pretty=true"

# 요청 헤더와 데이터 설정
headers = {"Content-Type": "application/json"}
data = {
    "query": {
        "bool": {
            "must": [
                {
                    "match": {
                        "no_slip_progress": "PARAEO20221100692"
                    }
                }
            ],
            "filter": [
                {
                    "range": {
                        "tm_rcv": {
                            "lt": "20241212000000"
                        }
                    }
                }
            ]
        }
    },
    "size": 30,
    "sort": [
        {
            "tm_rcv": {
                "order": "asc"
            }
        }
    ]
}

# Elasticsearch에서 데이터 가져오기
response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    results = response.json()
    hits = results.get("hits", {}).get("hits", [])
else:
    print(f"Error: {response.status_code}")
    hits = []

# HTML 파일로 원본 데이터 저장
with open("email_raw_preview.html", "w", encoding="utf-8") as file:
    for hit in hits:
        source = hit.get("_source", {})
        html_body = source.get("dc_body", "")  # 원본 HTML 본문
        file.write(html_body + "\n\n")

print("HTML 파일이 'email_raw_preview.html'로 저장되었습니다.")

# HTML 내용에서 텍스트 추출 및 대화 분리
def clean_and_split_html(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    text = re.sub(r'\s+', ' ', text).strip()  # 다중 줄바꿈 정리

    # 이메일 대화 구분 패턴 (발신/수신 기준으로 분리)
    conversation_pattern = r"(From:.*?)(?=(From:|$))"  # 'From:' 키워드로 각 대화 분리
    conversations = re.findall(conversation_pattern, text, re.DOTALL)

    # 각 대화를 리스트로 정리
    parsed_conversations = [conv[0].strip() for conv in conversations]
    return parsed_conversations

# 실제 데이터를 가공
processed_data = []
for hit in hits:
    source = hit.get("_source", {})
    html_body = source.get("dc_body", "")  # 이메일 본문
    cleaned_body = clean_and_split_html(html_body)  # HTML 본문 정리 (리스트 반환)

    processed_data.append({
        "subject": source.get("dc_subject", ""),  # 제목
        "content": cleaned_body,  # 본문 텍스트 (리스트 형태)
        "cd_classify": source.get("cd_classify", ""),  # 분류
        "tm_rcv": source.get("tm_rcv", ""),  # 수신 시간
        "sender": source.get("dc_from", ""),  # 발신자
        "receiver": ", ".join(source.get("dc_to", [])) if isinstance(source.get("dc_to"), list) else source.get("dc_to", ""),  # 수신자
        "cc": ", ".join(source.get("dc_cc", [])) if isinstance(source.get("dc_cc"), list) else source.get("dc_cc", ""),  # 참조
        "no_id_sender": source.get("no_id_sender", "")  # 발신자 ID
    })

# 터미널에 데이터 출력
for idx, data in enumerate(processed_data, start=1):
    print(f"--- Email {idx} ---")
    print(f"Subject     : {data['subject']}")
    print(f"Classification: {data['cd_classify']}")
    print(f"Received Time: {data['tm_rcv']}")
    print(f"Sender      : {data['sender']}")
    print(f"Receiver    : {data['receiver']}")
    print(f"CC          : {data['cc']}")
    print(f"Sender ID   : {data['no_id_sender']}")
    print("\n✅✅✅Content (Conversations):\n")
    
    # 본문을 대화별로 출력
    conversations = data['content']  # 이미 리스트 형태
    for i, conversation in enumerate(conversations, start=1):
        print(f"--- Conversation {i} ---\n{conversation}\n")

    print("\n" + "-"*50 + "\n")  # 이메일 간 구분선

with open("email_preview.html", "w", encoding="utf-8") as file:
    file.write(html_body)

print("HTML 파일이 'email_preview.html'로 저장되었습니다.")