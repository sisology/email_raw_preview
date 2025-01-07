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
    "size": 20,
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

# # HTML 파일로 원본 데이터 저장
# with open("email_raw_preview.html", "w", encoding="utf-8") as file:
#     for hit in hits:
#         source = hit.get("_source", {})
#         html_body = source.get("dc_body", "")
#         file.write(html_body + "\n\n")

# print("HTML 파일이 'email_raw_preview.html'로 저장되었습니다.")

# 메타데이터 추출 함수
def extract_metadata(html):
    soup = BeautifulSoup(html, "html.parser")
    metadata = {}
    
    # 키워드 매핑 정의 (다국어 -> 영어)
    keywords = {
        # English
        "From :": "From",
        "Sent :": "Sent",
        "To :": "To",
        "Cc :": "Cc",
        "Subject :": "Subject",
        # French
        "De :": "From",
        "Envoyé :": "Sent",
        "À :": "To",
        "Cc :": "Cc",
        "Objet :": "Subject",
        # 특수문자가 없는 버전도 추가
        "From": "From",
        "Sent": "Sent",
        "To": "To",
        "Cc": "Cc",
        "Subject": "Subject",
        "De": "From",
        "Envoyé": "Sent",
        "À": "To",
        "Objet": "Subject"
    }
    
    # cs2654AE3A 클래스를 가진 p 태그 찾기
    p_tags = soup.find_all("p", class_="cs2654AE3A")
    
    for p_tag in p_tags:
        # p 태그 내의 텍스트를 직접 확인
        text = p_tag.get_text(strip=True)

        # 메타데이터 키워드 찾기
        for pattern, key in keywords.items():
            if text.startswith(pattern):
                # 키워드 이후의 텍스트를 값으로 저장
                value = text[len(pattern):].strip()
                # 값이 있는 경우에만 저장 (빈 문자열이 아닌 경우)
                if value and key not in metadata:
                    metadata[key] = value
                break
    
    return metadata

# 데이터 처리
processed_data = []
for hit in hits:
    source = hit.get("_source", {})
    html_body = source.get("dc_body", "")
    
    metadata = extract_metadata(html_body)
    processed_data.append(metadata)

# 결과 출력
for idx, data in enumerate(processed_data, start=1):
    print(f"\n--- Email {idx} ---")
    for key in ["From", "Sent", "To", "Cc", "Subject"]:
        value = data.get(key, "")
        print(f"✅{key}: {value}")
    print("=" * 50)

# HTML 내용에서 텍스트 추출 및 대화 분리
def clean_and_split_html(html):
    soup = BeautifulSoup(html, "html.parser")
    
    # cs2654AE3A 클래스를 가진 p 태그들 찾기
    p_tags = soup.find_all("p", class_="cs2654AE3A")
    
    current_conversation = []
    conversations = []
    
    for p_tag in p_tags:
        text = p_tag.get_text(strip=True)
        
        # 새로운 대화의 시작을 나타내는 패턴
        if text.startswith(("From:", "From :", "De:", "De :")):
            if current_conversation:  # 이전 대화가 있으면 저장
                conversations.append("\n".join(current_conversation))
                current_conversation = []
        
        # 빈 줄이 아닌 경우만 추가
        if text:
            current_conversation.append(text)
    
    # 마지막 대화 추가
    if current_conversation:
        conversations.append("\n".join(current_conversation))
    
    return conversations

# 실제 데이터를 가공
processed_data = []
for hit in hits:
    source = hit.get("_source", {})
    html_body = source.get("dc_body", "")
    
    # HTML에서 메타데이터 추출
    email_metadata = extract_metadata(html_body)
    
    # 기존 cleaned_body 처리
    cleaned_body = clean_and_split_html(html_body)
    
    processed_data.append({
        "subject": source.get("dc_subject", ""),
        "extracted_subject": email_metadata.get("Subject", ""),
        "content": cleaned_body,
        "cd_classify": source.get("cd_classify", ""),
        "tm_rcv": source.get("tm_rcv", ""),
        "sender": source.get("dc_from", ""),
        "extracted_sender": email_metadata.get("From", ""),
        "receiver": ", ".join(source.get("dc_to", [])) if isinstance(source.get("dc_to"), list) else source.get("dc_to", ""),
        "extracted_receiver": email_metadata.get("To", ""),
        "cc": ", ".join(source.get("dc_cc", [])) if isinstance(source.get("dc_cc"), list) else source.get("dc_cc", ""),
        "extracted_cc": email_metadata.get("Cc", ""),
        "sent_date": email_metadata.get("Sent", ""),
        "no_id_sender": source.get("no_id_sender", "")
    })

# 터미널에 데이터 출력
for idx, data in enumerate(processed_data, start=1):
    print(f"\n{'='*50}")
    print(f"Email {idx}")
    print(f"{'='*50}")
    
    # 메타데이터 출력
    fields = [
        ("Subject", "subject", "extracted_subject"),
        ("Classification", "cd_classify", None),
        ("Received Time", "tm_rcv", None),
        ("Sent Date", None, "sent_date"),
        ("From", "sender", "extracted_sender"),
        ("To", "receiver", "extracted_receiver"),
        ("Cc", "cc", "extracted_cc"),
        ("Sender ID", "no_id_sender", None)
    ]
    
    for label, es_field, html_field in fields:
        es_value = data.get(es_field, "") if es_field else ""
        html_value = data.get(html_field, "") if html_field else ""
        
        if html_value:
            print(f"{label}:")
            print(f"  ES  : {es_value}")
            print(f"  HTML: {html_value}")
        else:
            print(f"{label}: {es_value}")
    
    print("\nContent:")
    for i, conversation in enumerate(data['content'], start=1):
        print(f"\nConversation {i}:")
        print("-" * 30)
        print(conversation.strip())

# 마지막 HTML 파일 저장
with open("email_preview.html", "w", encoding="utf-8") as file:
    file.write(html_body)

print("HTML 파일이 'email_preview.html'로 저장되었습니다.")