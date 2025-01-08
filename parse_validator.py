import pandas as pd
from bs4 import BeautifulSoup
import re
import requests
from order_view import clean_and_split_html

def analyze_parsing_results(html_content, parsed_data):
    """파싱 결과 분석"""
    analysis = {
        'original_length': len(html_content),
        'parsed_length': sum(len(str(conv)) for conv in parsed_data['content']),
        'conversation_count': len(parsed_data['content']),
        'metadata_fields': {},
        'missing_fields': []
    }
    
    # 메타데이터 필드 존재 여부 확인
    expected_fields = ['subject', 'sender', 'receiver', 'sent_date']
    for field in expected_fields:
        value = parsed_data.get(field, '')
        analysis['metadata_fields'][field] = bool(value)
        if not value:
            analysis['missing_fields'].append(field)
    
    # 원본 HTML에서 주요 패턴 검색
    soup = BeautifulSoup(html_content, 'lxml')
    text = soup.get_text()
    
    # 이메일 주소 패턴 검색
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    original_emails = set(re.findall(email_pattern, text))
    parsed_emails = set(re.findall(email_pattern, str(parsed_data)))
    
    analysis['email_addresses'] = {
        'original_count': len(original_emails),
        'parsed_count': len(parsed_emails),
        'missing_emails': original_emails - parsed_emails
    }
    
    return analysis

def print_analysis(analysis):
    """분석 결과 출력"""
    print("\n=== 파싱 결과 분석 ===")
    print(f"\n1. 텍스트 길이 비교:")
    print(f"  - 원본 길이: {analysis['original_length']} 문자")
    print(f"  - 파싱된 길이: {analysis['parsed_length']} 문자")
    print(f"  - 보존율: {(analysis['parsed_length']/analysis['original_length']*100):.1f}%")
    
    print(f"\n2. 대화 분리:")
    print(f"  - 분리된 대화 수: {analysis['conversation_count']}")
    
    print("\n3. 메타데이터 필드 존재 여부:")
    for field, exists in analysis['metadata_fields'].items():
        print(f"  - {field}: {'✓' if exists else '✗'}")
    
    if analysis['missing_fields']:
        print("\n4. 누락된 필드:")
        for field in analysis['missing_fields']:
            print(f"  - {field}")
    
    print("\n5. 이메일 주소 분석:")
    print(f"  - 원본 이메일 주소 수: {analysis['email_addresses']['original_count']}")
    print(f"  - 파싱된 이메일 주소 수: {analysis['email_addresses']['parsed_count']}")
    if analysis['email_addresses']['missing_emails']:
        print("\n누락된 이메일 주소:")
        for email in analysis['email_addresses']['missing_emails']:
            print(f"  - {email}")

# Elasticsearch 서버 정보
url = "http://110.234.28.58:9200/air.france1@atlanticif.com/_search?pretty=true"

# 요청 헤더와 데이터 설정
headers = {"Content-Type": "application/json"}
data = {
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "no_slip_progress.keyword": "PARAEO20221100692"
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
    "size": 300,
    "sort": [
        {"tm_rcv": {"order": "asc"}}
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

# 파싱 검증 실행
for idx, hit in enumerate(hits, 1):
    print(f"\n{'='*50}")
    print(f"Email {idx} 분석")
    print(f"{'='*50}")
    
    source = hit.get("_source", {})
    html_body = source.get("dc_body", "")
    
    # 파싱된 데이터 생성
    parsed_data = {
        'subject': source.get("dc_subject", ""),
        'sender': source.get("dc_from", ""),
        'receiver': source.get("dc_to", ""),
        'sent_date': source.get("tm_rcv", ""),
        'content': clean_and_split_html(html_body)
    }
    
    # 분석 실행
    analysis = analyze_parsing_results(html_body, parsed_data)
    print_analysis(analysis)