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

# HTML 파일로 원본 데이터 저장
with open("email_raw_preview.html", "w", encoding="utf-8") as file:
    for hit in hits:
        source = hit.get("_source", {})
        html_body = source.get("dc_body", "")
        file.write(html_body + "\n\n")

print("HTML 파일이 'email_raw_preview.html'로 저장되었습니다.")

# def analyze_html_structure(html):
#     soup = BeautifulSoup(html, "lxml")
    
#     print("\n=== HTML 구조 분석 ===")
    
#     print("\n1. HTML 전체 구조:")
#     for tag in soup.find_all(['div', 'p', 'span']):
#         print(f"\n태그: {tag.name}")
#         print(f"속성들: {tag.attrs}")
#         if tag.string:
#             print(f"텍스트: {tag.string[:100]}")  # 처음 100자만 출력
    
#     print("\n2. From이 포함된 부분:")
#     for text in soup.find_all(text=re.compile('From:', re.IGNORECASE)):
#         print("\n---")
#         print(f"찾은 텍스트: {text}")
#         print(f"태그 종류: {text.parent.name}")
#         print(f"태그 속성: {text.parent.attrs}")

# if hits:
#     for idx, hit in enumerate(hits[:2]):  # 처음 2개의 이메일만 분석
#         print(f"\n{'='*50}")
#         print(f"Email {idx+1} 분석")
#         print(f"{'='*50}")
#         source = hit.get("_source", {})
#         html_body = source.get("dc_body", "")
#         analyze_html_structure(html_body)

# # 테스트를 위해 하나의 이메일만 분석
# if hits:
#     source = hits[0].get("_source", {})
#     html_body = source.get("dc_body", "")
#     analyze_html_structure(html_body)

# 메타데이터 추출 함수
def extract_metadata(html):
    soup = BeautifulSoup(html, "lxml")
    metadata = {}
    
    # 헤더 매핑 (영어/프랑스어)
    header_mapping = {
        'From': ['From:', 'De :'],
        'Sent': ['Sent:', 'Envoyé :'],
        'To': ['To:', 'À :'],
        'Subject': ['Subject:', 'Objet :'],
        'Cc': ['Cc:', 'Copie :']
    }

    # 1. border-top이 있는 div 찾기 (스타일 속성의 다양한 변형을 고려)
    email_headers = soup.find('div', style=lambda x: x and ('border-top' in x or 'border-top:solid' in x))
    if email_headers:
        # 텍스트 추출을 위한 모든 span 태그 찾기
        spans = email_headers.find_all(['span', 'b'])
        
        current_key = None
        current_value = []
        
        for span in spans:
            text = span.get_text(strip=True)
            if not text:
                continue
                
            # 헤더 키 확인
            is_header = False
            for key, patterns in header_mapping.items():
                if any(text.startswith(pattern) for pattern in patterns):
                    if current_key and current_value:
                        metadata[current_key] = ' '.join(current_value).strip()
                    current_key = key
                    current_value = []
                    is_header = True
                    break
            
            # 값 추가
            if not is_header and current_key:
                current_value.append(text)

        # 마지막 키-값 쌍 처리
        if current_key and current_value:
            metadata[current_key] = ' '.join(current_value).strip()

    return metadata

def clean_and_split_html(html):
    soup = BeautifulSoup(html, "lxml")

    # 1. 이미지 관련 태그 제거
    for img in soup.find_all(['img', 'figure', 'picture', 'svg']):
        img.decompose()
    
    # 2. 표 관련 태그 제거
    for table in soup.find_all(['table', 'tbody', 'thead', 'tr', 'td', 'th']):
        table.decompose()

    visual_content_classes = [
        'table',
        'grid',
        'image',
        'img',
        'picture',
        'photo',
        'gallery',
        'thumbnail'
    ]
    
    for class_name in visual_content_classes:
        for element in soup.find_all(class_=lambda x: x and class_name.lower() in x.lower()):
            element.decompose()
        for element in soup.find_all(id=lambda x: x and class_name.lower() in x.lower()):
            element.decompose()
    
    # 불필요한 요소 제거
    removable_elements = [
        'style',           # CSS 스타일
        'script',          # 자바스크립트
        'meta',           # 메타 태그
        'link',           # 외부 리소스 링크
        'img',            # 이미지
        'table',          # 표
        # 'form',           # 폼
        # 'button',         # 버튼
        # 'input',          # 입력 필드
        # 'select',         # 선택 박스
        # 'textarea'        # 텍스트 영역
    ]
    
    for element in removable_elements:
        for tag in soup.find_all(element):
            tag.decompose()

    # 공지사항, 법적 고지 등 일반적인 안내문구 제거
    common_notices = [
        "This email and any files transmitted with it are confidential",
        "CONFIDENTIALITY NOTICE",
        "This email message and any attachments are for the sole use of",
        "The information contained in this email",
        "본 메일은 정보통신망",
        "본 메일에는",
        "위 메일에 포함된 내용은",
        "선진에서는 대내외 모든 미팅에서 마스크를 꼭 착용하고 있습니다 . 대화에 조금 어려움이 있으시더라도 고객 분들의 양해 부탁말씀 드립니다",
        "This message contains confidential information",
        "The contents of this email message and any attachments",
        "This communication is for its intended recipient only",
        "Best regards",
        "Thank you",
        "Thanks"
    ]
    
    text_blocks = soup.stripped_strings
    clean_blocks = []
    
    for block in text_blocks:
        # 일반적인 안내문구 제외
        if not any(notice.lower() in block.lower() for notice in common_notices):
            # 구분선으로 사용되는 반복 문자 제거
            if not re.match(r'^[=\-_*]{3,}$', block):
                # 빈 줄이나 특수문자만으로 이루어진 줄 제거
                if block.strip() and not re.match(r'^[\s\W]+$', block):
                    clean_blocks.append(block)

    # 이메일 구분자 패턴 (더 포괄적으로)
    separator_patterns = [
        r"(?:From|De)\s*:?\s*.+?@.+?\..+",  # 이메일 주소를 포함하는 From/De
        r"(?:Sent|Envoyé)\s*:?\s*\d",       # 날짜/시간으로 시작하는 Sent/Envoyé
        r"_{5,}|={5,}|-{5,}",               # 구분선
        r"Original Message",                 # 원본 메시지 표시
        r"Message d'origine"                 # 프랑스어 원본 메시지 표시
    ]
    
    # HTML에서 텍스트 추출
    text = " ".join(soup.stripped_strings)
    
    # 대화 분리
    conversations = []
    current_pos = 0
    
    for pattern in separator_patterns:
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        for match in matches:
            if match.start() > current_pos:
                conversation = text[current_pos:match.start()].strip()
                if conversation:
                    conversations.append(conversation)
            current_pos = match.start()
    
    # 마지막 부분 추가
    if current_pos < len(text):
        conversation = text[current_pos:].strip()
        if conversation:
            conversations.append(conversation)
    
    # 빈 대화 제거 및 중복 제거
    conversations = [conv for conv in conversations if conv.strip()]
    conversations = list(dict.fromkeys(conversations))
    
    return conversations

# 데이터 처리
processed_data = []
for hit in hits:
    source = hit.get("_source", {})
    html_body = source.get("dc_body", "")
    
    # HTML에서 메타데이터 추출
    email_metadata = extract_metadata(html_body)
    
    # HTML에서 대화 내용 분리
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

# 정제된 이메일 내용을 HTML 파일로 저장
if hits:
    with open("email_preview.html", "w", encoding="utf-8") as file:
        # HTML 기본 구조와 스타일 추가
        file.write("""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            background-color: #f0f0f0;
            padding: 20px;
        }
        .email {
            border: 1px solid #ccc;
            margin: 20px 0;
            padding: 20px;
            background-color: #fff;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metadata {
            background-color: #f8f9fa;
            padding: 15px;
            margin-bottom: 15px;
            border-left: 4px solid #007bff;
            border-radius: 4px;
        }
        .metadata p {
            margin: 5px 0;
            color: #333;
        }
        .metadata-label {
            font-weight: bold;
            color: #555;
            width: 80px;
            display: inline-block;
        }
        .content {
            padding: 10px;
            background-color: #fff;
            line-height: 1.5;
        }
        .conversation {
            margin: 10px 0;
            padding: 10px;
            border-left: 3px solid #28a745;
            background-color: #f8f9fa;
        }
        .email-divider {
            border: none;
            border-top: 2px dashed #ccc;
            margin: 30px 0;
        }
    </style>
</head>
<body>
""")
        
        # 각 이메일 처리
        for idx, hit in enumerate(hits, 1):
            source = hit.get("_source", {})
            html_body = source.get("dc_body", "")
            
            # 메타데이터 추출
            metadata = extract_metadata(html_body)
            cleaned_conversations = clean_and_split_html(html_body)
            
            # 이메일 컨테이너 시작
            file.write(f'<div class="email">')
            
            # 메타데이터 섹션
            file.write('<div class="metadata">')
            
            # 기본 메타데이터 필드
            metadata_fields = [
                ('Subject', metadata.get('Subject', source.get("dc_subject", ""))),
                ('From', metadata.get('From', source.get("dc_from", ""))),
                ('To', metadata.get('To', source.get("dc_to", ""))),
                ('Cc', metadata.get('Cc', source.get("dc_cc", ""))),
                ('Sent', metadata.get('Sent', source.get("tm_rcv", "")))
            ]
            
            for label, value in metadata_fields:
                if value:  # 값이 있는 경우만 표시
                    file.write(f'<p><span class="metadata-label">{label}:</span> {value}</p>')
            
            file.write('</div>')  # metadata div 종료
            
            # 정제된 대화 내용
            file.write('<div class="content">')
            for conversation in cleaned_conversations:
                file.write(f'<div class="conversation">')
                # HTML 이스케이프 처리
                escaped_content = conversation.replace('<', '&lt;').replace('>', '&gt;')
                file.write(f'<p>{escaped_content}</p>')
                file.write('</div>')
            file.write('</div>')  # content div 종료
            
            file.write('</div>')  # email div 종료
            
            # 이메일 구분선 (마지막 이메일 제외)
            if idx < len(hits):
                file.write('<hr class="email-divider">')
        
        # HTML 문서 닫기
        file.write("""
</body>
</html>
""")

print("정제된 이메일 내용이 'email_preview.html'에 저장되었습니다.")