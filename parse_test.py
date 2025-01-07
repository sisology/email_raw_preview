from bs4 import BeautifulSoup
import re

def parse_email_chain(html):
    soup = BeautifulSoup(html, "html.parser")

    # 대화 분리: -----Original Message----- 기준으로 분리
    email_chain = re.split(r'-----Original Message-----', soup.get_text(), flags=re.IGNORECASE)
    
    parsed_emails = []
    
    for email in email_chain:
        # 메타데이터 추출: From, To, Sent, Subject
        metadata_match = re.search(
            r'(From:.*?)(To:.*?)(Cc:.*?|)(Sent:.*?)(Subject:.*?)', 
            email, 
            flags=re.DOTALL
        )
        
        if metadata_match:
            from_info = metadata_match.group(1).strip()
            to_info = metadata_match.group(2).strip()
            cc_info = metadata_match.group(3).strip() if metadata_match.group(3) else ""
            sent_info = metadata_match.group(4).strip()
            subject_info = metadata_match.group(5).strip()

            # 본문 내용
            body = email[metadata_match.end():].strip()

            # 이메일 저장
            parsed_emails.append({
                "from": from_info,
                "to": to_info,
                "cc": cc_info,
                "sent": sent_info,
                "subject": subject_info,
                "body": body,
            })

    return parsed_emails


# 테스트 HTML 데이터 (html_body 변수로 대체)
html_body = """
<html>
    <body>
        <p>From: Patricia <patricia@example.com><br>To: Yena <yena@example.com><br>Cc: <br>Sent: 2022. 6. 28.<br>Subject: Example Subject</p>
        <p>안녕하세요,<br>이메일 본문 내용입니다.</p>
        -----Original Message-----
        <p>From: John Doe <john@example.com><br>To: Patricia <patricia@example.com><br>Cc: Yena <yena@example.com><br>Sent: 2022. 6. 27.<br>Subject: Previous Subject</p>
        <p>이전 이메일 내용입니다.</p>
    </body>
</html>
"""

# 이메일 체인 파싱
parsed_chain = parse_email_chain(html_body)

# 결과 출력
for idx, email in enumerate(parsed_chain, start=1):
    print(f"--- Email {idx} ---")
    print(f"{email['from']}")
    print(f"{email['to']}")
    print(f"{email['cc']}")
    print(f"{email['sent']}")
    print(f"{email['subject']}")
    print(f"Body: {email['body']}")
    print("\n")
