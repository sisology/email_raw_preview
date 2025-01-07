from bs4 import BeautifulSoup

html_body = """
<p class="cs2654AE3A"><span>De :</span><span>John Doe</span></p>
<p class="cs2654AE3A"><span>Envoyé :</span><span>2023-01-07</span></p>
<p class="cs2654AE3A"><span>À :</span><span>Jane Doe</span></p>
<p class="cs2654AE3A"><span>Cc :</span><span>cc@example.com</span></p>
<p class="cs2654AE3A"><span>Objet :</span><span>Test Email</span></p>
"""

# BeautifulSoup 객체 생성
soup = BeautifulSoup(html_body, "html.parser")

# 디버깅을 위한 출력
print("1. soup 객체 생성 확인:", soup.prettify())

# <p> 태그 찾기
p_tags = soup.find_all("p", class_="cs2654AE3A")
print("\n2. 찾은 p 태그 수:", len(p_tags))

# 메타데이터를 저장할 딕셔너리
metadata = {}

# 키워드 리스트 정의
keywords = ["De", "Envoyé", "À", "Cc", "Objet", "From", "Sent", "To", "Subject"]

# <p> 태그를 순회하면서 키-값 추출
for p_tag in p_tags:
    spans = p_tag.find_all("span")
    print("\n3. 현재 p 태그의 span 수:", len(spans))
    if len(spans) >= 2:
        key = spans[0].get_text(strip=True).strip(":")
        value = spans[1].get_text(strip=True)
        print(f"4. 추출된 키-값: {key} - {value}")
        if key in keywords:
            metadata[key] = value

# 결과 출력
print("\n--- Extracted Metadata ---")
for key, value in metadata.items():
    print(f"{key}: {value}")