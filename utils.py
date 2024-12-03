import os
import re
import json
import pickle
import tempfile
from functools import wraps

import yaml
import requests
import pandas as pd
from bs4 import BeautifulSoup


ROOT = "./"
YAML_DIR = "urls"

def temp_pickle(func):
    """
    함수 결과를 임시 파일에 피클링하는 데코레이터.
    캐시가 있으면 로드하고, 최종적으로 캐시 파일을 삭제.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 임시 파일 이름 생성
        temp_dir = tempfile.gettempdir()
        file_name = f"{func.__name__}_{hash(args)}_{hash(frozenset(kwargs.items()))}.pkl"
        file_path = os.path.join(temp_dir, file_name)

        # 이미 캐시 파일이 존재하면 이를 로드
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                return pickle.load(f)

        # 파일이 없으면 함수 실행 후 결과를 피클링
        result = func(*args, **kwargs)
        with open(file_path, 'wb') as f:
            pickle.dump(result, f)

        # 최종적으로 캐시 파일 삭제
        os.remove(file_path)

        return result

    return wrapper


def get_full_html(url: str):
    # HTTP GET 요청
    response = requests.get(url)
    response.raise_for_status()  # 요청 실패 시 예외 발생

    # HTML 파싱
    return BeautifulSoup(response.text, 'html.parser')


def extract_title_url(soup):
    title_url_mapping = {}
    
    # rfp-bid-title 클래스에서만 a 태그 찾기
    title_divs = soup.find_all('div', class_='rfp-bid-title')
    for title_div in title_divs:
        a_tag = title_div.find('a')  # a 태그 찾기
        if a_tag:
            title = a_tag.get_text(strip=True).replace("'", "").replace(" ", "_").replace("_–_", "-")  # 텍스트 (제목) 추출
            url = a_tag.get('href')  # 'href' 속성 (URL) 추출
            if title and url:  # 제목과 URL이 모두 존재할 경우 추가
                title_url_mapping[title] = url

    return title_url_mapping


def extract_data(soup):
    # 페이지 제목
    title_tag = soup.find("h1", class_="page-header")
    title = title_tag.get_text(strip=True) if title_tag else "Title not found"

    # As Notice에서 "estimated"와 숫자 추출
    estimated_numbers = []
    notice_section = soup.find("div", id="rfp-ad-notice")
    if notice_section:
        sentences = notice_section.get_text(strip=True).split(".")
        for sentence in sentences:
            if "estimated" in sentence.lower():
                numbers = re.findall(r"\b\d[\d,]*\b", sentence)
                estimated_numbers.append((sentence.strip(), numbers))

    # Bid Results에서 Company와 Bid Amount 추출
    bid_results = []
    bid_table = soup.find("table", id="bidresultlist")
    if bid_table:
        rows = bid_table.find_all("tr")[1:]  # Skip header row
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                company = cols[1].get_text(strip=True)
                bid_amount = cols[2].get_text(strip=True)
                bid_results.append({"company": company, "bid_amount": bid_amount})

    # Awards에서 Firm Name과 Award Amt 추출
    awards = []
    awards_table = soup.find("table", id="awardlist")
    if awards_table:
        rows = awards_table.find_all("tr")[1:]  # Skip header row
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                firm_name = cols[1].get_text(strip=True)
                award_amt = cols[2].get_text(strip=True)
                awards.append({"firm_name": firm_name, "award_amt": award_amt})

    return {
        "title": title,
        "estimated_numbers": estimated_numbers,
        "bid_results": bid_results,
        "awards": awards
    }


@temp_pickle
def extract_multiple_to_df(data_list, iscsv=False):
    """
    여러 딕셔너리를 받아 pandas DataFrame으로 변환하고 CSV로 저장.

    Args:
        data_list (List[Dict]): 각 데이터를 담은 딕셔너리 리스트.
        output_file (str): 저장할 CSV 파일 경로.
    """
    # 각 딕셔너리를 처리하여 DataFrame으로 변환
    processed_data = []
    for data in data_list:
        # 모든 값을 문자열로 변환
        if iscsv:
            row = {key: str(value) for key, value in data.items()}
            
        else:
            row = {key: value for key, value in data.items()}

        processed_data.append(row)
    
    return processed_data


def save_dict_to_yaml(data, path: str):
    """
    딕셔너리를 YAML 파일로 저장하는 함수.

    Args:
        data (dict): 저장할 딕셔너리 데이터.
        file_path (str): 저장할 YAML 파일 경로.
    """

    out_dir = os.path.join(ROOT, YAML_DIR)
    file_path = os.path.join(out_dir, path)

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    try:
        with open(file_path, 'w', encoding='utf-8') as yaml_file:
            yaml.dump(data, yaml_file, allow_unicode=True, default_flow_style=False)
        print(f"Data successfully saved to {file_path}")

    except Exception as e:
        print(f"Error saving data to YAML: {e}")


def load_yaml_to_dict(file_path):
    """
    YAML 파일을 읽어서 Python 딕셔너리로 변환하는 함수.

    Args:
        file_path (str): 읽을 YAML 파일의 경로.

    Returns:
        dict: YAML 데이터를 변환한 딕셔너리.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as yaml_file:
            data = yaml.safe_load(yaml_file)  # 안전하게 YAML 로드
        return data
    except Exception as e:
        print(f"Error reading YAML file: {e}")
        return None


def save2json(path, df):
    # DataFrame을 딕셔너리로 변환
    data_dict = df.to_dict(orient='records')
    # JSON으로 저장
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=4)