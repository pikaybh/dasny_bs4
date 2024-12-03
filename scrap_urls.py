from typing import Optional

import requests
from fire import Fire
from tqdm import tqdm
from bs4 import BeautifulSoup

from utils import get_full_html, extract_title_url, save_dict_to_yaml


# URL 설정 (크롤링하려는 실제 URL로 변경 필요)
URL = "https://www.dasny.org/index.php/opportunities/"

def main(target: Optional[str] = "bid-results-and-awards", pages: int = 13):
    urls = [f"{URL}{target}?page={page}" for page in range(pages)]
    
    url_dict = {}
    for url in tqdm(urls):
        url_dict.update(extract_title_url(get_full_html(url)))
    save_dict_to_yaml(url_dict, f"{target}.yaml")

if __name__ == "__main__":
    Fire(main)