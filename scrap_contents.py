from typing import Optional, Union

import requests
import pandas as pd
from fire import Fire
from tqdm import tqdm
from bs4 import BeautifulSoup

from utils import get_full_html, extract_data, extract_multiple_to_df, load_yaml_to_dict, save2json


SITE = "https://www.dasny.org/"

def main(yaml: Optional[str] = "urls/bid-results-and-awards.yaml", output: Optional[Union[str, None]] = None):
    # YAML에서 URL 목록 로드
    urls = load_yaml_to_dict(yaml)
    
    # 결과를 담을 리스트
    data = []

    for url in tqdm(urls.values(), desc="Processing URLs"):
        # HTML 페이지에서 데이터 추출
        soup = get_full_html(SITE+url)  # HTML 가져오는 함수
        extracted = extract_data(soup)  # 데이터 추출
        data.append(extracted)

    iscsv = not output or output.endswith(".csv")
    
    # pandas DataFrame으로 변환
    df = pd.DataFrame(extract_multiple_to_df(data, iscsv))

    if not output:
        df.to_csv(f"output/csv/{yaml.split('/')[-1].split('.')[0]}.csv", index=False)
        print("Data processing complete. File saved as combined_data.csv")

    elif output.endswith(".json"):
        save2json(output, df)
        print(f"Data saved to {output} in JSON format.")

    elif output.endswith(".csv"):
        df.to_csv(output, index=False)
        print(f"Data saved to {output} in CSV format.")

    else:
        raise ValueError("Unsupported file format. Please use .csv or .json extension.")


if __name__ == "__main__":
    Fire(main)