import os
import io
import time
import requests
import boto3
from bs4 import BeautifulSoup
from urllib.parse import urljoin

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-1")

s3_bucket = "BUCKET_NAME"
upload_dir = "pdfs"
keyword = "対策案"

def upload_pdf_to_s3(pdf_url):
    # S3クライアント
    s3 = boto3.client(
        's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='ap-northeast-1'
        )
    s3_key = f"{upload_dir}/{os.path.basename(pdf_url)}"

    # PDF取得
    res = requests.get(pdf_url)
    res.raise_for_status()

    # メモリ上でファイル化
    file_obj = io.BytesIO(res.content)

    # S3にアップロード
    s3.upload_fileobj(file_obj, s3_bucket, s3_key)

    # 署名付きURL生成（1時間有効）
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': s3_bucket, 'Key': s3_key},
        ExpiresIn=3600
    )
    
    print(f"Signed URL: {url}")

def download_all_pdfs(keyword):
    # 検索キーワードを指定
    base_url = "https://data.e-gov.go.jp/data/dataset/"
    params = {
        "q": keyword,
        "organization": "default",
        "page": 1
    }

    while True:
        # ページを取得
        response = requests.get(base_url, params=params)
        soup = BeautifulSoup(response.text, "html.parser")

        # データセットページのリンクを抽出
        dataset_links = [
            urljoin(base_url, a["href"])
            for a in soup.select("a.link_normal.list_dataset_item_title")
        ]

        if not dataset_links:
            break

        # 各データセットにアクセスしてPDFを探す
        for dataset_url in dataset_links:
            dataset_res = requests.get(dataset_url)
            dataset_soup = BeautifulSoup(dataset_res.text, "html.parser")

            # PDFリンクを抽出
            pdf_links = [
                urljoin(dataset_url, a["href"])
                for a in dataset_soup.select("a[href$='.pdf']")
            ]

            # PDFをS3にアップロード
            for pdf_url in pdf_links:
                filename = pdf_url.split("/")[-1]
                upload_pdf_to_s3(pdf_url)
                time.sleep(1)  # サーバー負荷軽減

        # 次のページへ
        next_page = soup.select_one("a[rel='next']")
        if not next_page:
            break
        params["page"] += 1

    print("Complete.")
    
def main():
    download_all_pdfs(keyword)

if __name__ == "__main__":
    main()