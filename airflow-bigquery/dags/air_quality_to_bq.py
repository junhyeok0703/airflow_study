# 라이브러리 불러오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from google.cloud import bigquery
import logging
import pendulum

# 환경변수 설정하기
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/data/airflowbigquery-459215-f6b100bf62dc.json"
### E : Extract
# API 호출 (PythonOperator)
def call_api(**kwargs): # 마치 지하수와 같다. 땅 밑에서 흐르고 있는 천연 수
    # key = 'YOUR_DECODING_KEY'
    # SERVICE_KEY = os.getenv("SERVICE_KEY")
    key = '6yfcPk8KhWGB0lzeAn2/XzVwa+T19ZcB91wg4JyXZzDz1/pYzfFOZexpamJ1ceWKShdiSoUPaRnafRcLPhBhsQ=='
    url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty'
    
    params = {
        'serviceKey': key,
        'returnType': 'xml',
        'numOfRows': '10000',
        'sidoName': '전국',
        'ver': '1.0'
    }

    # GET 요청
    response = requests.get(url, params=params)

    # 응답 확인
    if response.status_code == 200:
        data = response.text
        kwargs['ti'].xcom_push(key='api_data', value=data)
    else:
        logging.error(f"API 호출 실패: {response.status_code} - {response.text}")
        raise Exception(f"API 호출 실패: {response.status_code} - {response.text}")

# 특정 태그 값을 안전하게 추출하는 함수
def convert_string(item_, key_):
    try:
        return item_.find(key_.lower()).text.strip()
    except AttributeError:
        return None

# XML 데이터를 DataFrame으로 변환하는 함수
def parse_to_dataframe(xml_data):
    xml = BeautifulSoup(xml_data.replace('\n', ''), "lxml")
    items = xml.findAll("item")
    item_list = []

    for item in items:
        item_dict = {
            '측정소명': convert_string(item, "stationName"),
            '측정망 정보': convert_string(item, "mangName"),
            '시도명': convert_string(item, "sidoName"),
            '측정일시': convert_string(item, "dataTime"),
            '아황산가스 농도': convert_string(item, "so2Value"),
            '일산화탄소 농도': convert_string(item, "coValue"),
            '오존 농도': convert_string(item, "o3Value"),
            '이산화질소 농도': convert_string(item, "no2Value"),
            '미세먼지 PM10 농도': convert_string(item, "pm10Value"),
            '미세먼지 PM10 24시간 예측이동농도': convert_string(item, "pm10Value24"),
            '초미세먼지 PM2point5 농도': convert_string(item, "pm25Value"),
            '초미세먼지 PM2point5 24시간 예측이동농도': convert_string(item, "pm25Value24"),
            '통합대기환경수치': convert_string(item, "khaiValue"),
            '통합대기환경지수': convert_string(item, "khaiGrade"),
            '아황산가스 지수': convert_string(item, "so2Grade"),
            '일산화탄소 지수': convert_string(item, "coGrade"),
            '오존 지수': convert_string(item, "o3Grade"),
            '이산화질소 지수': convert_string(item, "no2Grade"),
            '미세먼지 PM10 24시간 등급': convert_string(item, "pm10Grade"),
            '초미세먼지 PM2point5 24시간 등급': convert_string(item, "pm25Grade"),
            '미세먼지 PM10 1시간 등급': convert_string(item, "pm10Grade1h"),
            '초미세먼지 PM2point5 1시간 등급': convert_string(item, "pm25Grade1h"),
            '아황산가스 플래그': convert_string(item, "so2Flag"),
            '일산화탄소 플래그': convert_string(item, "coFlag"),
            '오존 플래그': convert_string(item, "o3Flag"),
            '이산화질소 플래그': convert_string(item, "no2Flag"),
            '미세먼지 PM10 플래그': convert_string(item, "pm10Flag"),
            '초미세먼지 PM2point5 플래그': convert_string(item, "pm25Flag"),
        }
        item_list.append(item_dict)

    df = pd.DataFrame(item_list)
    return df

### T : Transform
# 데이터 확인 및 가공 (PythonOperator)
def process_data(**kwargs):
    ti = kwargs['ti']
    api_data = ti.xcom_pull(task_ids='call_api', key='api_data')

    if api_data is None:
        raise Exception("API에서 받은 데이터가 없습니다.")

    df = parse_to_dataframe(api_data)
    file_path = '/opt/airflow/dags/data/data.csv'  # 로컬 저장 경로 설정

    df.to_csv(file_path, index=False)
    logging.info("데이터가 data.csv로 저장되었습니다.")

    # XCom에 CSV 파일 경로 저장
    ti.xcom_push(key='csv_file_path', value=file_path)

# L : Load
# 로컬 파일을 BigQuery로 로드하는 함수
def load_data_to_bigquery(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='process_data', key='csv_file_path')
    dataset_id = "airkorean"
    table_id = "data_sprint"

    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,  # 스키마 자동 감지 활성화
    )

    try:
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        logging.info(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")
    except Exception as e:
        logging.error(f"BigQuery 로드 실패: {str(e)}")
        raise Exception(f"BigQuery 로드 실패: {str(e)}")

default_args = dict(
    owner='junhyeok',
    email=['junhyeok7707@gmail.com'],
    email_on_failure=False,
    retries=1
)

# DAG 정의
with DAG(
    dag_id='air_quality_to_bq',
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False,
    default_args = default_args
) as dag:

    api_task = PythonOperator(
        task_id='call_api',
        python_callable=call_api,
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,

    )

    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_data_to_bigquery,
    
    )

    api_task >> process_task >> load_task # ETL