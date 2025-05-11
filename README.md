# 🛠️ Airflow study Repository

이 저장소는 **Apache Airflow**를 활용하여 데이터 파이프라인을 구축하고 자동화하는 실습을 위한 공간입니다.  
Docker 환경에서 Airflow를 구성하고, 다양한 Operator를 이용해 실제로 동작하는 ETL 프로세스를 구성하는 연습을 진행합니다.

---

## 🎯 학습 목표

- Airflow의 핵심 개념(DAG, Task, Operator, Scheduler)을 이해하고 직접 구성해보기
- PythonOperator, BashOperator, BranchPythonOperator 등 주요 Operator 실습
- 외부 API에서 데이터를 수집하고 전처리하여 BigQuery와 같은 데이터 웨어하우스로 적재하는 파이프라인 구성
- XCom, Trigger Rules, Task 의존성 설정을 통해 복잡한 워크플로우 제어 방법 학습
- Docker 기반 개발환경 설정 및 운영 경험 쌓기
- 실전과 유사한 시나리오(예: 미세먼지 API → BigQuery 업로드)로 ETL 자동화 실습

---

## 🧪 앞으로 할 실습 주제

### 📌 1. API 기반 ETL 파이프라인

- 공공데이터포털 또는 날씨, 환율 API에서 데이터 수집
- 흐름: API 호출 → JSON 파싱 → CSV 저장 → DB 적재
- 사용 기술: `PythonOperator`, `HttpSensor`, 재시도 설정

---

### 📌 2. 외부 CSV 파일 수집 및 DB 적재

- FTP/S3/GCS 등에 업로드되는 일일 리포트 자동 수집
- 흐름: 파일 감지 → 전처리 → MySQL or BigQuery 적재
- 사용 기술: `FileSensor`, `Pandas`, `BigQueryInsertJobOperator`

---

### 📌 3. 주기적인 웹 크롤링 + 분석

- 뉴스 헤드라인 크롤링 후 워드 클라우드 생성
- 흐름: 웹스크래핑 → 텍스트 분석 → 시각화 이미지 생성
- 사용 기술: `PythonOperator`, `BashOperator`, Slack 알림 연동

---

### 📌 4. 데이터 품질 검사 자동화

- 매일 수집되는 데이터에서 null, 중복, 이상치 자동 점검
- 흐름: CSV 로딩 → 검사 로직 실행 → 오류 리포트 생성
- 사용 기술: `BranchPythonOperator`, `TriggerRule`, SLA 경고

---

### 📌 5. GCP BigQuery 통합 파이프라인

- 수집 데이터 → BigQuery staging 테이블 → SQL 마트 테이블 생성
- 흐름: API or CSV → BQ 적재 → BQ SQL 실행
- 사용 기술: `BigQueryInsertJobOperator`, `PythonOperator`

---

### 📌 6. 로그 데이터 집계 파이프라인

- 사용자 이벤트 로그 → 세션 분리 및 주간 집계 테이블 생성
- 흐름: GCS 업로드된 로그 수집 → 전처리 → 요약 테이블 생성
- 사용 기술: 파티션 처리, 날짜 필터링, 반복 실행

---

### 📌 7. 데이터 시각화 및 보고 자동화

- 매출 데이터 수집 → Plotly로 시각화 → Slack 전송
- 흐름: DB 쿼리 실행 → 그래프 생성 → 이미지 전송
- 사용 기술: `PostgresOperator`, `Plotly`, `SlackWebhookOperator`

---

## 🐳 실행 환경

- Python 3.12
- Apache Airflow 3.0+
- Docker / Docker Compose
- Google Cloud SDK

---

## 📝 기타

- GCP 키 파일, 로그 파일 등 민감하거나 불필요한 항목은 `.gitignore`로 관리합니다.
- DAG 작성 시에는 운영에 적합한 Task 의존성, 실패 알림, 리소스 관리 등을 고려합니다.

---

> 🔐 실습 중 사용되는 API Key, GCP 키 파일 등은 절대 커밋하지 않고 `.env` 또는 `Docker Secret` 방식으로 관리하세요.
