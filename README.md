# Data Pipeline
- 여러 가지 데이터 파이프라인 툴을 사용하여 파이프라인 구축을 시도해보는 프로젝트입니다.
- 실험하고자 하는 파이프라인이 있으면 이 프로젝트에 계속 업데이트할 예정입니다.
- 파이프라인에 사용할 만한 툴들을 테스트하는 코드도 포함되어 있습니다. (ex. kinesis 동작 테스트)

---

### Stack

  - Airflow
  - Kafka
  - Spark, Fluentd, Flink
  - GCP:BigQuery(질의), DataStudio
  - Kibana
  - DB
    - MariaDB
    - ES
  - Storage
    - S3

---

작업 리스트
1. Airflow를 이용한 batch ELT/ETL 작업
   - EDA 등 데이터 분석 포함
   - Data: [ListenBrainz](https://listenbrainz.org/data/)
2. Bigdata 파이프라인 작업