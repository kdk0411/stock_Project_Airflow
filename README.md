# 주식 정보 시각화 - Airflow & YFinance

## 프로젝트 소개
- 지난 프로젝트에서 Django를 사용하여 주식 정보를 시각화 하였으나 
- [stock_Porject](https://github.com/kdk0411/stock_Project)

## 1. 개발 환경
- Astro 12.1.0
- MinIO RELEASE.2024-06-13T22-53-53Z-cpuv1
- Metabase v0.50.4

## 2. ETL 파이프라인
### 2-1. Extract
- Beautiful Soup을 사용해 주식 시장에서 가장 활발한 Symbol 추출
- YFinance API로 Symbol의 유효성을 검사하고 데이터를 추출한 뒤, MinIO에 저장

### 2-2. Transform
- JSON 형태로 저장된 데이터를 Pandas 데이터프레임으로 변환 후, CSV 파일로 변환하여 다시 MinIO에 저장

### 2-3. Load
추출 및 변환된 CSV 파일을 PostgreSQL에 저장하여, 이후 Metabase를 통한 대시보드 시각화

![{F1A50514-818E-46A0-907E-EFCE5F71F175}](https://github.com/user-attachments/assets/17ebce96-1d9d-456e-ae17-9ebd72e9b580)

## 3. 시각화
![{80D346DA-B076-4F4C-AE29-A9F11B0F08AA}](https://github.com/user-attachments/assets/49f53936-fd48-4bc7-af43-f9bc18948a39)
