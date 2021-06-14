"""
etl process class, function 구현
다음의 ELT process 구현

- Extraction
  - HDFS File Extraction(binding)
  - postgreSQL Extraction
  - read the csv file

- Loading
  - load csv file to HDFS
  - load statistics to postgreSQL
  - load control chart UCL, LCL, CL value

- Transaction
  - n 개월치 window data slicing
  - data 기초 통계량 계산(평균, 중앙값, 표준편차, 분산, 최대, 최소, 1분위, 3분위, 왜도, 첨도)
  - Shewhart 관리도 UCL, LCL, CL
  - 결측치 처리
"""