

<!-- PROJECT LOGO -->
<br />
  <h1 align="center">Spotify API를 이용한 K-POP 인기 탐색 분석 대시보드</h1>
  <p align="center">
    Tableau를 이용한 K-POP 인기 탐색 분석
    <br />
    <br />
    <img src="https://img.shields.io/badge/spotify-1DB954?style=for-the-badge&logo=spotify&logoColor=white">
    <img src="https://img.shields.io/badge/Reddit-FF4500?style=for-the-badge&logo=Reddit&logoColor=white">
    <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white">
    <img src="https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white">
    <br />
    <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white">
    <img src="https://img.shields.io/badge/tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white">
    <img src="https://img.shields.io/badge/amazon aws-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white">
    <img src="https://img.shields.io/badge/aws s3-569A31?style=for-the-badge&logo=amazons3&logoColor=white"> 
  </p>
</div>

<!-- ABOUT THE PROJECT -->
## 📋 About The Project
* **기간** : 6/26(월) ~ 6/30(금)
* **목표** : 이전에 진행했던 대시보드 프로젝트의 데이터 파이프라인을 구축한 고도화 프로젝트 진행
* **주제** : End-to-end 데이터 파이프라인 구성하기

Spotify, Reddit API를 활용해서 얻은 대량의 데이터를 Amazon S3에 저장하고, 이후에 Snowflake에 데이터를 로드하였습니다. </br>
또한 DBT를 사용하여 데이터를 가공하고 이 과정을 Airflow 워크플로우로 관리하였습니다. 이렇게 구축한 환경을 활용하여 K-POP 인기 탐색 분석을 Tableau로 대시보드를 제작하여 데이터 시각화를 했습니다.

<!-- CONTACT -->
## 👥 Contact

### Members
<table>
  <tbody>
    <tr>
      <td align="center"><a href=""><img src="https://avatars.githubusercontent.com/u/62873925?v=4" width="200px;" alt=""/><br /><sub><b>박세정</b></sub></a><br /></td>
      <td align="center"><a href=""><img src="https://avatars.githubusercontent.com/u/131653718?v=4" width="200px;" alt=""/><br /><sub><b>박재연</b></sub></a><br /></td>
      <td align="center"><a href=""><img src="https://avatars.githubusercontent.com/u/69243257?v=4" width="200px;" alt=""/><br /><sub><b>신유창</b></sub></a><br /></td>
      <td align="center"><a href=""><img src="https://avatars.githubusercontent.com/u/103317018?v=4" width="200px;" alt=""/><br /><sub><b>오유정</b></sub></a><br /></td>
      <td align="center"><a href=""><img src="https://avatars.githubusercontent.com/u/79040336?v=4" width="200px;" alt=""/><br /><sub><b>이소연</b></sub></a><br /></td>
     </tr>
  </tbody>
</table>

## 🔎 프로젝트 구조
![image](https://github.com/data-engineering-team4/kpop_dashboard/assets/103317018/de5ecc94-65ca-4b08-a128-b41acc09a568)

## 📦 Features
### Data Pipeline

**✔️ Spotify Data ETL**
<img width="1181" alt="스크린샷 2023-07-06 오전 2 13 18" src="https://github.com/data-engineering-team4/kpop_dashboard/assets/79040336/8fdb3485-884a-4abb-841c-12c8f492fe4f">

Spotify API를 사용하여 K-pop 아티스트, 앨범, 트랙 데이터를 스크래핑하여 S3에 저장하고, 이후 Snowflake에 적재합니다.


**✔️ Spotify chart ETL**

![스크린샷 2023-07-06 오전 2 11 45](https://github.com/data-engineering-team4/kpop_dashboard/assets/79040336/23a61a67-fd23-482e-99e7-5c6fbb5b7aac)

Spotify의 주간 및 연간 차트 데이터를 웹 스크래핑하여 전처리한 후 S3에 저장하고, Snowflake에 로드하는 작업을 자동화했습니다.


**✔️ Reddit Data ETL**
![스크린샷 2023-07-06 오전 2 15 44](https://github.com/data-engineering-team4/kpop_dashboard/assets/79040336/4bb9e0a9-fcb0-4e93-ad92-43faf560172e)


Reddit에서 포스트와 댓글 데이터를 수집하여 CSV 파일로 저장하고, S3에 업로드한 후 Snowflake에 로드하는 작업을 Airflow로 자동화했습니다.


**✔️ delete log DAG**

<img width="772" alt="스크린샷 2023-07-06 오전 2 15 58" src="https://github.com/data-engineering-team4/kpop_dashboard/assets/79040336/7fedc8b5-4588-4de2-a6f4-cd99da9384eb">


Airflow 운영 기간 동안 누적된 Tasks log 용량 관리를 위해 매일 오전 2시에 30일 이상된 로그 파일을 자동으로 삭제하는 작업을 수행합니다.


**✔️ dbt run DAG**

<img width="433" alt="스크린샷 2023-07-06 오전 2 16 16" src="https://github.com/data-engineering-team4/kpop_dashboard/assets/79040336/de74b418-b2dd-41d5-b608-a22f2eb3b393">


DBT(Data Build Tool)를 사용하여 데이터 변환 작업을 실행합니다.

---

### Dashboard
* 1년간 글로벌 스트리밍 현황을 분석합니다.
* K-POP 트랙과 아티스트에 대한 국가별 선호도 및 오디오 특성을 분석합니다.
* K-POP 그룹별 차트 순위와 추이를 보여주고, 오디오 특성을 비교합니다.
* Reddit 사용자들의 K-POP 관련 게시물과 댓글의 반응을 분석합니다.


## 📊 Charts
### 1) Spotify Global 최근 1년 스트리밍 현황 
<img width="900" alt="image" src="https://github.com/learn-programmers/KDT_DATA_1st/assets/103317018/e5d1dfdb-18b4-47bf-9e82-db940d3b81d6">

### 2) K-pop 트랙과 아티스트에 대한 국가 선호 빈도 수 및 오디오 특성
<img width="900" alt="image" src="https://github.com/learn-programmers/KDT_DATA_1st/assets/103317018/5d7bbf2a-cc76-4f05-ad78-266afe262da3">

### 3) K-pop 그룹별 Spotify 차트 최고 순위와 추이
<img width="900" alt="image" src="https://github.com/learn-programmers/KDT_DATA_1st/assets/103317018/a565ed15-2f8b-4cd8-b920-ea0419971942">

### 4) Global, K-pop, 유명 K-pop 그룹의 오디오 특성 비교
<img width="900" alt="image" src="https://github.com/learn-programmers/KDT_DATA_1st/assets/103317018/6312d24d-627f-4676-9e96-4e6662e03faa">

### 5) K-pop 아티스트 별 오디오 특성
<img width="900" alt="image" src="https://github.com/learn-programmers/KDT_DATA_1st/assets/103317018/e0f997e7-33a3-4017-9b67-954d4b0cb70e">

### 6) K-pop 아티스트 콜라보레이션 네트워크
<img width="900" alt="image" src="https://github.com/learn-programmers/KDT_DATA_1st/assets/103317018/4cfc3924-3a08-4a3d-aa12-1d253098f487">

### 7) Reddit의 K-pop 관련 게시물과 댓글 데이터 WordCloud
Reddit K-pop 관련 게시물 데이터 WordCloud
![posts_wordcloud](https://github.com/data-engineering-team4/kpop_dashboard/assets/79040336/589f020f-36a3-4684-bb6b-466ceb35108a)

Reddit K-pop 관련 댓글 데이터 WordCloud
![comments_wordcloud](https://github.com/data-engineering-team4/kpop_dashboard/assets/79040336/0eeee9c9-9d08-46ed-8745-446f23b04597)



## 🎥 View Demo
[![Spotify API를 이용한 K-POP 인기 탐색 분석 대시보드](https://img.youtube.com/vi/J-N1ytzZilU/0.jpg)](https://www.youtube.com/watch?v=J-N1ytzZilU)





