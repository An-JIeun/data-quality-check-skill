# data-quality-check-skill

💫 **테이블 데이터의 품질을 자동으로 검사하는 Claude Skill**

---

## 소개

`data-quality-check-skill`은 CSV, XLSX, XLS 등 다양한 테이블 데이터 파일의 품질을 자동으로 평가하고,
결과를 표 형식 리포트와 함께 데이터 정제(클렌징) 가이드로 제공합니다.



---

## 설치 및 환경 준비

최초 사용 시 아래 명령어로 Python 가상환경과 의존성을 자동으로 설치하세요.

```bash
# 1. 셸 스크립트 실행 (최초 1회)
./setup_env.sh

# 2. 가상환경 활성화 (매 세션)
source .venv/bin/activate
```

---


## Claude Code에서 사용하는 방법

### 1. 품질 검사 실행

> *반드시 `data-quality-check-skill` 폴더에서 claude를 실행시켜주세요!*

Claude 대화창에서 아래와 같이 입력하세요:

```
품질 검사 해줘
```
또는
```
/quality check
```
또는
```
이 데이터 검수해줘
```

그리고 CSV/XLSX/XLS 파일을 업로드하거나 경로를 입력하면 자동으로 품질 검사가 실행됩니다.

#### (선택) 컬럼 타입 지정
특정 컬럼의 타입을 지정하고 싶다면 아래처럼 JSON 형태로 함께 입력할 수 있습니다.

```
품질 검사 /path/to/data.csv {"col1": "date", "col2": "email"}
```

### 2. 결과 확인

- 검사 결과는 마크다운 표와 함께, 데이터 정제 가이드가 자동으로 출력됩니다.
- (자동 저장) qc_reports/ 폴더에 MD/TTL 리포트 파일이 생성됩니다.

---

---

## 주요 평가 항목

| 평가 차원     | 주요 지표 예시                | 설명                                 |
|:-------------:|:----------------------------:|:-------------------------------------|
| 기계가독성    | 파일 포맷, 인코딩, 구조 정규성 | 기계가 읽을 수 있는 표준 포맷 여부   |
| 완전성        | 결측률, 중복률, 후보키 비율    | 데이터 누락/중복/식별자 적정성 평가 |
| 정확성        | 구문 정확성, 값 오류율         | 컬럼별 데이터 형식 및 값의 정확성    |
| 적시성        | 기준일 컬럼, 최신성 점수       | 데이터의 최신성 및 기준일 적정성     |

---

## 폴더 구조

```
.
├── pipeline.py         # 품질 평가 파이프라인
├── report.py           # 평가 결과 리포트 모델
├── scripts/
│   ├── run_check.py    # 품질 검사 실행기 (CLI)
│   └── generate_report.py # 리포트 생성기
├── evaluators/         # 평가 모듈 (기계가독성, 완전성, 정확성, 적시성 등)
│   ├── acceptability.py
│   ├── completeness.py
│   ├── machine_readability.py
│   ├── validity.py
│   └── type_inferencer.py
├── requirements.txt    # 의존성 목록
├── setup_env.sh        # 가상환경 자동 구축 스크립트
└── SKILL.md            # Claude Skill 설명 및 트리거 조건
```

---

## 참고

- Python 3.10 이상 필요
- MIT License

---

문의/기여: PR 또는 이슈로 남겨주세요.
