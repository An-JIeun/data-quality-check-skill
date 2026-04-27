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

## 사용법

### 1. Python 스크립트로 직접 실행

```bash
# 품질 검사 실행 (결과: JSON)
python scripts/run_check.py <data.csv> [--col-dict '{"col1":"date",...}']

# 품질 리포트(MD/TTL) 생성
python scripts/generate_report.py <result.json> --output-dir ./qc_reports
```

### 2. Claude Skill로 사용

- Claude에서 "품질 검사", "quality check", "데이터 검수" 등 명령어와 함께 데이터 파일을 업로드하거나 경로를 입력하면 자동 실행됩니다.

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

- Python 3.8 이상 필요
- MIT License

---

문의/기여: PR 또는 이슈로 남겨주세요.
