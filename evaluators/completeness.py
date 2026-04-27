"""완전성(Completeness) 평가 모듈

지표:
- 결측률 (테이블·컬럼)
- 미사용률
- 중복률 — 컬럼 일관성
- 중복률 — 값 일관성 (행 중복)
- 후보키 비율
"""

from __future__ import annotations

import math

import pandas as pd

from ..report import EvalStatus, MetricResult

_DIMENSION = "완전성"


# ---- 공통 헬퍼 --------------------------------------------------------
def _blank_mask(series: pd.Series) -> pd.Series:
    """NULL 또는 빈 문자열을 결측으로 처리한 boolean 마스크 반환."""
    normalized = series.astype(str).str.strip()
    normalized = normalized.str.strip("'\"")
    lowered = normalized.str.lower()
    blank_tokens = {
        "",
        "none",
        "nan",
        "null",
        "na",
        "n/a",
        "해당없음",
        "해당 없음",
        "없음",
        "미해당",
        "-",
    }
    return series.isnull() | lowered.isin(blank_tokens)


# ------------------------------------------------------------------ #
# 결측 — 테이블                                                         #
# ------------------------------------------------------------------ #
def missing_rate_table(df: pd.DataFrame) -> MetricResult:
    """테이블 전체 결측률.

    결측률(%) = (전체 공백 데이터 수 / (전체 행 × 전체 컬럼)) × 100
    """
    total_cells = df.shape[0] * df.shape[1]
    if total_cells == 0:
        rate = 0.0
        missing_count = 0
    else:
        missing_count = int(sum(_blank_mask(df[c]).sum() for c in df.columns))
        rate = (missing_count / total_cells) * 100

    score = 100 - rate

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="결측률",
        scope="table",
        value=round(rate, 4),
        score=round(score, 4),
        status=EvalStatus.PASS,
        formula="결측률=(전체 공백 데이터 / (전체 행 × 전체 컬럼)) × 100; 점수=100-결측률",
        detail=(
            f"결측 셀 {missing_count:,}개 / 전체 {total_cells:,}개 → "
            f"결측률 {rate:.4f}%, 점수 {score:.4f}"
        ),
    )


# ------------------------------------------------------------------ #
# 결측 — 컬럼                                                           #
# ------------------------------------------------------------------ #
def missing_rate_column(df: pd.DataFrame, column: str) -> MetricResult:
    """개별 컬럼 결측률.

    결측률(%) = (대상 컬럼의 공백 데이터 수 / 전체 행 수) × 100
    """
    total_rows = len(df)
    if total_rows == 0:
        rate, missing_count = 0.0, 0
    else:
        missing_count = int(_blank_mask(df[column]).sum())
        rate = (missing_count / total_rows) * 100

    score = 100 - rate

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="결측률",
        scope="column",
        column=column,
        value=round(rate, 4),
        score=round(score, 4),
        status=EvalStatus.PASS,
        formula="결측률=(대상 컬럼의 공백 데이터 / 전체 행 수) × 100; 점수=100-결측률",
        detail=(
            f"결측 {missing_count:,}개 / {total_rows:,}행 → "
            f"결측률 {rate:.4f}%, 점수 {score:.4f}"
        ),
    )


# ------------------------------------------------------------------ #
# 미사용 컬럼률                                                          #
# ------------------------------------------------------------------ #
def unused_column_rate(df: pd.DataFrame) -> MetricResult:
    """미사용(전체 행이 공백인) 컬럼 비율.

    미사용률(%) = (미사용 컬럼 개수 / 전체 컬럼 개수) × 100
    """
    total_cols = len(df.columns)
    if total_cols == 0:
        rate = 0.0
        unused = []
    else:
        unused = [col for col in df.columns if _blank_mask(df[col]).all()]
        rate = (len(unused) / total_cols) * 100

    score = 100 - rate

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="미사용률",
        scope="table",
        value=round(rate, 4),
        score=round(score, 4),
        status=EvalStatus.PASS,
        formula="미사용률=(미사용 컬럼 개수 / 전체 컬럼 개수) × 100; 점수=100-미사용률",
        detail=(
            f"미사용 컬럼 {len(unused)}개: {unused} → "
            f"미사용률 {rate:.4f}%, 점수 {score:.4f}"
        ),
    )


# ------------------------------------------------------------------ #
# 고유성 — 컬럼 일관성 (컬럼명 + 값이 완전히 중복인 컬럼)               #
# ------------------------------------------------------------------ #
def column_duplicate_rate(df: pd.DataFrame) -> MetricResult:
    """중복 컬럼 비율 — 컬럼명과 모든 값이 동일한 컬럼.

    중복률(%) = (중복 컬럼 개수 / 전체 컬럼 개수) × 100
    """
    total_cols = len(df.columns)
    if total_cols == 0:
        rate = 0.0
        dup_cols: list[str] = []
    else:
        # 컬럼명 → (컬럼명, 값 튜플) fingerprint 목록
        seen: dict[tuple, str] = {}
        dup_cols = []
        for col in df.columns:
            fp = (col, tuple(df[col].values))
            if fp in seen:
                dup_cols.append(col)
            else:
                seen[fp] = col
        rate = (len(dup_cols) / total_cols) * 100

    score = 100 - rate

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="중복률 (컬럼 일관성)",
        scope="table",
        value=round(rate, 4),
        score=round(score, 4),
        status=EvalStatus.PASS,
        formula="중복률=(중복 컬럼 개수 / 전체 컬럼 개수) × 100; 점수=100-중복률",
        detail=(
            f"중복 컬럼 {len(dup_cols)}개: {dup_cols} → "
            f"중복률 {rate:.4f}%, 점수 {score:.4f}"
        ),
    )


# ------------------------------------------------------------------ #
# 고유성 — 값 일관성 (완전히 동일한 행)                                 #
# ------------------------------------------------------------------ #
def row_duplicate_rate(df: pd.DataFrame) -> MetricResult:
    """행 중복 비율.

    중복률(%) = (중복 행 개수 / 전체 행 개수) × 100
    """
    total_rows = len(df)
    if total_rows == 0:
        rate, dup_count = 0.0, 0
    else:
        dup_count = int(df.duplicated().sum())
        rate = (dup_count / total_rows) * 100

    score = 100 - rate

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="중복률 (값 일관성)",
        scope="table",
        value=round(rate, 4),
        score=round(score, 4),
        status=EvalStatus.PASS,
        formula="중복률=(중복 행 개수 / 전체 행 개수) × 100; 점수=100-중복률",
        detail=(
            f"중복 행 {dup_count:,}개 / {total_rows:,}행 → "
            f"중복률 {rate:.4f}%, 점수 {score:.4f}"
        ),
    )


# ------------------------------------------------------------------ #
# 식별성 — 후보키 비율                                                   #
# ------------------------------------------------------------------ #
def candidate_key_rate(df: pd.DataFrame) -> MetricResult:
    """후보키 컬럼 비율.

    후보키 조건: 모든 값이 유니크하고 결측이 없는 컬럼.
    비율(%) = (후보키 컬럼 개수 / 전체 컬럼 개수) × 100
    """
    total_cols = len(df.columns)
    excluded_suffix_keywords = ["명", "이름", "부서", "기관", "주소"]

    def _is_excluded_column(col_name: str) -> bool:
        return any(kw in str(col_name) for kw in excluded_suffix_keywords)

    if total_cols == 0 or len(df) == 0:
        rate = 0.0
        keys: list[str] = []
        excluded_cols: list[str] = []
    else:
        excluded_cols = [col for col in df.columns if _is_excluded_column(col)]
        keys = [
            col
            for col in df.columns
            if (
                not _is_excluded_column(col)
                and not _blank_mask(df[col]).any()
                and df[col].nunique() == len(df)
            )
        ]
        rate = (len(keys) / total_cols) * 100

    k = 0.1
    sigmoid_score = 1.0 / (1.0 + math.exp(-k * (rate - 0.0)))
    score = sigmoid_score * 100.0

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="후보키 비율",
        scope="table",
        value=round(rate, 4),
        score=round(score, 4),
        status=EvalStatus.PASS,
        formula=(
            "sigmoid(x) = 1 / (1 + e^(-k × (x - 0))), k=0.1; "
            "x = (후보키 컬럼 개수 / 전체 컬럼 개수) × 100 "
            "(단, 컬럼명에 명/이름/부서/기관/주소 포함 컬럼 제외)"
        ),
        detail=(
            f"후보키 컬럼 {len(keys)}개: {keys}, "
            f"제외 컬럼 {len(excluded_cols)}개: {excluded_cols} → "
            f"비율 {rate:.4f}%, 시그모이드 점수 {score:.4f}"
        ),
    )
