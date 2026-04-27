"""적시성(Timeliness) 평가 모듈

지표:
- 데이터기준시점 컬럼 여무 (테이블)   — 자동화 수준 0
- 데이터 기준 시점 최신성 (컬럼)      — 자동화 수준 0 (조건부 수행)

최신성 점수 공식:
    score(t) = 1 / (1 + e^(k × (t − 365)))
    t : 기준일로부터 오늘까지의 일수
    k : 기울기 파라미터 (기본값 0.010)
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ..report import EvalStatus, MetricResult

_DIMENSION = "적시성"

# 기준시점 컬럼을 탐지할 때 사용하는 키워드 목록
_DATE_KEYWORDS = [
    "기준일",
    "기준시점",
    "기준날짜",
    "기준일자",
    "데이터기준일자",
    "기준연월",
    "기준년월",
    "reference_date",
    "ref_date",
    "base_date",
    "data_date",
]


def detect_reference_date_columns(
    df: pd.DataFrame,
    reference_date_columns: Optional[List[str]] = None,
) -> List[str]:
    """기준시점 컬럼 후보를 반환한다.

    1) 사용자 지정(reference_date_columns) 우선
    2) 미지정 시 컬럼명 키워드 기반 자동 탐지
    """
    if reference_date_columns:
        return [c for c in reference_date_columns if c in df.columns]

    return [
        col
        for col in df.columns
        if any(kw.lower() in col.lower() for kw in _DATE_KEYWORDS)
    ]


# ------------------------------------------------------------------ #
# 데이터기준시점 컬럼 여무 — 테이블                                      #
# ------------------------------------------------------------------ #
def timeliness_column_presence(
    df: pd.DataFrame,
    reference_date_columns: Optional[List[str]] = None,
) -> MetricResult:
    """데이터기준시점 컬럼 존재 여부.

    Parameters
    ----------
    reference_date_columns:
        column_type_dict에서 is_reference_date=True 로 지정된 컬럼 목록.
        지정이 없으면 컬럼명 키워드로 자동 탐지한다.

    존재=1, 부재=0
    """
    found = detect_reference_date_columns(
        df,
        reference_date_columns=reference_date_columns,
    )

    if found:
        score, status = 1, EvalStatus.PASS
        detail = f"데이터기준시점 컬럼 존재: {found}"
    else:
        score, status = 0, EvalStatus.WARNING
        detail = "데이터기준시점 컬럼 미존재"

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="데이터기준시점 컬럼 여무",
        scope="table",
        value=float(score),
        score=score,
        status=status,
        formula="존재=1, 부재=0",
        detail=detail,
    )


# ------------------------------------------------------------------ #
# 데이터 기준 시점 최신성 — 컬럼                                         #
# ------------------------------------------------------------------ #
def timeliness_freshness(
    df: pd.DataFrame,
    column: str,
    column_config: Dict[str, Any],
    k: float = 0.010,
    reference_date: Optional[datetime] = None,
) -> Optional[MetricResult]:
    """데이터 기준 시점 최신성 점수 계산.

    score(t) = 1 / (1 + e^(k × (t − 365)))
    개별 행의 점수를 산술 평균하여 컬럼 대표값으로 사용한다.

    Parameters
    ----------
    column_config:
        {"type": "date", "format": "%Y-%m-%d", "is_reference_date": True}
    k:
        기울기 파라미터 (기본값 0.010)
        예상 점수 — 현재 기준: 1개월≈0.96, 6개월≈0.86, 1년≈0.50, 2년≈0.025
    reference_date:
        기준 시점 (기본값: 오늘)
    """
    today = reference_date or datetime.now()
    date_fmt = column_config.get("format")

    scores: List[float] = []
    parse_errors: List[str] = []

    for v in df[column].dropna():
        raw = str(v).strip()
        if not raw:
            continue
        try:
            if date_fmt:
                ref_dt = datetime.strptime(raw, date_fmt)
            else:
                ref_dt = pd.to_datetime(raw).to_pydatetime()
            t = (today - ref_dt).days
            score = 1.0 / (1.0 + math.exp(k * (t - 365)))
            scores.append(score)
        except (ValueError, TypeError, OverflowError):
            parse_errors.append(raw)

    if not scores:
        return MetricResult(
            dimension=_DIMENSION,
            metric_name="데이터 기준 시점 최신성",
            scope="column",
            column=column,
            value=None,
            score=None,
            status=EvalStatus.SKIP,
            formula="score(t) = 1 / (1 + e^(k × (t − 365))), k=0.010",
            detail=f"유효한 날짜 데이터 없음 (파싱 오류 {len(parse_errors)}건)",
        )

    avg_score = sum(scores) / len(scores)
    point_score = avg_score * 100.0

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="데이터 기준 시점 최신성",
        scope="column",
        column=column,
        value=round(point_score, 4),
        score=round(point_score, 4),
        status=EvalStatus.PASS,
        formula="score(t) = (1 / (1 + e^(k × (t − 365)))) × 100, k=0.010",
        detail=(
            f"평균 최신성 점수: {point_score:.4f}점 "
            f"(유효 {len(scores)}건, 파싱 오류 {len(parse_errors)}건)"
        ),
    )
