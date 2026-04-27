"""정확성(Accuracy) 평가 모듈 — 구문정확성

자동화 수준 2 (업무규칙 필요).
기본 파이프라인(level=0)에서는 실행되지 않으며,
column_type_dict에 타입이 정의된 경우에만 평가한다.

지원 타입:
- literal        : allowed_values 목록과 대조
- email          : RFC 5322 기반 정규식
- phone_kr       : 한국 전화번호 패턴
- date           : 지정 포맷 파싱 가능 여부
- postcode       : 우편번호 (5자리 또는 NNN-NNN)
- sido_kr        : 시도코드 (2자리 숫자)
- sigungu_kr     : 시군구코드 (5자리 숫자)
- beopjeongdong  : 법정동코드 (10자리 숫자)
- haengjeong     : 행정구역코드 (10자리 숫자)
- latitude       : 위도 (소수점 포함 십진수, -90.0 ~ 90.0)
- longitude      : 경도 (소수점 포함 십진수, -180.0 ~ 180.0)
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ..report import EvalStatus, MetricResult

_DIMENSION = "정확성"
_FORMULA = "(허용값 외 데이터 수 / 전체 비공백 데이터 수) × 100"
_SCORE_FORMULA = "100 - 오류율(%)"


def syntactic_validity(
    df: pd.DataFrame,
    column: str,
    column_config: Dict[str, Any],
    business_rules: Optional[Dict] = None,
) -> MetricResult:
    """구문정확성 — 컬럼 수준 (자동화 수준 2).

    Parameters
    ----------
    column_config:
        {"type": "literal", "allowed_values": [...]} 등
    business_rules:
        {"col_name": {"allowed_values": [...]}} 형태의 업무규칙
        (column_config에 allowed_values가 없을 때 폴백)
    """
    rules = business_rules or {}

    col_data = df[column].dropna()
    col_data = col_data[col_data.astype(str).str.strip() != ""]
    total_non_blank = len(col_data)

    def _skip(reason: str) -> MetricResult:
        return MetricResult(
            dimension=_DIMENSION,
            metric_name="구문정확성",
            scope="column",
            column=column,
            value=float("nan"),
            score=float("nan"),
            status=EvalStatus.SKIP,
            formula=_FORMULA,
            detail=reason,
            automation_level=2,
        )

    if total_non_blank == 0:
        return _skip("비공백 데이터 없음")

    col_type = column_config.get("type", "")
    invalid_count = 0

    # ---- literal -------------------------------------------------
    if col_type == "literal":
        # 자동 추론으로 생성된 allowed_values(관측값 기반)는 오류율 산정 불가
        if column_config.get("auto_allowed_values"):
            unique_vals = column_config.get("allowed_values", [])
            basis = column_config.get("basis", "")
            return MetricResult(
                dimension=_DIMENSION,
                metric_name="구문정확성",
                scope="column",
                column=column,
                value=None,
                score=None,
                status=EvalStatus.SKIP,
                formula=_FORMULA,
                detail=(
                    f"자동추론 범주형 컬럼 [{basis}] — "
                    f"관측 고유값({len(unique_vals)}종): {unique_vals[:10]}"
                    + (" ..." if len(unique_vals) > 10 else "")
                    + " (허용값 목록 확정 필요)"
                ),
                automation_level=2,
            )

        allowed: List[Any] = column_config.get(
            "allowed_values",
            rules.get(column, {}).get("allowed_values", []),
        )
        if not allowed:
            return _skip("허용값 목록 미정의 (업무규칙 필요)")
        allowed_set = {str(v) for v in allowed}
        invalid_count = int(
            sum(1 for v in col_data if str(v) not in allowed_set)
        )

    # ---- email ---------------------------------------------------
    elif col_type == "email":
        pattern = re.compile(
            r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
        )
        invalid_count = int(
            sum(1 for v in col_data if not pattern.match(str(v).strip()))
        )

    # ---- 한국 전화번호 --------------------------------------------
    elif col_type == "phone_kr":
        pattern = re.compile(
            r"^(01[016789]\d{3,4}\d{4}|0\d{1,2}\d{3,4}\d{4})$"
        )
        invalid_count = int(
            sum(
                1
                for v in col_data
                if not pattern.match(re.sub(r"[-\s]", "", str(v)))
            )
        )

    # ---- date ---------------------------------------------------
    elif col_type == "date":
        fmt = column_config.get("format", "%Y-%m-%d")

        def _valid_date(v: Any) -> bool:
            try:
                datetime.strptime(str(v).strip(), fmt)
                return True
            except (ValueError, TypeError):
                return False

        invalid_count = int(sum(1 for v in col_data if not _valid_date(v)))

    # ---- 우편번호 (5자리 또는 NNN-NNN) ---------------------------
    elif col_type == "postcode":
        pattern = re.compile(r"^(\d{5}|\d{3}-\d{3})$")
        invalid_count = int(
            sum(1 for v in col_data if not pattern.match(str(v).strip()))
        )

    # ---- 시도코드 (2자리 숫자) ------------------------------------
    elif col_type == "sido_kr":
        pattern = re.compile(r"^\d{2}$")
        invalid_count = int(
            sum(1 for v in col_data if not pattern.match(str(v).strip()))
        )

    # ---- 시군구코드 (5자리 숫자) -----------------------------------
    elif col_type == "sigungu_kr":
        pattern = re.compile(r"^\d{5}$")
        invalid_count = int(
            sum(1 for v in col_data if not pattern.match(str(v).strip()))
        )

    # ---- 법정동코드 (10자리 숫자) ----------------------------------
    elif col_type == "beopjeongdong":
        pattern = re.compile(r"^\d{10}$")
        invalid_count = int(
            sum(1 for v in col_data if not pattern.match(str(v).strip()))
        )

    # ---- 행정구역코드 (10자리 숫자) --------------------------------
    elif col_type == "haengjeong":
        pattern = re.compile(r"^\d{10}$")
        invalid_count = int(
            sum(1 for v in col_data if not pattern.match(str(v).strip()))
        )

    # ---- 위도 (소수점 포함 십진수, -90.0 ~ 90.0) ------------------
    elif col_type == "latitude":
        pattern = re.compile(r"^-?\d{1,2}(\.\d+)?$")

        def _valid_lat(v: Any) -> bool:
            s = str(v).strip()
            if not pattern.match(s):
                return False
            try:
                return -90.0 <= float(s) <= 90.0
            except (ValueError, TypeError):
                return False

        invalid_count = int(sum(1 for v in col_data if not _valid_lat(v)))

    # ---- 경도 (소수점 포함 십진수, -180.0 ~ 180.0) ----------------
    elif col_type == "longitude":
        pattern = re.compile(r"^-?\d{1,3}(\.\d+)?$")

        def _valid_lon(v: Any) -> bool:
            s = str(v).strip()
            if not pattern.match(s):
                return False
            try:
                return -180.0 <= float(s) <= 180.0
            except (ValueError, TypeError):
                return False

        invalid_count = int(sum(1 for v in col_data if not _valid_lon(v)))

    else:
        reason = column_config.get("invalid_reason")
        if reason:
            return _skip(reason)
        return _skip(f"알 수 없는 컬럼 타입: {col_type!r}")

    error_rate = (invalid_count / total_non_blank) * 100
    score = 100 - error_rate

    return MetricResult(
        dimension=_DIMENSION,
        metric_name="구문정확성",
        scope="column",
        column=column,
        value=round(error_rate, 4),
        score=round(score, 4),
        status=EvalStatus.PASS,
        formula=f"{_FORMULA}; 점수={_SCORE_FORMULA}",
        detail=(
            f"오류율 {error_rate:.4f}% "
            f"({invalid_count:,}/{total_non_blank:,}건 오류), 점수 {score:.4f}"
        ),
        automation_level=2,
    )
