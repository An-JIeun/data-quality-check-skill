"""컬럼 타입 자동 추론 모듈

column_type_dict 없이 아래 규칙으로 타입을 추론한다.

1) 컬럼명에 타입 키워드가 포함되어야 함
2) 해당 컬럼 샘플 값이 구문 규칙을 만족해야 함

둘 중 하나라도 불충족이면 타입 미확정(None)으로 처리한다.

추론 가능 타입:
  email           — 이메일 형식
  phone_kr        — 한국 전화번호 형식
  date            — 날짜/일시 형식 (포맷 자동 감지)
  postcode        — 우편번호 (5자리 또는 NNN-NNN)
  sido_kr         — 시도코드 (2자리 숫자)
  sigungu_kr      — 시군구코드 (5자리 숫자)
  beopjeongdong   — 법정동코드 (10자리 숫자)
  haengjeong      — 행정구역코드 (10자리 숫자)
  latitude        — 위도 (소수점 포함 십진수, -90.0 ~ 90.0)
  longitude       — 경도 (소수점 포함 십진수, -180.0 ~ 180.0)
  time            — 시각 (HH:MM:SS 형식, 00:00:00 ~ 23:59:59)
  url             — URL (http(s)://, ftp://, www. 등)
  year_month      — 연월 (YYYY-MM, YYYYMM, YYYY년MM월 등)
  year            — 연도 (YYYY, YYYY년 형식)
  literal         — 범주형 컬럼 (고유값 목록만 보고함, 오류율 산정 불가)

추론 우선순위:
    1. 컬럼명 키워드 일치  → 타입 후보 선정
    2. 값 패턴 일치율      → 후보 확인 / 기각
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ------------------------------------------------------------------ #
# 타입별 컬럼명 키워드                                                   #
# ------------------------------------------------------------------ #
_EMAIL_KW = {
    "이메일", "메일", "email", "mail", "e_mail",
}
_PHONE_KW = {
    "전화번호", "전화", "휴대폰", "핸드폰", "연락처",
    "휴대전화", "전화연락처", "phone", "tel", "mobile", "cellphone",
}
_DATE_KW = {
    "일자", "날짜", "일시", "date", "time",
    "기준일", "생년월일", "입사일", "퇴사일",
    "등록일", "수정일", "처리일", "시작일", "종료일",
    "발생일", "년월일", "연월일", "생성일", "변경일",
    "작성일", "기준연월", "기준년월",
}
_LITERAL_KW = {
    "성별", "구분", "여부", "코드", "유형", "상태",
    "gender", "flag", "type", "code", "status",
    "category", "분류", "구분코드", "유형코드", "종류",
}

_POSTCODE_KW = {
    "우편번호", "우편", "postal", "zipcode", "zip_code",
    "소재지우편번호", '도로명우편번호', '신우편번호', '구우편번호',
    "우편코드", "도로명주소우편번호"
}
_SIDO_KW = {
    "시도", "시도코드", "시도명", "광역시도", "sido",
    "시도구분코드", "광역시도코드",
}
_SIGUNGU_KW = {
    "시군구", "시군구코드", "시군구명", "sigungu",
    "시군구구분코드",
}
_BEOPJEONGDONG_KW = {
    "법정동코드", "법정동", "법정코드", "bj_cd", "bjdong_cd",
    "법정동번호", "법정행정코드"
}
_HAENGJEONG_KW = {
    "행정구역코드", "행정동코드", "행정코드", "adm_cd", "행정동번호",
    "행정구역번호", "행정구역", "행정동",
}
_LATITUDE_KW = {
    "위도", "lat", "latitude", "y좌표", "y_coord", "위도값",
}
_LONGITUDE_KW = {
    "경도", "lon", "lng", "longitude", "x좌표", "x_coord", "경도값",
}
_TIME_KW = {
    "시각", "시간", "time", "hour", "hh:mm:ss",
    "시:분:초", "time_of_day", "시간대", "시점",
}
_URL_KW = {
    "url", "웹주소", "링크", "address", "uri", "homepage",
    "website", "웹사이트", "사이트", "웹", "링크주소",
}
_YEAR_MONTH_KW = {
    "연월", "년월", "기준연월", "기준년월", "year_month",
    "month", "월", "년월일자",
}
_YEAR_KW = {
    "연도", "년", "year", "기준년", "회계연도",
}
# 날짜 포맷 후보 (시도 순서)
_DATE_FORMATS: List[str] = [
    "%Y-%m-%d",
    "%Y%m%d",
    "%Y/%m/%d",
    "%Y.%m.%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y%m%d%H%M%S",
    "%Y년 %m월 %d일",
    "%Y년%m월%d일",
    "%d/%m/%Y",
    "%m/%d/%Y",
]

# 이메일 패턴
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

# 한국 전화번호 패턴 (하이픈·공백 제거 후 적용)
_PHONE_RE = re.compile(
    r"^(01[016789]\d{3,4}\d{4}|0\d{1,2}\d{3,4}\d{4})$"
)

# 우편번호 패턴 (5자리 신형 또는 NNN-NNN 구형)
_POSTCODE_RE = re.compile(r"^(\d{5}|\d{3}-\d{3})$")

# 행정구역 관련 패턴
_SIDO_RE = re.compile(r"^\d{2}$")
_SIGUNGU_RE = re.compile(r"^\d{5}$")
_BEOPJEONGDONG_RE = re.compile(r"^\d{10}$")
_HAENGJEONG_RE = re.compile(r"^\d{10}$")

# 위도/경도 패턴 (소수점 포함 십진수)
_LATITUDE_RE = re.compile(r"^-?\d{1,2}(\.\d+)?$")
_LONGITUDE_RE = re.compile(r"^-?\d{1,3}(\.\d+)?$")

# 시각 패턴 (HH:MM:SS, 00:00:00 ~ 23:59:59)
_TIME_RE = re.compile(r"^([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$")

# URL 패턴 (http(s), ftp, www 시작)
_URL_RE = re.compile(
    r"^(https?|ftp)://[^\s/$.?#].[^\s]*$|^www\.[^\s]*$",
    re.IGNORECASE
)

# 연월 패턴 (YYYY-MM, YYYYMM, YYYY년MM월 등)
_YEAR_MONTH_RE = re.compile(
    r"^(\d{4})[-/]?(0[1-9]|1[0-2])$|^(\d{4})년\s*(0[1-9]|1[0-2])월$"
)

# 연도 패턴 (YYYY, YYYY년)
_YEAR_RE = re.compile(r"^(\d{4})(년)?$")



# 최대 샘플 수
_SAMPLE_SIZE = 200
# 타입 확정 임계값
_THRESHOLD_NAME_AND_PATTERN = 0.30   # 이름 일치 + 패턴 비율 ≥ 30 %
_MAX_LITERAL_CARDINALITY    = 20     # 범주형으로 판단할 최대 고유값 수


# ------------------------------------------------------------------ #
# 내부 헬퍼                                                             #
# ------------------------------------------------------------------ #
def _name_matches(col_lower: str, keywords: set) -> bool:
    """키워드 중 하나라도 컬럼명 부분 문자열로 포함되면 True."""
    return any(kw in col_lower for kw in keywords)


def _sample_nonnull(series: pd.Series, n: int = _SAMPLE_SIZE) -> pd.Series:
    """비공백 값만 최대 n 개 샘플링."""
    s = series.dropna()
    s = s[s.astype(str).str.strip() != ""]
    if len(s) > n:
        s = s.sample(n, random_state=42)
    return s


def _email_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _EMAIL_RE.match(str(v).strip()))
    return matched / len(sample)


def _phone_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(
        1 for v in sample if _PHONE_RE.match(re.sub(r"[-\s]", "", str(v)))
    )
    return matched / len(sample)


def _postcode_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _POSTCODE_RE.match(str(v).strip()))
    return matched / len(sample)


def _sido_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _SIDO_RE.match(str(v).strip()))
    return matched / len(sample)


def _sigungu_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _SIGUNGU_RE.match(str(v).strip()))
    return matched / len(sample)


def _beopjeongdong_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _BEOPJEONGDONG_RE.match(str(v).strip()))
    return matched / len(sample)


def _haengjeong_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _HAENGJEONG_RE.match(str(v).strip()))
    return matched / len(sample)


def _latitude_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0

    def _ok(v: Any) -> bool:
        s = str(v).strip()
        if not _LATITUDE_RE.match(s):
            return False
        try:
            return -90.0 <= float(s) <= 90.0
        except (ValueError, TypeError):
            return False

    matched = sum(1 for v in sample if _ok(v))
    return matched / len(sample)


def _longitude_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0

    def _ok(v: Any) -> bool:
        s = str(v).strip()
        if not _LONGITUDE_RE.match(s):
            return False
        try:
            return -180.0 <= float(s) <= 180.0
        except (ValueError, TypeError):
            return False

    matched = sum(1 for v in sample if _ok(v))
    return matched / len(sample)


def _time_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _TIME_RE.match(str(v).strip()))
    return matched / len(sample)


def _url_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _URL_RE.match(str(v).strip()))
    return matched / len(sample)


def _year_month_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _YEAR_MONTH_RE.match(str(v).strip()))
    return matched / len(sample)


def _year_match_rate(sample: pd.Series) -> float:
    if len(sample) == 0:
        return 0.0
    matched = sum(1 for v in sample if _YEAR_RE.match(str(v).strip()))
    return matched / len(sample)


def _detect_date_format(sample: pd.Series) -> Tuple[Optional[str], float]:
    """가장 높은 성공률의 포맷과 그 비율을 반환."""
    from datetime import datetime

    if len(sample) == 0:
        return None, 0.0

    best_fmt, best_rate = None, 0.0
    for fmt in _DATE_FORMATS:
        ok = 0
        for v in sample:
            try:
                datetime.strptime(str(v).strip(), fmt)
                ok += 1
            except (ValueError, TypeError):
                pass
        rate = ok / len(sample)
        if rate > best_rate:
            best_rate = rate
            best_fmt = fmt
    return best_fmt, best_rate


# ------------------------------------------------------------------ #
# 공개 인터페이스                                                        #
# ------------------------------------------------------------------ #
def infer_column_type(
    df: pd.DataFrame,
    column: str,
) -> Optional[Dict[str, Any]]:
    """컬럼명 + 값 패턴으로 타입을 추론한다.

    Returns
    -------
    dict | None
        타입 추론 성공 시 column_type_dict 형식의 dict:

        email    : {"type": "email", "inferred": True, "confidence": ..., "basis": ...}
        phone_kr : {"type": "phone_kr", ...}
        date     : {"type": "date", "format": "%Y-%m-%d", ...}
        literal  : {"type": "literal", "allowed_values": [...], "inferred": True,
                    "auto_allowed_values": True, ...}

        추론 실패 시 None.
    """
    col_lower = column.lower()
    series = df[column]
    sample = _sample_nonnull(series)

    if len(sample) == 0:
        return {
            "type": None,
            "inferred": True,
            "basis": "none",
            "invalid_reason": "비공백 샘플 없음",
        }

    keyword_matched = False

    # ---- 1. email ------------------------------------------------
    name_email = _name_matches(col_lower, _EMAIL_KW)
    keyword_matched = keyword_matched or name_email
    rate_email = _email_match_rate(sample)
    if name_email and rate_email >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "email",
            "inferred": True,
            "confidence": "high" if rate_email >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_email, 3),
        }

    # ---- 2. phone_kr ---------------------------------------------
    name_phone = _name_matches(col_lower, _PHONE_KW)
    keyword_matched = keyword_matched or name_phone
    rate_phone = _phone_match_rate(sample)
    if name_phone and rate_phone >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "phone_kr",
            "inferred": True,
            "confidence": "high" if rate_phone >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_phone, 3),
        }

    # ---- 3. date -------------------------------------------------
    name_date = _name_matches(col_lower, _DATE_KW)
    keyword_matched = keyword_matched or name_date
    fmt, rate_date = _detect_date_format(sample)
    if name_date and rate_date >= _THRESHOLD_NAME_AND_PATTERN and fmt:
        return {
            "type": "date",
            "format": fmt,
            "inferred": True,
            "confidence": "high" if rate_date >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_date, 3),
        }

    # ---- 4. postcode ---------------------------------------------
    name_postcode = _name_matches(col_lower, _POSTCODE_KW)
    keyword_matched = keyword_matched or name_postcode
    rate_postcode = _postcode_match_rate(sample)
    if name_postcode and rate_postcode >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "postcode",
            "inferred": True,
            "confidence": "high" if rate_postcode >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_postcode, 3),
        }

    # ---- 5. sido_kr (시도코드) ------------------------------------
    name_sido = _name_matches(col_lower, _SIDO_KW)
    keyword_matched = keyword_matched or name_sido
    rate_sido = _sido_match_rate(sample)
    if name_sido and rate_sido >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "sido_kr",
            "inferred": True,
            "confidence": "high" if rate_sido >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_sido, 3),
        }

    # ---- 6. sigungu_kr (시군구코드) --------------------------------
    name_sigungu = _name_matches(col_lower, _SIGUNGU_KW)
    keyword_matched = keyword_matched or name_sigungu
    rate_sigungu = _sigungu_match_rate(sample)
    if name_sigungu and rate_sigungu >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "sigungu_kr",
            "inferred": True,
            "confidence": "high" if rate_sigungu >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_sigungu, 3),
        }

    # ---- 7. beopjeongdong (법정동코드) / haengjeong (행정구역코드) ----
    # 이름 키워드로 먼저 구분하고, 이름 불일치 시 패턴만으로는 두 타입이 동일하므로
    # beopjeongdong을 우선 적용한다.
    name_beopjeong = _name_matches(col_lower, _BEOPJEONGDONG_KW)
    name_haengjeong = _name_matches(col_lower, _HAENGJEONG_KW)
    keyword_matched = keyword_matched or name_beopjeong or name_haengjeong
    rate_10digit = _beopjeongdong_match_rate(sample)  # 패턴 동일

    if name_beopjeong:
        thr = _THRESHOLD_NAME_AND_PATTERN
        if rate_10digit >= thr:
            return {
                "type": "beopjeongdong",
                "inferred": True,
                "confidence": "high" if rate_10digit >= 0.7 else "medium",
                "basis": "name+pattern",
                "pattern_match_rate": round(rate_10digit, 3),
            }
    elif name_haengjeong:
        thr = _THRESHOLD_NAME_AND_PATTERN
        if rate_10digit >= thr:
            return {
                "type": "haengjeong",
                "inferred": True,
                "confidence": "high" if rate_10digit >= 0.7 else "medium",
                "basis": "name+pattern",
                "pattern_match_rate": round(rate_10digit, 3),
            }

    # ---- 8. latitude (위도) --------------------------------------
    name_lat = _name_matches(col_lower, _LATITUDE_KW)
    keyword_matched = keyword_matched or name_lat
    rate_lat = _latitude_match_rate(sample)
    if name_lat and rate_lat >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "latitude",
            "inferred": True,
            "confidence": "high" if rate_lat >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_lat, 3),
        }

    # ---- 9. longitude (경도) --------------------------------------
    name_lon = _name_matches(col_lower, _LONGITUDE_KW)
    keyword_matched = keyword_matched or name_lon
    rate_lon = _longitude_match_rate(sample)
    if name_lon and rate_lon >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "longitude",
            "inferred": True,
            "confidence": "high" if rate_lon >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_lon, 3),
        }

    # ---- 10. time (시각) -----------------------------------------------
    name_time = _name_matches(col_lower, _TIME_KW)
    keyword_matched = keyword_matched or name_time
    rate_time = _time_match_rate(sample)
    if name_time and rate_time >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "time",
            "inferred": True,
            "confidence": "high" if rate_time >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_time, 3),
        }

    # ---- 11. url (URL) -----------------------------------------------
    name_url = _name_matches(col_lower, _URL_KW)
    keyword_matched = keyword_matched or name_url
    rate_url = _url_match_rate(sample)
    if name_url and rate_url >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "url",
            "inferred": True,
            "confidence": "high" if rate_url >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_url, 3),
        }

    # ---- 12. year_month (연월) ----------------------------------------
    name_year_month = _name_matches(col_lower, _YEAR_MONTH_KW)
    keyword_matched = keyword_matched or name_year_month
    rate_year_month = _year_month_match_rate(sample)
    if name_year_month and rate_year_month >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "year_month",
            "inferred": True,
            "confidence": "high" if rate_year_month >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_year_month, 3),
        }

    # ---- 13. year (연도) ----------------------------------------------
    name_year = _name_matches(col_lower, _YEAR_KW)
    keyword_matched = keyword_matched or name_year
    rate_year = _year_match_rate(sample)
    if name_year and rate_year >= _THRESHOLD_NAME_AND_PATTERN:
        return {
            "type": "year",
            "inferred": True,
            "confidence": "high" if rate_year >= 0.7 else "medium",
            "basis": "name+pattern",
            "pattern_match_rate": round(rate_year, 3),
        }

    # ---- 14. literal (범주형) ------------------------------------
    # 이름 일치 + 저카디널리티: 고유값만 보고, 오류율 산정 불가
    name_literal = _name_matches(col_lower, _LITERAL_KW)
    keyword_matched = keyword_matched or name_literal
    n_unique = series.dropna().nunique()
    is_low_cardinality = (
        1 < n_unique <= _MAX_LITERAL_CARDINALITY
        and not pd.api.types.is_numeric_dtype(series)
    )

    if name_literal and is_low_cardinality:
        unique_vals = [
            str(v) for v in series.dropna().unique() if str(v).strip() != ""
        ]
        return {
            "type": "literal",
            "allowed_values": unique_vals,
            "inferred": True,
            "auto_allowed_values": True,   # 관측값으로 자동 생성 — 오류율 산정 불가
            "confidence": "high",
            "basis": "name+low_cardinality",
        }

    if not keyword_matched:
        return {
            "type": None,
            "inferred": True,
            "basis": "none",
            "invalid_reason": "컬럼명 키워드 미포함",
        }

    return {
        "type": None,
        "inferred": True,
        "basis": "name_only",
        "invalid_reason": "키워드 포함 컬럼이지만 샘플 구문규칙 불일치",
    }


def describe_inferred(cfg: Dict[str, Any]) -> str:
    """추론 결과를 사람이 읽기 쉬운 문자열로 변환."""
    if not cfg.get("type"):
        reason = cfg.get("invalid_reason", "타입 미확정")
        return f"자동추론: NaN [{reason}]"

    t = cfg.get("type", "?")
    basis = cfg.get("basis", "")
    conf = cfg.get("confidence", "")
    rate = cfg.get("pattern_match_rate")
    rate_str = f", 패턴일치율 {rate:.0%}" if rate is not None else ""
    fmt = cfg.get("format", "")
    fmt_str = f" ({fmt})" if fmt else ""
    return f"자동추론: {t}{fmt_str} [{basis}, {conf}{rate_str}]"
