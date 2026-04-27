"""품질 평가 파이프라인

워크플로우
----------
1. 평가 리포트 객체 생성
2. 기계가독성 평가  (1) 기계가독형 포맷 → (2) 표준 인코딩 준수 → (3) 구조 정규성
   └─ 어느 단계에서든 score=0 이면 평가 중단
3. 테이블 단위 집계 평가
4. 컬럼 단위 평가
    - 자동화 수준 0: 결측률, 적시성 최신성
    - 자동화 수준 2: 구문정확성 (column_type_dict 정의 컬럼만, automation_level >= 2)
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional, Union

import pandas as pd

from .evaluators import acceptability as acc
from .evaluators import completeness as comp
from .evaluators import machine_readability as mr
from .evaluators import validity as val
from .evaluators.type_inferencer import describe_inferred, infer_column_type
from .report import EvalStatus, EvaluationReport


class QualityPipeline:
    """품질 평가 파이프라인.

    Parameters
    ----------
    data : str | pd.DataFrame
        평가 데이터.
        - str: 파일 경로 (CSV / XLSX / XLS) → 기계가독성 평가 포함
        - pd.DataFrame: 이미 로드된 데이터 → 기계가독성 평가 생략
    column_type_dict : dict, optional
        컬럼 타입 사전. 형식::

            {
                "gender": {
                    "type": "literal",
                    "allowed_values": ["M", "F"]
                },
                "created_date": {
                    "type": "date",
                    "format": "%Y-%m-%d",
                    "is_reference_date": True   # 적시성 평가 대상
                },
                "email": {"type": "email"},
                "phone": {"type": "phone_kr"},
            }

    master_data : dict, optional
        마스터데이터. {테이블명: pd.DataFrame} 형태 (자동화 수준 1 지표에 사용).
    business_rules : dict, optional
        업무규칙. {"컬럼명": {"allowed_values": [...]}} 형태.
        column_type_dict에 allowed_values 미정의 시 폴백으로 참조.
    automation_level : int, optional
        실행할 최대 자동화 수준 (0·1·2). 기본값: 0.
        현재 구현 범위: 0단계·2단계.
    """

    def __init__(
        self,
        data: Union[str, pd.DataFrame],
        column_type_dict: Optional[Dict] = None,
        master_data: Optional[Dict[str, pd.DataFrame]] = None,
        business_rules: Optional[Dict] = None,
        automation_level: int = 0,
        output_csv: bool = False,
    ):
        self.column_type_dict: Dict = column_type_dict or {}
        self.master_data: Dict[str, pd.DataFrame] = master_data or {}
        self.business_rules: Dict = business_rules or {}
        self.automation_level = automation_level
        self.output_csv = output_csv

        self._file_path: Optional[str] = None
        self._df: Optional[pd.DataFrame] = None

        if isinstance(data, str):
            self._file_path = data
            source = os.path.basename(data)
        elif isinstance(data, pd.DataFrame):
            self._df = data
            source = "DataFrame"
        else:
            raise TypeError("data must be a file path (str) or pd.DataFrame")

        # 평가 리포트 객체 생성
        self.report = EvaluationReport(source=source)

    # ------------------------------------------------------------------ #
    # 공개 인터페이스                                                        #
    # ------------------------------------------------------------------ #
    def run(self) -> EvaluationReport:
        """품질 평가 전체 실행."""

        # ① 기계가독성 평가
        if self._file_path is not None:
            passed = self._run_machine_readability()
            if not passed:
                return self.report
        else:
            # DataFrame 직접 입력 → 기계가독성 평가 생략
            self.report.machine_readability_passed = True

        if self._df is None:
            return self.report

        # ② 테이블 단위 집계 평가
        self._run_table_evaluation()

        # ③ 컬럼 단위 평가
        self._run_column_evaluation()

        # ④ CSV 저장
        if self.output_csv:
            saved = self.report.to_csv()
            print(f"결과 저장: {saved}")

        return self.report

    # ------------------------------------------------------------------ #
    # 내부 메서드                                                           #
    # ------------------------------------------------------------------ #
    def _run_machine_readability(self) -> bool:
        """기계가독성 3단계 평가. score=0 이면 False 반환."""
        path = self._file_path

        # (1) 기계가독형 포맷
        r_fmt = mr.check_format(path)
        self.report.add(r_fmt)
        if r_fmt.score == 0:
            self.report.failed_step = "기계가독형 포맷"
            return False

        # (2) 표준 인코딩 준수
        r_enc = mr.check_encoding(path)
        self.report.add(r_enc)
        if r_enc.score == 0:
            self.report.failed_step = "표준 인코딩 준수"
            return False

        # (3) 구조 정규성 — DataFrame 로드 포함
        r_struct, loaded_df = mr.check_structure(path)
        self.report.add(r_struct)
        if r_struct.score == 0:
            self.report.failed_step = "구조 정규성"
            return False

        self._df = loaded_df
        self.report.machine_readability_passed = True
        return True

    def _run_table_evaluation(self) -> None:
        """테이블 범위 지표 산출."""
        df = self._df

        # 완전성
        self.report.add(comp.missing_rate_table(df))
        self.report.add(comp.unused_column_rate(df))
        self.report.add(comp.column_duplicate_rate(df))
        self.report.add(comp.row_duplicate_rate(df))
        self.report.add(comp.candidate_key_rate(df))

        # 적시성 — 데이터기준시점 컬럼 여무
        ref_date_cols: List[str] = [
            col
            for col, cfg in self.column_type_dict.items()
            if cfg.get("is_reference_date")
        ]
        self.report.add(
            acc.timeliness_column_presence(
                df,
                reference_date_columns=ref_date_cols or None,
            )
        )

    def _resolve_column_config(self, column: str) -> Dict:
        """column_type_dict 우선, 없으면 자동 추론으로 컬럼 설정 반환."""
        explicit = self.column_type_dict.get(column)
        if explicit:
            return explicit
        inferred = infer_column_type(self._df, column)
        return inferred if inferred else {}

    def _run_column_evaluation(self) -> None:
        """컬럼 단위 평가 — 자동화 수준 0(→2) 순서."""
        df = self._df
        configured_ref_cols: List[str] = [
            col
            for col, cfg in self.column_type_dict.items()
            if cfg.get("is_reference_date")
        ]
        detected_ref_cols = set(
            acc.detect_reference_date_columns(
                df,
                reference_date_columns=configured_ref_cols or None,
            )
        )

        for column in df.columns:
            cfg = self._resolve_column_config(column)

            # ---- 자동화 수준 0 ----------------------------------------
            # 결측률 (컬럼)
            self.report.add(comp.missing_rate_column(df, column))

            # 적시성 최신성 — 기준시점 날짜 컬럼만 (조건부 수행)
            is_reference_col = cfg.get("is_reference_date") or column in detected_ref_cols
            if is_reference_col:
                freshness_cfg = dict(cfg)
                if not freshness_cfg.get("type"):
                    freshness_cfg["type"] = "date"
                result = acc.timeliness_freshness(df, column, freshness_cfg)
                if result is not None:
                    self.report.add(result)

            # ---- 구문정확성 -------------------------------------------
            # · explicit type이 있으면: automation_level >= 2 일 때만 실행
            # · 자동 추론 type이면: level 0 부터 실행 (외부 데이터 불필요)
            # · automation_level >= 2 이면 타입 미확정 컬럼도 NaN(SKIP)으로 기록
            is_inferred = cfg.get("inferred", False)
            has_type = bool(cfg.get("type"))

            should_run_validity = (has_type and is_inferred) or self.automation_level >= 2
            if should_run_validity:
                self.report.add(
                    val.syntactic_validity(
                        df, column, cfg, self.business_rules
                    )
                )
