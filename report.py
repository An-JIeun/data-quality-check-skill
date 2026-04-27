"""품질 평가 리포트 모델"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Optional

import pandas as pd


class EvalStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"
    WARNING = "WARNING"


@dataclass
class MetricResult:
    dimension: str          # 완전성 / 정확성 / 적시성 / 기계가독성
    metric_name: str        # e.g. 결측률, 미사용률, 기계가독형 포맷
    scope: str              # file / table / column
    column: Optional[str] = None
    value: Optional[float] = None   # 원시 측정값
    score: Optional[Any] = None     # 점수 (정규화 또는 등급)
    status: EvalStatus = EvalStatus.PASS
    formula: str = ""
    detail: str = ""
    automation_level: int = 0       # 0 / 1 / 2


class EvaluationReport:
    """품질 평가 결과를 수집·조회하는 리포트 객체"""

    def __init__(self, source: str = ""):
        self.source = source
        self.results: List[MetricResult] = []
        self.machine_readability_passed: bool = False
        self.failed_step: Optional[str] = None  # 기계가독성 실패 단계

    # ------------------------------------------------------------------ #
    # 결과 추가                                                             #
    # ------------------------------------------------------------------ #
    def add(self, result: MetricResult) -> None:
        self.results.append(result)

    # ------------------------------------------------------------------ #
    # 조회                                                                  #
    # ------------------------------------------------------------------ #
    def file_results(self) -> List[MetricResult]:
        return [r for r in self.results if r.scope == "file"]

    def table_results(self) -> List[MetricResult]:
        return [r for r in self.results if r.scope == "table"]

    def column_results(self, column: Optional[str] = None) -> List[MetricResult]:
        if column:
            return [r for r in self.results if r.scope == "column" and r.column == column]
        return [r for r in self.results if r.scope == "column"]

    # ------------------------------------------------------------------ #
    # 출력                                                                  #
    # ------------------------------------------------------------------ #
    def to_dataframe(self) -> pd.DataFrame:
        records = [
            {
                "평가 차원": r.dimension,
                "지표명": r.metric_name,
                "범위": r.scope,
                "컬럼": r.column or "-",
                "측정값": r.value,
                "점수": r.score,
                "상태": r.status.value,
                "자동화 수준": r.automation_level,
                "비고": r.detail,
            }
            for r in self.results
        ]
        return pd.DataFrame(records)

    def to_csv(self, *, encoding: str = "utf-8-sig") -> str:
        """평가 결과를 ``report/report_{source}.csv`` 로 저장한다.

        Returns
        -------
        str
            실제로 저장된 파일 경로.
        """
        base = os.path.splitext(self.source)[0] if self.source else "report"
        os.makedirs("report", exist_ok=True)
        path = os.path.join("report", f"report_{base}.csv")

        df = self.to_dataframe()

        # 차원별 총점 요약 행 추가
        dim_scores = self.dimension_scores()
        if dim_scores:
            summary_rows = [
                {
                    "평가 차원": dim,
                    "지표명": "【차원 총점】",
                    "범위": "-",
                    "컬럼": "-",
                    "측정값": info["avg_score"],
                    "점수": info["avg_score"],
                    "상태": f"지표 {info['count']}건 평균",
                    "자동화 수준": "-",
                    "비고": info.get("note", "SKIP 제외 평균"),
                }
                for dim, info in dim_scores.items()
            ]
            df = pd.concat([df, pd.DataFrame(summary_rows)], ignore_index=True)

        df.to_csv(path, index=False, encoding=encoding)
        return path

    def dimension_scores(self) -> dict:
        """차원별 평균 점수를 반환한다.

        - SKIP 결과 및 score=None 결과는 제외한다.
        - score 가 숫자가 아닌 결과(기계가독성 등급 등)도 제외한다.
        - 반환값: {차원명: {"avg_score": float, "count": int}}
        """
        from collections import defaultdict

        buckets: dict = defaultdict(list)
        timeliness_presence: Optional[float] = None
        timeliness_freshness: list[float] = []

        for r in self.results:
            if r.status == EvalStatus.SKIP:
                continue
            try:
                val = float(r.score)
            except (TypeError, ValueError):
                continue

            if r.dimension == "적시성":
                if r.metric_name == "데이터기준시점 컬럼 여무":
                    timeliness_presence = val
                elif r.metric_name == "데이터 기준 시점 최신성":
                    timeliness_freshness.append(val)

            buckets[r.dimension].append(val)

        scores = {
            dim: {"avg_score": round(sum(vals) / len(vals), 4), "count": len(vals)}
            for dim, vals in buckets.items()
        }

        # 적시성 총점은 컬럼 유무(0/1)와 최신성 점수를 곱해 산출한다.
        if timeliness_presence is not None:
            freshness_avg = (
                sum(timeliness_freshness) / len(timeliness_freshness)
                if timeliness_freshness
                else 0.0
            )
            timeliness_score = freshness_avg * timeliness_presence
            scores["적시성"] = {
                "avg_score": round(timeliness_score, 4),
                "count": 1 + len(timeliness_freshness),
                "note": "최신성 × 기준일자 컬럼 유무(0/1)",
            }

        return scores

    def summary(self) -> dict:
        failed = [r for r in self.results if r.status == EvalStatus.FAIL]
        skipped = [r for r in self.results if r.status == EvalStatus.SKIP]
        warnings = [r for r in self.results if r.status == EvalStatus.WARNING]
        return {
            "source": self.source,
            "machine_readability_passed": self.machine_readability_passed,
            "failed_step": self.failed_step,
            "total_metrics": len(self.results),
            "failed_count": len(failed),
            "warning_count": len(warnings),
            "skipped_count": len(skipped),
            "dimension_scores": self.dimension_scores(),
        }

    def print_summary(self) -> None:
        s = self.summary()
        divider = "=" * 50
        print(divider)
        print("품질 평가 리포트")
        print(divider)
        print(f"소스            : {s['source']}")
        print(f"기계가독성 통과 : {s['machine_readability_passed']}")
        if s["failed_step"]:
            print(f"실패 단계       : {s['failed_step']}")
        print(f"총 지표 수      : {s['total_metrics']}")
        print(f"  FAIL          : {s['failed_count']}")
        print(f"  WARNING       : {s['warning_count']}")
        print(f"  SKIP          : {s['skipped_count']}")
        print(divider)
        print("차원별 총점 (평균 점수, SKIP 제외)")
        dim_scores = s["dimension_scores"]
        if dim_scores:
            for dim, info in dim_scores.items():
                print(f"  {dim:<12} : {info['avg_score']:.4f}  (지표 {info['count']}건)")
        else:
            print("  (산출 가능한 점수 없음)")
        print(divider)
        df = self.to_dataframe()
        if not df.empty:
            with pd.option_context("display.max_colwidth", 60, "display.width", 120):
                print(df.to_string(index=False))
        print(divider)
