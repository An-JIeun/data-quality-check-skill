#!/usr/bin/env python3
"""quality-check 스킬용 파이프라인 실행기

Usage:
    python run_check.py <file_path> [--col-dict JSON] [--automation-level N]

Output:
    JSON (stdout) - 품질 검사 결과
"""

import argparse
import json
import os
import sys

# quality_check 패키지 위치 탐색 (스킬이 어느 디렉토리에서 실행되든 동작)
_SEARCH_PATHS = [
    os.environ.get("QUALITY_CHECK_ROOT", ""),
    "/Users/anjieun/Documents/GitHub/quality-check",
    os.path.expanduser("~/Documents/GitHub/quality-check"),
]

for _p in _SEARCH_PATHS:
    if _p and os.path.isdir(os.path.join(_p, "quality_check")):
        sys.path.insert(0, _p)
        break
else:
    print(
        json.dumps(
            {"error": "quality_check 패키지를 찾을 수 없습니다. "
             "환경변수 QUALITY_CHECK_ROOT 를 설정하거나 README 를 확인하세요."}
        )
    )
    sys.exit(1)

from quality_check.pipeline import QualityPipeline  # noqa: E402
from quality_check.report import EvalStatus          # noqa: E402


def _score_grade(score) -> str:
    """점수 → 등급 문자열"""
    try:
        s = float(score)
    except (TypeError, ValueError):
        return "측정불가"
    if s >= 90:
        return "우수"
    if s >= 70:
        return "양호"
    if s >= 50:
        return "보통"
    return "미흡"


def _status_label(status: str) -> str:
    labels = {"PASS": "✅ PASS", "FAIL": "❌ FAIL", "WARNING": "⚠️ WARNING", "SKIP": "⏭️ SKIP"}
    return labels.get(status, status)


def run(file_path: str, column_type_dict: dict, automation_level: int) -> dict:
    pipeline = QualityPipeline(
        data=file_path,
        column_type_dict=column_type_dict,
        automation_level=automation_level,
    )
    report = pipeline.run()
    summary = report.summary()

    # 지표 상세 목록
    metrics = []
    for r in report.results:
        try:
            score_val = round(float(r.score), 2) if r.score is not None else None
        except (TypeError, ValueError):
            score_val = r.score

        try:
            value_val = round(float(r.value), 4) if r.value is not None else None
        except (TypeError, ValueError):
            value_val = r.value

        metrics.append({
            "dimension": r.dimension,
            "metric_name": r.metric_name,
            "scope": r.scope,
            "column": r.column,
            "value": value_val,
            "score": score_val,
            "status": r.status.value,
            "automation_level": r.automation_level,
            "detail": r.detail,
        })

    # 차원별 점수 + 등급
    dim_scores = {}
    for dim, info in summary["dimension_scores"].items():
        dim_scores[dim] = {
            "avg_score": round(info["avg_score"] * 100, 2) if info["avg_score"] <= 1 else round(info["avg_score"], 2),
            "count": info["count"],
            "grade": _score_grade(info["avg_score"] * 100 if info["avg_score"] <= 1 else info["avg_score"]),
        }

    # FAIL / WARNING 지표 요약 (정제 조언용)
    issues = [
        m for m in metrics
        if m["status"] in ("FAIL", "WARNING") and m["scope"] != "file"
    ]

    return {
        "file": os.path.basename(file_path),
        "machine_readability_passed": summary["machine_readability_passed"],
        "failed_step": summary["failed_step"],
        "total_metrics": summary["total_metrics"],
        "failed_count": summary["failed_count"],
        "warning_count": summary["warning_count"],
        "skipped_count": summary["skipped_count"],
        "dimension_scores": dim_scores,
        "metrics": metrics,
        "issues": issues,
    }


def main():
    parser = argparse.ArgumentParser(
        description="데이터 파일 품질 검사 — JSON 결과 출력 및 MD/TTL 리포트 저장"
    )
    parser.add_argument("file_path", help="검사할 파일 경로 (CSV/XLSX/XLS)")
    parser.add_argument("--col-dict", default="{}", help="컬럼 타입 사전 (JSON 문자열)")
    parser.add_argument("--automation-level", type=int, default=0,
                        help="자동화 수준 (0 or 2, 기본값: 0)")
    parser.add_argument("--output-dir", default=None,
                        help="MD/TTL 리포트 저장 디렉토리 (미지정 시 저장 안 함)")
    args = parser.parse_args()

    if not os.path.isfile(args.file_path):
        print(json.dumps({"error": f"파일을 찾을 수 없습니다: {args.file_path}"}))
        sys.exit(1)

    try:
        col_dict = json.loads(args.col_dict)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"컬럼 사전 JSON 파싱 오류: {e}"}))
        sys.exit(1)

    try:
        result = run(args.file_path, col_dict, args.automation_level)

        # MD / TTL 리포트 저장 (--output-dir 지정 시)
        if args.output_dir:
            _scripts_dir = os.path.dirname(os.path.abspath(__file__))
            sys.path.insert(0, _scripts_dir)
            from generate_report import save_reports
            paths = save_reports(result, args.output_dir)
            result["_reports"] = paths   # JSON 결과에 저장 경로 포함

        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
