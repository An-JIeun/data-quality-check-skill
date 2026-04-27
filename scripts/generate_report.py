#!/usr/bin/env python3
"""품질 검사 결과를 MD(사람 확인용) / TTL(DQV 기계가독용) 파일로 저장

Usage:
    # run_check.py 결과를 파이프로 연결
    python run_check.py data.csv | python generate_report.py - --output-dir ./qc_reports

    # 저장된 JSON 파일 사용
    python generate_report.py result.json --output-dir ./qc_reports
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Optional

# ── 네임스페이스 ──────────────────────────────────────────────────────
QC_NS = "https://github.com/anjieun/quality-check/vocab#"

# ── 차원 가중치 ─────────────────────────────────────────────────────
DIMENSION_WEIGHTS = {
    "완전성":    0.35,
    "정확성":    0.35,
    "적시성":    0.15,
    "기계가독성": 0.15,
}

# 차원명 → 영문 (URI용)
DIM_EN = {
    "완전성":    "completeness",
    "정확성":    "accuracy",
    "적시성":    "timeliness",
    "기계가독성": "machineReadability",
}

# ── 메트릭 어휘 (프레임워크 도식화 반영) ────────────────────────────────
#   평가 범위  : dataset | table | column | file
#   검증 유형  : 오류율 | 비율 | 순서척도
#   게이트 여부: True → 0점이면 평가 중단
METRIC_VOCAB: dict[str, dict] = {
    "기계가독형 포맷": dict(
        id="MachineReadableFormat", dimension="기계가독성",
        assessment_scope="file",   automation_level=0,
        validation_type="순서척도", is_gate=True,
        column_types=[],           data_format="file",
        assessment_level="quality_ready",
    ),
    "표준 인코딩 준수": dict(
        id="StandardEncoding",     dimension="기계가독성",
        assessment_scope="file",   automation_level=0,
        validation_type="순서척도", is_gate=True,
        column_types=[],           data_format="file",
        assessment_level="quality_ready",
    ),
    "구조 정규성": dict(
        id="StructuralRegularity", dimension="기계가독성",
        assessment_scope="file",   automation_level=0,
        validation_type="순서척도", is_gate=True,
        column_types=[],           data_format="file",
        assessment_level="quality_ready",
    ),
    "결측률_table": dict(
        id="MissingRateTable",     dimension="완전성",
        assessment_scope="table",  automation_level=0,
        validation_type="비율",    is_gate=False,
        column_types=[],           data_format="table",
        assessment_level="quality_ready",
    ),
    "미사용률": dict(
        id="UnusedColumnRate",     dimension="완전성",
        assessment_scope="table",  automation_level=0,
        validation_type="비율",    is_gate=False,
        column_types=[],           data_format="table",
        assessment_level="quality_ready",
    ),
    "중복률 (컬럼 일관성)": dict(
        id="ColumnDuplicateRate",  dimension="완전성",
        assessment_scope="table",  automation_level=0,
        validation_type="비율",    is_gate=False,
        column_types=[],           data_format="table",
        assessment_level="quality_ready",
    ),
    "중복률 (값 일관성)": dict(
        id="RowDuplicateRate",     dimension="완전성",
        assessment_scope="table",  automation_level=0,
        validation_type="비율",    is_gate=False,
        column_types=[],           data_format="table",
        assessment_level="quality_ready",
    ),
    "후보키 비율": dict(
        id="CandidateKeyRate",     dimension="완전성",
        assessment_scope="table",  automation_level=0,
        validation_type="비율",    is_gate=False,
        column_types=[],           data_format="table",
        assessment_level="quality_ready",
    ),
    "결측률_column": dict(
        id="MissingRateColumn",    dimension="완전성",
        assessment_scope="column", automation_level=0,
        validation_type="비율",    is_gate=False,
        column_types=[],           data_format="table",
        assessment_level="quality_ready",
    ),
    "데이터기준시점 컬럼 여무": dict(
        id="ReferenceDataPresence", dimension="적시성",
        assessment_scope="table",   automation_level=0,
        validation_type="비율",     is_gate=False,
        column_types=["date"],      data_format="table",
        assessment_level="quality_ready",
    ),
    "데이터 기준 시점 최신성": dict(
        id="DataFreshness",         dimension="적시성",
        assessment_scope="column",  automation_level=0,
        validation_type="비율",     is_gate=False,
        column_types=["date"],      data_format="table",
        assessment_level="quality_ready",
    ),
    "구문정확성": dict(
        id="SyntacticValidity",     dimension="정확성",
        assessment_scope="column",  automation_level=2,
        validation_type="오류율",   is_gate=False,
        column_types=["email", "phone_kr", "date", "postcode",
                      "latitude", "longitude", "literal"],
        data_format="table",
        assessment_level="quality_ready",
    ),
}


# ── 유틸 ──────────────────────────────────────────────────────────────

def _slugify(text: str) -> str:
    """문자열을 ASCII URI-safe 슬러그로 변환 (한글 포함 그대로 유지)"""
    text = re.sub(r'[^\w가-힣\-]', '_', str(text))
    return re.sub(r'_+', '_', text).strip('_')


def _metric_key(name: str, scope: str) -> str:
    """어휘 사전 키 반환 (결측률은 scope로 구분)"""
    if name == "결측률":
        return "결측률_table" if scope in ("table", "file") else "결측률_column"
    return name


def _metric_id(name: str, scope: str) -> str:
    """메트릭 URI local name 반환"""
    vocab = METRIC_VOCAB.get(_metric_key(name, scope), {})
    return vocab.get("id") or _slugify(name)


def _score_label(score) -> str:
    try:
        s = float(score)
    except (TypeError, ValueError):
        return "측정불가"
    return "🟢 우수" if s >= 90 else "🟡 양호" if s >= 70 else "🟠 보통" if s >= 50 else "🔴 미흡"


def _status_icon(status: str) -> str:
    return {"PASS": "✅", "FAIL": "❌", "WARNING": "⚠️", "SKIP": "⏭️"}.get(status, status)


def _fmt_val(value, scope: str) -> str:
    """원시 측정값 포맷 (비율이면 % 표시)"""
    if value is None:
        return "—"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    # 소수 0~1 범위가 아닌 퍼센트 값이면 % 붙이기
    if v > 1.0 or scope in ("table", "column"):
        return f"{v:.2f}%"
    return f"{v:.4f}"


def _esc(text: str) -> str:
    """TTL 문자열 이스케이프"""
    return str(text).replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ')


# ════════════════════════════════════════════════════════════════════
#  마크다운 생성
# ════════════════════════════════════════════════════════════════════

def generate_md(result: dict) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_name = result["file"]
    dim_scores = result.get("dimension_scores", {})
    metrics = result.get("metrics", [])
    issues = result.get("issues", [])

    L: list[str] = []

    def row(*cols):
        L.append("| " + " | ".join(str(c) for c in cols) + " |")

    def hr(n: int):
        L.append("|" + "|".join(["---"] * n) + "|")

    # ── 헤더 ──────────────────────────────────────────────────────
    L.append(f"# 📊 품질 검사 리포트: `{file_name}`")
    L.append(f"\n> 검사 일시: {now}  |  생성 도구: quality-check skill\n")

    # ── 1. 개요 ───────────────────────────────────────────────────
    L.append("## 1. 개요\n")
    row("항목", "값");                 hr(2)
    row("파일명",      f"`{file_name}`")
    row("기계가독성 통과",
        "✅ 통과" if result["machine_readability_passed"] else "❌ 실패")
    if result.get("failed_step"):
        row("평가 중단 단계", result["failed_step"])
    row("총 지표 수",  result["total_metrics"])
    row("FAIL",       result["failed_count"])
    row("WARNING",    result["warning_count"])
    row("SKIP",       result["skipped_count"])
    L.append("")

    # ── 2. 차원별 점수 ─────────────────────────────────────────────
    L.append("## 2. 차원별 점수\n")
    row("평가 차원", "가중치", "점수", "등급", "지표 수"); hr(5)

    weighted_sum = 0.0
    for dim in ["완전성", "정확성", "적시성", "기계가독성"]:
        w = DIMENSION_WEIGHTS[dim]
        info = dim_scores.get(dim)
        if info:
            s = info["avg_score"]
            weighted_sum += s * w
            row(dim, f"{w*100:.0f}%", f"**{s:.1f}**", _score_label(s), info["count"])
        else:
            row(dim, f"{w*100:.0f}%", "—", "측정 안됨", 0)

    row("**종합 (가중평균)**", "100%",
        f"**{weighted_sum:.1f}**", _score_label(weighted_sum), "—")
    L.append("")

    # ── 3. 세부 지표 ───────────────────────────────────────────────
    L.append("## 3. 세부 지표\n")
    DIM_ORDER = ["기계가독성", "완전성", "정확성", "적시성"]
    for idx, dim in enumerate(DIM_ORDER, 1):
        subset = [m for m in metrics if m["dimension"] == dim]
        if not subset:
            continue
        L.append(f"### 3-{idx}. {dim}\n")
        if dim == "기계가독성":
            row("지표명", "점수", "상태", "비고"); hr(4)
            for m in subset:
                sc = f"{m['score']:.1f}" if m["score"] is not None else "—"
                row(m["metric_name"], sc,
                    f"{_status_icon(m['status'])} {m['status']}", m["detail"])
        else:
            row("범위", "지표명", "컬럼", "측정값", "점수", "상태", "비고"); hr(7)
            for m in subset:
                col  = m["column"] or "—"
                val  = _fmt_val(m["value"], m["scope"])
                sc   = f"{m['score']:.1f}" if m["score"] is not None else "—"
                det  = m["detail"]
                det  = det[:60] + "…" if len(det) > 60 else det
                row(m["scope"], m["metric_name"], col, val, sc,
                    f"{_status_icon(m['status'])} {m['status']}", det)
        L.append("")

    # ── 4. 문제 지표 요약 ──────────────────────────────────────────
    L.append("## 4. 문제 지표 요약 (FAIL / WARNING)\n")
    if issues:
        row("평가 차원", "지표명", "컬럼", "측정값", "점수", "상태", "세부 내용"); hr(7)
        for m in issues:
            col = m["column"] or "—"
            val = _fmt_val(m["value"], m["scope"])
            sc  = f"{m['score']:.1f}" if m["score"] is not None else "—"
            row(m["dimension"], m["metric_name"], col, val, sc,
                f"{_status_icon(m['status'])} {m['status']}", m["detail"])
    else:
        L.append("> ✅ 검출된 품질 문제 없음 — 모든 지표가 양호합니다.")
    L.append("")

    return "\n".join(L)


# ════════════════════════════════════════════════════════════════════
#  TTL (DQV) 생성
# ════════════════════════════════════════════════════════════════════

_PREFIXES = f"""\
@prefix dqv:     <http://www.w3.org/ns/dqv#> .
@prefix dcat:    <http://www.w3.org/ns/dcat#> .
@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos:    <http://www.w3.org/2004/02/skos/core#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix oa:      <http://www.w3.org/ns/oa#> .
@prefix qc:      <{QC_NS}> .
"""

# 프레임워크 어휘 고정 블록 (카테고리 / 차원 / 메트릭 정의)
_VOCAB_BLOCK = """
# ════════════════════════════════════════════════════════
#  어휘 정의 — 카테고리 · 차원 · 메트릭
#  (프레임워크 도식화: Category → Dimension ↔ Metric)
# ════════════════════════════════════════════════════════

# ── 카테고리 ──────────────────────────────────────────
qc:DataQuality a dqv:Category ;
    skos:prefLabel "데이터 품질"@ko, "Data Quality"@en .

# ── 차원 (Dimension) ──────────────────────────────────
#   qc:assessmentLevel  : quality_ready | …
#   qc:dataFormat       : table | file | …

qc:완전성 a dqv:Dimension ;
    skos:prefLabel "완전성"@ko, "Completeness"@en ;
    dqv:inCategory qc:DataQuality ;
    qc:assessmentLevel "quality_ready" ;
    qc:dataFormat "table" .

qc:정확성 a dqv:Dimension ;
    skos:prefLabel "정확성"@ko, "Accuracy"@en ;
    dqv:inCategory qc:DataQuality ;
    qc:assessmentLevel "quality_ready" ;
    qc:dataFormat "table" .

qc:적시성 a dqv:Dimension ;
    skos:prefLabel "적시성"@ko, "Timeliness"@en ;
    dqv:inCategory qc:DataQuality ;
    qc:assessmentLevel "quality_ready" ;
    qc:dataFormat "table" .

qc:기계가독성 a dqv:Dimension ;
    skos:prefLabel "기계가독성"@ko, "Machine Readability"@en ;
    dqv:inCategory qc:DataQuality ;
    qc:assessmentLevel "quality_ready" ;
    qc:dataFormat "file" .

# ── 메트릭 (Metric) ───────────────────────────────────
#   qc:assessmentScope   : dataset | table | column | file
#   qc:automationLevel   : 0 | 1 | 2
#   qc:validationType    : 오류율 | 비율 | 순서척도
#   qc:isGate            : true → 0점이면 이후 평가 중단
#   qc:columnTypes       : 적용 가능한 컬럼 타입 목록

qc:MachineReadableFormat a dqv:Metric ;
    skos:prefLabel "기계가독형 포맷"@ko, "Machine-readable Format"@en ;
    dqv:inDimension qc:기계가독성 ;
    qc:assessmentScope "file" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "순서척도" ;
    qc:isGate "true"^^xsd:boolean .

qc:StandardEncoding a dqv:Metric ;
    skos:prefLabel "표준 인코딩 준수"@ko, "Standard Encoding"@en ;
    dqv:inDimension qc:기계가독성 ;
    qc:assessmentScope "file" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "순서척도" ;
    qc:isGate "true"^^xsd:boolean .

qc:StructuralRegularity a dqv:Metric ;
    skos:prefLabel "구조 정규성"@ko, "Structural Regularity"@en ;
    dqv:inDimension qc:기계가독성 ;
    qc:assessmentScope "file" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "순서척도" ;
    qc:isGate "true"^^xsd:boolean .

qc:MissingRateTable a dqv:Metric ;
    skos:prefLabel "결측률 (테이블)"@ko, "Missing Rate (Table)"@en ;
    dqv:inDimension qc:완전성 ;
    qc:assessmentScope "table" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean .

qc:UnusedColumnRate a dqv:Metric ;
    skos:prefLabel "미사용률"@ko, "Unused Column Rate"@en ;
    dqv:inDimension qc:완전성 ;
    qc:assessmentScope "table" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean .

qc:ColumnDuplicateRate a dqv:Metric ;
    skos:prefLabel "중복률 (컬럼 일관성)"@ko, "Column Duplicate Rate"@en ;
    dqv:inDimension qc:완전성 ;
    qc:assessmentScope "table" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean .

qc:RowDuplicateRate a dqv:Metric ;
    skos:prefLabel "중복률 (값 일관성)"@ko, "Row Duplicate Rate"@en ;
    dqv:inDimension qc:완전성 ;
    qc:assessmentScope "table" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean .

qc:CandidateKeyRate a dqv:Metric ;
    skos:prefLabel "후보키 비율"@ko, "Candidate Key Rate"@en ;
    dqv:inDimension qc:완전성 ;
    qc:assessmentScope "table" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean .

qc:MissingRateColumn a dqv:Metric ;
    skos:prefLabel "결측률 (컬럼)"@ko, "Missing Rate (Column)"@en ;
    dqv:inDimension qc:완전성 ;
    qc:assessmentScope "column" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean .

qc:ReferenceDataPresence a dqv:Metric ;
    skos:prefLabel "데이터기준시점 컬럼 여무"@ko, "Reference Date Column Presence"@en ;
    dqv:inDimension qc:적시성 ;
    qc:assessmentScope "table" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean ;
    qc:columnTypes "date" .

qc:DataFreshness a dqv:Metric ;
    skos:prefLabel "데이터 기준 시점 최신성"@ko, "Data Freshness"@en ;
    dqv:inDimension qc:적시성 ;
    qc:assessmentScope "column" ;
    qc:automationLevel "0"^^xsd:integer ;
    qc:validationType "비율" ;
    qc:isGate "false"^^xsd:boolean ;
    qc:columnTypes "date" .

qc:SyntacticValidity a dqv:Metric ;
    skos:prefLabel "구문정확성"@ko, "Syntactic Validity"@en ;
    dqv:inDimension qc:정확성 ;
    qc:assessmentScope "column" ;
    qc:automationLevel "2"^^xsd:integer ;
    qc:validationType "오류율" ;
    qc:isGate "false"^^xsd:boolean ;
    qc:columnTypes "email, phone_kr, date, postcode, latitude, longitude, literal" .
"""


def generate_ttl(result: dict) -> str:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    file_name  = result["file"]
    file_slug  = _slugify(os.path.splitext(file_name)[0])
    dataset_id = f"qc:dataset_{file_slug}"
    metrics    = result.get("metrics", [])
    dim_scores = result.get("dimension_scores", {})

    # ── 측정값 URI 미리 생성 ─────────────────────────────────────
    # 동일 (metric_id, col_slug) 쌍이 중복될 경우 인덱스 추가
    meas_ids: list[str] = []
    seen: dict[str, int] = {}
    for m in metrics:
        col_slug  = _slugify(m["column"]) if m["column"] else "table"
        mid       = _metric_id(m["metric_name"], m["scope"])
        base_key  = f"{mid}_{col_slug}"
        cnt       = seen.get(base_key, 0)
        seen[base_key] = cnt + 1
        suffix    = f"_{cnt}" if cnt else ""
        meas_ids.append(f"meas_{file_slug}_{base_key}{suffix}")

    parts: list[str] = [_PREFIXES, _VOCAB_BLOCK]

    # ── 데이터셋 노드 ─────────────────────────────────────────────
    parts.append("# ════════════════════════════════════════════════════════")
    parts.append("#  데이터셋 인스턴스")
    parts.append("# ════════════════════════════════════════════════════════\n")

    ds_props: list[str] = [
        f'    dcterms:title "{_esc(file_name)}" ',
        f'    dcterms:modified "{now_iso}"^^xsd:dateTime ',
        f'    qc:machineReadabilityPassed '
        f'"{str(result["machine_readability_passed"]).lower()}"^^xsd:boolean ',
    ]

    # 차원별 점수 (영문 프로퍼티명)
    for dim_ko, dim_en in DIM_EN.items():
        info = dim_scores.get(dim_ko)
        if info:
            ds_props.append(
                f'    qc:{dim_en}Score "{info["avg_score"]}"^^xsd:double '
            )

    # 측정값 링크
    if meas_ids:
        refs = " ,\n        ".join(f"qc:{m}" for m in meas_ids)
        ds_props.append(f"    dqv:hasQualityMeasurement\n        {refs} ")

    # 조합 (마지막만 "." 나머지 ";")
    lines: list[str] = [f"{dataset_id} a dcat:Dataset ;"]
    for i, prop in enumerate(ds_props):
        sep = "." if i == len(ds_props) - 1 else ";"
        lines.append(prop.rstrip() + f" {sep}")
    parts.append("\n".join(lines))
    parts.append("")

    # ── 측정값 노드 ───────────────────────────────────────────────
    parts.append("# ════════════════════════════════════════════════════════")
    parts.append("#  측정값 인스턴스 (dqv:QualityMeasurement)")
    parts.append("#  dqv:value    → 정규화 점수 (0–100)")
    parts.append("#  qc:rawValue  → 원시 측정값 (결측률% 등)")
    parts.append("# ════════════════════════════════════════════════════════\n")

    for meas_id, m in zip(meas_ids, metrics):
        mid     = _metric_id(m["metric_name"], m["scope"])
        detail  = _esc(m["detail"])

        props: list[str] = [
            f"    dqv:isMeasurementOf qc:{mid} ",
            f"    dqv:computedOn {dataset_id} ",
        ]
        if m["score"] is not None:
            try:
                props.append(f'    dqv:value "{float(m["score"])}"^^xsd:double ')
            except (TypeError, ValueError):
                props.append(f'    dqv:value "{_esc(str(m["score"]))}" ')
        if m["value"] is not None:
            try:
                props.append(f'    qc:rawValue "{float(m["value"])}"^^xsd:double ')
            except (TypeError, ValueError):
                pass
        props += [
            f'    qc:status "{m["status"]}" ',
            f'    qc:assessmentScope "{m["scope"]}" ',
        ]
        if m["column"]:
            props.append(f'    qc:columnName "{_esc(m["column"])}" ')
        props += [
            f'    qc:automationLevel "{m["automation_level"]}"^^xsd:integer ',
            f'    dcterms:description "{detail}" ',
            f'    dcterms:date "{now_iso}"^^xsd:dateTime ',
        ]

        node: list[str] = [f"qc:{meas_id} a dqv:QualityMeasurement ;"]
        for i, prop in enumerate(props):
            sep = "." if i == len(props) - 1 else ";"
            node.append(prop.rstrip() + f" {sep}")
        parts.append("\n".join(node))
        parts.append("")

    return "\n".join(parts)


# ════════════════════════════════════════════════════════════════════
#  파일 저장
# ════════════════════════════════════════════════════════════════════

def save_reports(result: dict, output_dir: str = ".") -> dict[str, str]:
    """MD + TTL 리포트를 output_dir에 저장하고 경로 dict 반환"""
    os.makedirs(output_dir, exist_ok=True)
    base     = _slugify(os.path.splitext(result["file"])[0])
    now_str  = datetime.now().strftime("%Y%m%d_%H%M%S")

    md_path  = os.path.join(output_dir, f"qc_{base}_{now_str}.md")
    ttl_path = os.path.join(output_dir, f"qc_{base}_{now_str}.ttl")

    with open(md_path,  "w", encoding="utf-8") as f:
        f.write(generate_md(result))
    with open(ttl_path, "w", encoding="utf-8") as f:
        f.write(generate_ttl(result))

    return {"md": md_path, "ttl": ttl_path}


# ════════════════════════════════════════════════════════════════════
#  CLI 진입점
# ════════════════════════════════════════════════════════════════════

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(
        description="품질 검사 JSON 결과 → MD / TTL 저장"
    )
    parser.add_argument(
        "result_json",
        help="run_check.py 출력 JSON 파일 경로 (- 이면 stdin)"
    )
    parser.add_argument(
        "--output-dir", default=".",
        help="리포트 저장 디렉토리 (기본값: 현재 디렉토리)"
    )
    args = parser.parse_args()

    if args.result_json == "-":
        result = json.load(sys.stdin)
    else:
        with open(args.result_json, "r", encoding="utf-8") as f:
            result = json.load(f)

    paths = save_reports(result, args.output_dir)
    print(json.dumps({"saved": paths}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
