"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import csv
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from app.config import settings
from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _export_dir() -> Path:
    """交付文件落盘目录：环境变量优先，其次配置默认。"""
    return Path(os.environ.get("LOG_INGEST_EXPORT_DIR", settings.export_dir))


def _safe_name(name: str) -> str:
    """视图/批次名转文件名片段，避免路径分隔符等非法字符。"""
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in str(name))


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    def __init__(self) -> None:
        # 计数发布锁：聚合刷新与交付写盘必须整体成一套完整修订，
        # 迟到更正/邻批校验并发进来时也不会出现计数与明细对不上的中间态。
        self._publish_lock = threading.RLock()

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。

        读取范围按批次重新确认：只取本批仍处「待解析」的原始记录，
        已解析的不再重复产出明细，恢复中断批次时也不会混入他批数据。
        """
        raw_records_list = store.rows("raw_records")
        batch_raw_all = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw_all:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}
        batch_raw = [r for r in batch_raw_all if r.get("status") == "待解析"]
        if not batch_raw:
            return {
                "ok": True,
                "message": f"批次 {batch_no} 无待解析的新记录",
                "success": 0,
                "failure": 0,
                "parsed_ids": [],
                "failed_ids": [],
            }

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                # 累加本批已产出的明细ID，恢复/补采再解析时不覆盖历史回执
                try:
                    prior_ids = json.loads(b.get("parsed_ids") or "[]")
                except (json.JSONDecodeError, TypeError):
                    prior_ids = []
                b["parsed_ids"] = json.dumps([*prior_ids, *parsed_ids])
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 迟到更正 ----------
    def add_batch_correction(
        self,
        batch_no: str,
        raw_logs: list[dict[str, Any]],
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """向已存在的批次写入迟到更正行：原始行号在本批内顺延，
        新增行立即按本批读取范围解析并校验，供计数发布重新计算。
        """
        batches = store.rows("ingest_batch")
        batch_entry = None
        for b in batches:
            if b.get("批次编号") == batch_no:
                batch_entry = b
                break
        if batch_entry is None:
            return {"ok": False, "message": f"批次 {batch_no} 不存在"}
        if not raw_logs:
            return {"ok": False, "message": "迟到更正内容为空"}

        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        # 行号在本批现有最大值上顺延，保证详单原文行号唯一可追溯
        line_no = 0
        for r in batch_raw:
            try:
                line_no = max(line_no, int(r.get("原始行号", "0") or 0))
            except (TypeError, ValueError):
                continue

        created_ids: list[int] = []
        for log in raw_logs:
            line_no += 1
            raw_id = _next_id("raw_records")
            record = {
                "id": raw_id,
                "批次编号": batch_no,
                "原始行号": str(line_no),
                "采集时间戳": _now_iso(),
                "原始正文": log.get("body", str(log)),
                "解析状态": "未解析",
                "status": "待解析",
                "pending": True,
                "abnormal": False,
                "batch_version": batch_entry.get("batch_version", 1),
                "source_id": batch_entry.get("所属数据源"),
                "ingested_at": _now_iso(),
                "late_correction": True,
            }
            raw_records_list.append(record)
            created_ids.append(raw_id)

        # 批次回执同步：记录总数与原始记录范围一并更新
        try:
            raw_ids = json.loads(batch_entry.get("raw_record_ids") or "[]")
        except (json.JSONDecodeError, TypeError):
            raw_ids = []
        raw_ids.extend(created_ids)
        batch_entry["raw_record_ids"] = json.dumps(raw_ids)
        batch_entry["记录总数"] = str(len(raw_ids))
        batch_entry["batch_version"] = batch_entry.get("batch_version", 1) + 1
        batch_entry["corrected_at"] = _now_iso()

        # 只解析本批待处理的新行（读取范围按批次重新确认），随后校验
        parse_result = self.parse_batch(batch_no, parser_rule_name)
        validate_result = self.validate_entities(batch_no)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 已写入 {len(created_ids)} 行迟到更正",
            "batch_no": batch_no,
            "added_raw_ids": created_ids,
            "line_range": [str(line_no - len(created_ids) + 1), str(line_no)],
            "parsed": parse_result.get("success", 0),
            "validated": validate_result.get("passed", 0),
        }

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
        batch_no: str | None = None,
    ) -> dict[str, Any]:
        """对标准化明细执行去重。

        去重范围按批次隔离：显式给定 batch_no 时只处理本批；
        窗口为 batch 时按来源批次分组各自去重，邻批出数不会改动本批明细状态。
        """
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": batch_no or "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重：键空间按批次隔离，同一键值跨批不互相驱逐
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            rec_batch = rec.get("来源批次", "")
            if batch_no is not None and rec_batch != batch_no:
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if batch_no is None and window != "batch":
                scope = "__global__"
            else:
                scope = str(rec_batch)
            scoped_key = f"{scope}::{key}"
            if scoped_key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[scoped_key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[scoped_key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[scoped_key] = rec["id"]
            else:
                seen[scoped_key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "batch_no": batch_no,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
        batch_no: str | None = None,
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。

        批次统计只取本批「校验通过」的可用明细；视图落库时保留取数依据
        （来源批次 + 明细ID清单），计数与其引用的明细一一对应。
        同名视图按完整修订原地更新（兼容已有批次回执），并发刷新在发布锁
        下整体生效，不会出现计数与明细对不上的中间态。
        """
        with self._publish_lock:
            parsed = store.rows("parsed_entity")
            aggs = store.rows("aggregation_view")

            # 兼容已有批次回执：未显式指定批次时沿用视图已登记的批次范围
            existing = None
            for a in aggs:
                if a.get("视图名称") == view_name:
                    existing = a
            if batch_no is None and existing is not None:
                batch_no = existing.get("batch_no") or None

            # 先快照本批可用明细，再按快照计算，保证计数与引用明细一致
            candidates = [p for p in parsed if p.get("status") == "校验通过"]
            if batch_no is not None:
                candidates = [p for p in candidates if p.get("来源批次") == batch_no]

            results: dict[str, dict[str, Any]] = {}
            source_ids: list[int] = []
            for entity in candidates:
                try:
                    fields_data = json.loads(entity.get("标准化字段集", "{}"))
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(fields_data, dict):
                    continue

                dim_val = str(fields_data.get(dim_field, "unknown"))
                metric_val = fields_data.get(metric_field, "0")

                if dim_val not in results:
                    results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

                results[dim_val]["count"] += 1
                try:
                    results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
                except (ValueError, TypeError):
                    pass
                results[dim_val]["values"].append(metric_val)
                # 取数依据：只有真正计入结果的明细才进入引用清单
                source_ids.append(int(entity["id"]))

            if existing is not None:
                # 原地发布一套完整修订：结果、取数依据、修订号同时替换
                existing["聚合维度"] = dim_field
                existing["指标定义"] = f"{agg_func}({metric_field})"
                existing["batch_no"] = batch_no
                existing["results"] = results
                existing["source_entity_ids"] = json.dumps(source_ids)
                existing["source_records"] = len(source_ids)
                existing["revision"] = int(existing.get("revision", 1)) + 1
                existing["status"] = "已生效"
                existing["pending"] = False
                existing["abnormal"] = False
                existing["refreshed_at"] = _now_iso()
                agg_entry = existing
            else:
                agg_entry = {
                    "id": _next_id("aggregation_view"),
                    "视图名称": view_name,
                    "聚合维度": dim_field,
                    "指标定义": f"{agg_func}({metric_field})",
                    "数据源范围": f"标准化明细/{batch_no}" if batch_no else "标准化明细",
                    "刷新间隔": "manual",
                    "status": "已生效",
                    "pending": False,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "results": results,
                    "source_entity_ids": json.dumps(source_ids),
                    "source_records": len(source_ids),
                    "revision": 1,
                    "refreshed_at": _now_iso(),
                }
                aggs.append(agg_entry)

            return {
                "ok": True,
                "message": f"聚合视图 {view_name} 已刷新",
                "view": agg_entry,
                "batch_no": batch_no,
                "revision": agg_entry["revision"],
                "source_records": len(source_ids),
                "dimensions": len(results),
            }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图交付本批数据：写摘要与详单文件，并回写文件索引清单。

        详单按视图登记的取数依据（source_entity_ids）逐条展开，带出原始
        行号供下游对账；只统计对应批次的可用明细，邻批数据不进入本次交付。
        """
        with self._publish_lock:
            exports = store.rows("data_export")
            lineages = store.rows("data_lineage")

            # 查找目标视图：同名视图只保留一套完整修订，取最新一条回执
            aggs = store.rows("aggregation_view")
            view = None
            for a in aggs:
                if a.get("视图名称") == view_name:
                    view = a

            results_data = view.get("results", {}) if view else {}
            batch_no = view.get("batch_no") if view else None
            revision = int(view.get("revision", 1)) if view else 0
            try:
                source_ids = json.loads(view.get("source_entity_ids") or "[]") if view else []
            except (json.JSONDecodeError, TypeError):
                source_ids = []

            # 详单：按取数依据展开明细，关联原始记录拿到原文行号
            details: list[dict[str, Any]] = []
            for pid in source_ids:
                entity = store.find("parsed_entity", int(pid))
                if entity is None:
                    continue
                raw = None
                try:
                    raw = store.find("raw_records", int(entity.get("原始记录ID", "0") or 0))
                except (TypeError, ValueError):
                    raw = None
                details.append({
                    "明细ID": entity.get("id"),
                    "来源批次": entity.get("来源批次", ""),
                    "原始记录ID": entity.get("原始记录ID", ""),
                    "原始行号": raw.get("原始行号", "") if raw else "",
                    "解析规则版本": entity.get("解析规则版本", ""),
                    "校验状态": entity.get("status", ""),
                    "标准化字段集": entity.get("标准化字段集", ""),
                })

            export_id = _next_id("data_export")
            exported_at = _now_iso()

            # 交付写盘：本批摘要 + 详单，随后回写文件索引清单
            export_dir = _export_dir()
            export_dir.mkdir(parents=True, exist_ok=True)
            stem = f"{_safe_name(view_name)}_r{revision}"
            summary_path = export_dir / f"{stem}_summary.json"
            detail_path = export_dir / f"{stem}_detail.csv"

            summary_doc = {
                "视图名称": view_name,
                "批次编号": batch_no,
                "revision": revision,
                "聚合维度": view.get("聚合维度") if view else None,
                "指标定义": view.get("指标定义") if view else None,
                "统计结果": results_data,
                "明细数": len(details),
                "维度数": len(results_data),
                "过滤条件": filters or {},
                "exported_at": exported_at,
            }
            summary_path.write_text(
                json.dumps(summary_doc, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            with detail_path.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=["明细ID", "来源批次", "原始记录ID", "原始行号",
                                "解析规则版本", "校验状态", "标准化字段集"],
                    lineterminator="\n",
                )
                writer.writeheader()
                writer.writerows(details)

            export_entry = {
                "id": export_id,
                "导出范围": view_name,
                "目标格式": export_format,
                "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
                "导出状态": "",
                "文件路径": str(summary_path),
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "batch_no": batch_no,
                "revision": revision,
                "results": results_data,
                "record_count": len(details),
                "dimension_count": len(results_data),
                "summary_file": str(summary_path),
                "detail_file": str(detail_path),
                "exported_at": exported_at,
            }
            exports.append(export_entry)

            # 文件索引清单：登记本次交付的摘要/详单，供实际对账检索
            manifest_path = export_dir / "index.json"
            manifest = self._read_manifest(manifest_path)
            manifest["exports"].append({
                "export_id": export_id,
                "视图名称": view_name,
                "批次编号": batch_no,
                "revision": revision,
                "摘要文件": str(summary_path),
                "详单文件": str(detail_path),
                "明细数": len(details),
                "维度数": len(results_data),
                "exported_at": exported_at,
            })
            tmp_path = manifest_path.with_suffix(".json.tmp")
            tmp_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            os.replace(tmp_path, manifest_path)

            # 记录血缘
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": batch_no or view_name,
                "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
                "标准化版本": view_name,
                "下游消费标识": f"export-{export_id}",
                "记录时间": exported_at,
                "status": "已记录",
                "pending": False,
                "abnormal": False,
                "export_id": str(export_id),
                "export_format": export_format,
                "revision": revision,
            }
            lineages.append(lineage_entry)

            return {
                "ok": True,
                "message": f"数据导出 {view_name}.{export_format} 完成",
                "export": export_entry,
                "lineage": lineage_entry,
                "batch_no": batch_no,
                "record_count": len(details),
                "summary_file": str(summary_path),
                "detail_file": str(detail_path),
                "manifest": str(manifest_path),
            }

    @staticmethod
    def _read_manifest(manifest_path: Path) -> dict[str, Any]:
        """读取文件索引清单；不存在或损坏时返回空清单。"""
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("exports"), list):
                return data
        except (OSError, json.JSONDecodeError):
            pass
        return {"exports": []}

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源（同源邻批重跑时沿用已登记数据源）
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            existing = None
            for s in store.rows("source_registry"):
                if s.get("来源标识") == source_id:
                    existing = s
                    break
            if existing is None:
                return {"ok": False, "message": reg["message"]}
            reg = {"ok": True, "message": reg["message"], "entry": existing}
        if reg["entry"].get("status") != "已验证":
            self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重（按本批隔离，邻批明细不参与）
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first", batch_no=batch_no)
        steps["dedup"] = dedup_result

        # S5: 聚合（只统计本批标准化明细）
        agg_result = self.refresh_aggregation(
            f"view-{batch_no}", "level", "time", "count", batch_no=batch_no
        )
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。

        恢复范围按批次重新确认：优先以批次回执登记的 raw_record_ids 为准，
        旧批次缺少回执时退化为按批次编号扫描，绝不越出本批读取范围。
        """
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        # 按批次重新确认读取范围
        scope_ids: set[int] | None = None
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == batch_no:
                try:
                    ids = json.loads(b.get("raw_record_ids") or "[]")
                    scope_ids = {int(i) for i in ids}
                except (json.JSONDecodeError, TypeError, ValueError):
                    scope_ids = None
                break

        failed_raw = []
        for r in raw_records_list:
            if r.get("批次编号") != batch_no or r.get("status") != "解析失败":
                continue
            if scope_ids is not None and int(r.get("id", 0)) not in scope_ids:
                continue
            failed_raw.append(r)

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析：失败记录一律放回待解析，重试队列仅作计数跟踪
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析"
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()