"""跨模块数据流业务行为测试：covering version tracking, failure recovery,
batch deduplication, watermark management, schema version evolution, and
cross-service data consumption chains (source_registry → ingest_batch →
raw_records → field_parser → parsed_entity → dedup_engine →
aggregation_view → data_export)."""
from __future__ import annotations

import csv
import json
import os
import threading

import pytest

from app.store import store


class TestSourceRegistry:
    """数据源登记与验证测试。"""

    def test_register_new_source(self, pipeline):
        result = pipeline.register_source("app-error-log", "http", "beijing")
        assert result["ok"] is True
        assert result["entry"]["来源标识"] == "app-error-log"
        assert result["entry"]["status"] == "待验证"
        assert result["entry"]["source_version"] == 1

    def test_register_duplicate_source(self, pipeline):
        pipeline.register_source("duplicate-src", "http", "shanghai")
        result = pipeline.register_source("duplicate-src", "grpc", "beijing")
        assert result["ok"] is False
        assert "已登记" in result["message"]

    def test_validate_existing_source(self, pipeline):
        pipeline.register_source("to-validate", "http", "default")
        result = pipeline.validate_source("to-validate")
        assert result["ok"] is True
        assert result["entry"]["status"] == "已验证"
        assert result["entry"]["source_version"] == 2

    def test_validate_nonexistent_source(self, pipeline):
        result = pipeline.validate_source("ghost-source")
        assert result["ok"] is False

    def test_validate_increments_version(self, pipeline):
        pipeline.register_source("version-src", "http", "default")
        pipeline.validate_source("version-src")
        entry = pipeline.validate_source("version-src")["entry"]
        assert entry["source_version"] >= 2


class TestIngestBatch:
    """采集批次启动测试。"""

    def test_start_batch_unregistered_source(self, pipeline):
        result = pipeline.start_ingest_batch("missing-src", "2026Q1", [])
        assert result["ok"] is False

    def test_start_batch_unvalidated_source(self, pipeline):
        pipeline.register_source("unvalidated-src", "http", "default")
        result = pipeline.start_ingest_batch("unvalidated-src", "2026Q1", [])
        assert result["ok"] is False
        assert "未通过验证" in result["message"]

    def test_start_batch_success(self, pipeline, sample_raw_logs):
        pipeline.register_source("batch-src", "http", "default")
        pipeline.validate_source("batch-src")
        result = pipeline.start_ingest_batch("batch-src", "2026-01-15", sample_raw_logs)
        assert result["ok"] is True
        assert result["record_count"] == 5
        assert result["batch"]["status"] == "采集中"

    def test_batch_creates_raw_records(self, pipeline, sample_raw_logs):
        pipeline.register_source("record-src", "http", "default")
        pipeline.validate_source("record-src")
        result = pipeline.start_ingest_batch("record-src", "2026-01-15", sample_raw_logs)
        batch_no = result["batch"]["批次编号"]
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert len(batch_raw) == 5
        for r in batch_raw:
            assert r["status"] == "待解析"

    def test_no_duplicate_active_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("single-src", "http", "default")
        pipeline.validate_source("single-src")
        pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[:2])
        result = pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[2:])
        assert result["ok"] is False
        assert "进行中的批次" in result["message"]

    def test_batch_records_source_version(self, pipeline, sample_raw_logs):
        pipeline.register_source("ver-track-src", "http", "default")
        pipeline.validate_source("ver-track-src")
        result = pipeline.start_ingest_batch("ver-track-src", "2026Q1", sample_raw_logs[:1])
        assert result["batch"]["source_version"] == 2


class TestFieldParser:
    """字段解析测试。"""

    def test_parse_batch_no_records(self, pipeline):
        result = pipeline.parse_batch("NONEXIST-BATCH")
        assert result["ok"] is False

    def test_parse_batch_no_parser_rule_uses_default(self, pipeline, sample_raw_logs):
        pipeline.register_source("parse-src", "http", "default")
        pipeline.validate_source("parse-src")
        batch = pipeline.start_ingest_batch("parse-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]

        result = pipeline.parse_batch(batch_no)
        assert result["ok"] is True
        assert result["success"] > 0

    def test_parse_batch_sets_raw_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("status-src", "http", "default")
        pipeline.validate_source("status-src")
        batch = pipeline.start_ingest_batch("status-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert all(r["status"] == "已解析" for r in batch_raw)

    def test_parse_creates_parsed_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("ent-src", "http", "default")
        pipeline.validate_source("ent-src")
        batch = pipeline.start_ingest_batch("ent-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 2
        for p in parsed:
            assert p["status"] == "待校验"

    def test_parse_journals_created(self, pipeline, sample_raw_logs):
        pipeline.register_source("journal-src", "http", "default")
        pipeline.validate_source("journal-src")
        batch = pipeline.start_ingest_batch("journal-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        before = len(store.rows("parse_journal"))
        pipeline.parse_batch(batch_no)
        after = len(store.rows("parse_journal"))
        assert after > before


class TestValidation:
    """质量校验测试。"""

    def test_validate_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("valid-src", "http", "default")
        pipeline.validate_source("valid-src")
        batch = pipeline.start_ingest_batch("valid-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.validate_entities(batch_no)
        assert result["ok"] is True
        assert result["passed"] == 2

    def test_validate_sets_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("vstat-src", "http", "default")
        pipeline.validate_source("vstat-src")
        batch = pipeline.start_ingest_batch("vstat-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert all(p["status"] == "校验通过" for p in parsed)


class TestDeduplication:
    """去重引擎测试。"""

    def test_dedup_no_duplicates(self, pipeline, sample_raw_logs):
        pipeline.register_source("nodup-src", "http", "default")
        pipeline.validate_source("nodup-src")
        batch = pipeline.start_ingest_batch("nodup-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["time", "level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] == 0

    def test_dedup_with_duplicates(self, pipeline):
        pipeline.register_source("dup-src", "http", "default")
        pipeline.validate_source("dup-src")
        # 提交相同日志
        same_logs = [
            {"body": "t1|INFO|msg1"},
            {"body": "t1|INFO|msg1"},  # duplicate
            {"body": "t1|WARN|msg2"},
        ]
        batch = pipeline.start_ingest_batch("dup-src", "2026Q1", same_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] > 0

    def test_dedup_keep_last(self, pipeline):
        pipeline.register_source("kl-src", "http", "default")
        pipeline.validate_source("kl-src")
        logs = [
            {"body": "t1|INFO|first"},
            {"body": "t1|INFO|second"},
        ]
        batch = pipeline.start_ingest_batch("kl-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_last")
        assert result["ok"] is True


class TestAggregation:
    """聚合视图测试。"""

    def test_refresh_aggregation(self, pipeline, sample_raw_logs):
        pipeline.register_source("agg-src", "http", "default")
        pipeline.validate_source("agg-src")
        batch = pipeline.start_ingest_batch("agg-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("test-view", "level", "time", "count")
        assert result["ok"] is True
        assert "view" in result
        assert result["dimensions"] > 0

    def test_aggregation_dimensions(self, pipeline, sample_raw_logs):
        pipeline.register_source("dim-src", "http", "default")
        pipeline.validate_source("dim-src")
        batch = pipeline.start_ingest_batch("dim-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("dim-view", "level", "time", "count")
        results = result["view"]["results"]
        assert "INFO" in results
        assert results["INFO"]["count"] == 2  # two INFO logs


class TestDataExport:
    """数据导出与血缘测试。"""

    def test_build_export(self, pipeline, sample_raw_logs):
        pipeline.register_source("export-src", "http", "default")
        pipeline.validate_source("export-src")
        batch = pipeline.start_ingest_batch("export-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        result = pipeline.build_export(f"view-{batch_no}", "csv")
        assert result["ok"] is True
        assert result["export"]["status"] == "已完成"

    def test_export_creates_lineage(self, pipeline, sample_raw_logs):
        pipeline.register_source("lin-src", "http", "default")
        pipeline.validate_source("lin-src")
        batch = pipeline.start_ingest_batch("lin-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        lineage_before = len(store.rows("data_lineage"))
        pipeline.build_export(f"view-{batch_no}", "csv")
        lineage_after = len(store.rows("data_lineage"))
        assert lineage_after > lineage_before
        last = store.rows("data_lineage")[-1]
        assert last["export_format"] == "csv"


class TestWatermarkManagement:
    """水位管理测试。"""

    def test_advance_watermark_new_source(self, pipeline):
        result = pipeline.advance_watermark("wm-src-1", "INGE-0001", "2026-01-01T00:00:00")
        assert result["ok"] is True
        assert result["watermark"]["数据源"] == "wm-src-1"

    def test_advance_watermark_progress(self, pipeline):
        pipeline.advance_watermark("wm-src-2", "INGE-0001", "2026-01-01T00:00:00")
        result = pipeline.advance_watermark("wm-src-2", "INGE-0002", "2026-01-02T00:00:00")
        assert result["ok"] is True

    def test_watermark_rollback_prevention(self, pipeline):
        pipeline.advance_watermark("wm-src-3", "INGE-0001", "2026-01-05T00:00:00")
        result = pipeline.advance_watermark("wm-src-3", "INGE-0002", "2026-01-01T00:00:00")
        assert result["ok"] is False
        assert "倒退" in result["message"]
        wm = result["message"]
        assert "阻止" in wm

    def test_watermark_rollback_flags_abnormal(self, pipeline):
        pipeline.advance_watermark("wm-src-4", "INGE-0001", "2026-02-01T00:00:00")
        pipeline.advance_watermark("wm-src-4", "INGE-0002", "2026-01-01T00:00:00")
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-src-4"][0]
        assert wm["status"] == "异常回退"
        assert wm["abnormal"] is True


class TestSchemaEvolution:
    """Schema 版本演进测试。"""

    def test_evolve_schema_new(self, pipeline):
        result = pipeline.evolve_schema("v2.0", "app-logs", ["time", "level", "message", "host"], True)
        assert result["ok"] is True
        assert result["schema"]["版本号"] == "v2.0"
        assert result["schema"]["status"] == "已发布"

    def test_evolve_schema_duplicate(self, pipeline):
        pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        result = pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        assert result["ok"] is False
        assert "已存在" in result["message"]

    def test_evolve_schema_non_backward_compatible(self, pipeline):
        result = pipeline.evolve_schema("v4.0", "db-logs", ["timestamp", "query", "duration"], False)
        assert result["ok"] is True
        assert result["schema"]["向后兼容标记"] == "否"


class TestReplayEngine:
    """回溯重放引擎测试。"""

    def test_replay_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("replay-src", "http", "default")
        pipeline.validate_source("replay-src")
        batch = pipeline.start_ingest_batch("replay-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.replay_batch(batch_no, "new-rule-v2")
        assert result["ok"] is True
        assert result["replayed_count"] > 0
        assert result["replay"]["status"] == "已完成"


class TestFailureRecovery:
    """失败恢复测试。"""

    def test_recover_batch(self, pipeline):
        pipeline.register_source("recv-src", "http", "default")
        pipeline.validate_source("recv-src")
        logs = [
            {"body": "bad|format"},  # 只有单字段，解析会失败
            {"body": "t1|INFO|msg"},
        ]
        batch = pipeline.start_ingest_batch("recv-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.recover_batch(batch_no)
        assert result["ok"] is True
        assert isinstance(result["recovered"], int)


class TestFullPipeline:
    """全链路流水线测试。"""

    def test_run_full_pipeline(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "full-pipeline-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        assert result["ok"] is True
        steps = result["steps"]
        assert "source_registry" in steps
        assert "ingest_batch" in steps
        assert "field_parser" in steps
        assert "validate" in steps
        assert "dedup" in steps
        assert "aggregation" in steps
        assert "export" in steps
        assert "watermark" in steps

    def test_full_pipeline_data_lineage(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "lineage-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        lineages = store.rows("data_lineage")
        assert len(lineages) > 0

    def test_full_pipeline_parsed_entities_refer_to_batch(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "ref-check-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        batch_no = result["batch_no"]
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 3

    def test_full_pipeline_watermark_advanced(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "wm-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-check-src"]
        assert len(wm) == 1
        assert wm[0]["status"] == "正常"


class TestCrossModuleSnapshot:
    """跨模块数据快照测试。"""

    def test_snapshot_contains_all_modules(self, pipeline, sample_raw_logs):
        pipeline.run_full_pipeline("snap-src", "2026Q1", sample_raw_logs[:3])

        from app.services.base import BaseModuleService
        from app.services import SERVICES
        snapshots = {key: svc.domain_snapshot() for key, svc in SERVICES.items()}

        assert "source_registry" in snapshots
        assert "ingest_batch" in snapshots
        assert "raw_records" in snapshots
        assert "parsed_entity" in snapshots
        assert "dedup_engine" in snapshots
        assert "aggregation_view" in snapshots
        assert "data_export" in snapshots
        assert "data_lineage" in snapshots
        assert "watermark_mgr" in snapshots

    def test_snapshot_data_consistency(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline("consistency-src", "2026Q1", sample_raw_logs[:3])
        batch_no = result["batch_no"]

        from app.services import SERVICES
        parsed_snap = SERVICES["parsed_entity"].domain_snapshot()
        assert parsed_snap["count"] >= 3

        raw_snap = SERVICES["raw_records"].domain_snapshot()
        assert raw_snap["count"] > 0


class TestBatchIsolation:
    """混批修复验收：两批同级别日志跑全链路，统计、聚合与对账按批隔离。"""

    @staticmethod
    def _logs_a():
        return [
            {"body": "2026-01-15T10:00:00|INFO|a-first"},
            {"body": "2026-01-15T10:01:00|INFO|a-second"},
            {"body": "2026-01-15T10:02:00|WARN|a-third"},
        ]

    @staticmethod
    def _logs_b():
        # 与 A 批同级别，且首行 time+level 与 A 批完全相同的邻批日志
        return [
            {"body": "2026-01-15T10:00:00|INFO|b-first"},
            {"body": "2026-01-16T10:01:00|INFO|b-second"},
            {"body": "2026-01-16T10:02:00|ERROR|b-third"},
        ]

    @staticmethod
    def _view(view_name):
        for v in store.rows("aggregation_view"):
            if v.get("视图名称") == view_name:
                return v
        return None

    def test_two_batches_do_not_mix(self, pipeline):
        ra = pipeline.run_full_pipeline("iso-src", "2026-01-15", self._logs_a())
        assert ra["ok"] is True
        rb = pipeline.run_full_pipeline("iso-src", "2026-01-16", self._logs_b())
        assert rb["ok"] is True
        batch_a, batch_b = ra["batch_no"], rb["batch_no"]
        assert batch_a != batch_b

        view_a = self._view(f"view-{batch_a}")
        view_b = self._view(f"view-{batch_b}")
        # 本批统计只含本批明细：A 批 INFO=2/WARN=1，B 批 INFO=2/ERROR=1
        assert view_a["results"]["INFO"]["count"] == 2
        assert view_a["results"]["WARN"]["count"] == 1
        assert "ERROR" not in view_a["results"]
        assert view_b["results"]["INFO"]["count"] == 2
        assert view_b["results"]["ERROR"]["count"] == 1
        assert "WARN" not in view_b["results"]
        # 取数依据：视图登记来源批次与明细ID清单，计数与引用明细一致
        assert view_a["batch_no"] == batch_a
        ids_a = json.loads(view_a["source_entity_ids"])
        assert view_a["source_records"] == len(ids_a) == 3
        assert sum(d["count"] for d in view_a["results"].values()) == len(ids_a)
        entities_a = [p for p in store.rows("parsed_entity")
                      if p.get("来源批次") == batch_a and p.get("status") == "校验通过"]
        assert sorted(ids_a) == sorted(int(p["id"]) for p in entities_a)

    def test_neighbor_batch_does_not_change_persisted_counts(self, pipeline):
        ra = pipeline.run_full_pipeline("stable-src", "2026-01-15", self._logs_a())
        batch_a = ra["batch_no"]
        view_name = f"view-{batch_a}"
        before = self._view(view_name)
        snapshot = (json.dumps(before["results"], sort_keys=True), before["revision"],
                    before["source_entity_ids"])

        rb = pipeline.run_full_pipeline("stable-src", "2026-01-16", self._logs_b())
        assert rb["ok"] is True

        after = self._view(view_name)
        # 邻批出数后，本批已落库的计数、修订号与取数依据保持不动
        assert (json.dumps(after["results"], sort_keys=True), after["revision"],
                after["source_entity_ids"]) == snapshot
        # 邻批去重不驱逐本批明细（两批首行 time+level 相同）
        entities_a = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_a]
        assert all(p["status"] == "校验通过" for p in entities_a)

    def test_late_correction_recompute_and_export(self, pipeline):
        ra = pipeline.run_full_pipeline("corr-src", "2026-01-15", self._logs_a())
        rb = pipeline.run_full_pipeline("corr-src", "2026-01-16", self._logs_b())
        batch_a, batch_b = ra["batch_no"], rb["batch_no"]
        view_a_name, view_b_name = f"view-{batch_a}", f"view-{batch_b}"
        view_b_before = json.dumps(self._view(view_b_name)["results"], sort_keys=True)

        # 只为 A 批加一行迟到更正，再次计算
        corr = pipeline.add_batch_correction(batch_a, [{"body": "2026-01-15T10:03:00|INFO|a-fix"}])
        assert corr["ok"] is True
        assert corr["line_range"] == ["4", "4"]
        re = pipeline.refresh_aggregation(view_a_name, "level", "time", "count", batch_no=batch_a)
        assert re["revision"] == 2

        view_a = self._view(view_a_name)
        assert view_a["results"]["INFO"]["count"] == 3
        assert view_a["results"]["WARN"]["count"] == 1
        ids_a = json.loads(view_a["source_entity_ids"])
        assert len(ids_a) == 4 == view_a["source_records"]
        assert sum(d["count"] for d in view_a["results"].values()) == 4
        # 明细数：本批校验通过明细与视图取数依据一一对应
        entities_a = [p for p in store.rows("parsed_entity")
                      if p.get("来源批次") == batch_a and p.get("status") == "校验通过"]
        assert len(entities_a) == 4
        # 邻批聚合详情不受本批更正影响
        assert json.dumps(self._view(view_b_name)["results"], sort_keys=True) == view_b_before
        assert self._view(view_b_name)["revision"] == 1

        # 交付：摘要与详单落盘并回写文件索引清单，详单带原文行号供对账
        out = pipeline.build_export(view_a_name, "csv")
        assert out["record_count"] == 4
        export_dir = os.environ["LOG_INGEST_EXPORT_DIR"]
        with open(out["summary_file"], encoding="utf-8") as fh:
            summary = json.load(fh)
        assert summary["批次编号"] == batch_a
        assert summary["明细数"] == 4
        assert summary["revision"] == 2
        with open(out["detail_file"], encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 4
        assert [r["原始行号"] for r in rows] == ["1", "2", "3", "4"]
        assert all(r["来源批次"] == batch_a for r in rows)
        with open(os.path.join(export_dir, "index.json"), encoding="utf-8") as fh:
            manifest = json.load(fh)
        entries = [e for e in manifest["exports"] if e["视图名称"] == view_a_name]
        assert len(entries) == 2  # 首次交付 + 更正后再次交付
        assert entries[-1]["revision"] == 2
        assert entries[-1]["明细数"] == 4

    def test_concurrent_correction_publishes_single_consistent_revision(self, pipeline):
        ra = pipeline.run_full_pipeline("conc-src", "2026-01-15", self._logs_a())
        batch_a = ra["batch_no"]
        view_name = f"view-{batch_a}"

        def worker(i: int) -> None:
            pipeline.add_batch_correction(
                batch_a, [{"body": f"2026-01-15T11:{i:02d}:00|INFO|fix-{i}"}])
            pipeline.refresh_aggregation(view_name, "level", "time", "count", batch_no=batch_a)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 无论交错如何，落库的必是一套完整修订：计数与引用明细对得上
        view = self._view(view_name)
        ids = json.loads(view["source_entity_ids"])
        assert sum(d["count"] for d in view["results"].values()) == len(ids)
        pipeline.refresh_aggregation(view_name, "level", "time", "count", batch_no=batch_a)
        view = self._view(view_name)
        ids = json.loads(view["source_entity_ids"])
        entities = [p for p in store.rows("parsed_entity")
                    if p.get("来源批次") == batch_a and p.get("status") == "校验通过"]
        assert len(ids) == len(entities) == 8
        assert sum(d["count"] for d in view["results"].values()) == 8

    def test_legacy_view_refresh_upserts_receipt(self, pipeline, sample_raw_logs):
        pipeline.register_source("legacy-src", "http", "default")
        pipeline.validate_source("legacy-src")
        batch = pipeline.start_ingest_batch("legacy-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        # 兼容已有批次回执：不带批次的旧式刷新仍可用，同名视图只留一套修订
        r1 = pipeline.refresh_aggregation("legacy-view", "level", "time", "count")
        assert r1["ok"] is True
        r2 = pipeline.refresh_aggregation("legacy-view", "level", "time", "count")
        assert r2["revision"] == 2
        views = [v for v in store.rows("aggregation_view") if v.get("视图名称") == "legacy-view"]
        assert len(views) == 1
        # 已有回执登记批次后，再刷新只统计对应批次的可用明细
        view = views[0]
        view["batch_no"] = batch_no
        r3 = pipeline.refresh_aggregation("legacy-view", "level", "time", "count")
        assert r3["batch_no"] == batch_no
        ids = json.loads(r3["view"]["source_entity_ids"])
        assert len(ids) == 2

    def test_reparse_does_not_duplicate_details(self, pipeline, sample_raw_logs):
        pipeline.register_source("reparse-src", "http", "default")
        pipeline.validate_source("reparse-src")
        batch = pipeline.start_ingest_batch("reparse-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        again = pipeline.parse_batch(batch_no)
        assert again["success"] == 0
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 2

    def test_recover_reconfirms_batch_read_scope(self, pipeline):
        pipeline.register_source("recv-scope-src", "http", "default")
        pipeline.validate_source("recv-scope-src")
        b1 = pipeline.start_ingest_batch("recv-scope-src", "2026Q1", [{"body": "x|y"}])
        batch_1 = b1["batch"]["批次编号"]
        pipeline.parse_batch(batch_1)
        b2 = pipeline.start_ingest_batch("recv-scope-src", "2026Q2", [{"body": "p|q"}])
        batch_2 = b2["batch"]["批次编号"]
        pipeline.parse_batch(batch_2)

        # 两批各置一条解析失败，恢复本批时不得越界到邻批
        for no in (batch_1, batch_2):
            raw = [r for r in store.rows("raw_records") if r.get("批次编号") == no][0]
            raw["status"] = "解析失败"
        result = pipeline.recover_batch(batch_1)
        assert result["recovered"] == 1
        raw_1 = [r for r in store.rows("raw_records") if r.get("批次编号") == batch_1][0]
        raw_2 = [r for r in store.rows("raw_records") if r.get("批次编号") == batch_2][0]
        assert raw_1["status"] == "待解析"
        assert raw_2["status"] == "解析失败"