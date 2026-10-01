import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError, PermissionDenied
from src.repository import Repository
from src.service import Service
from src.snapshot import GENESIS
from src.rules import STATES


def change(ref, title=None, severity="major", quantity=5, threshold=10, records=None):
    return {
        "external_ref": ref,
        "title": title or f"缺陷{ref}",
        "description": f"缺陷{ref}的现场描述",
        "severity": severity,
        "quantity": quantity,
        "threshold": threshold,
        "records": records or [],
    }


def advance(service, item, target):
    """按状态机把项目推进到指定状态。"""
    roles = {
        "inspected": "inspector",
        "defect_confirmed": "dam_engineer",
        "repair": "dam_engineer",
        "verified": "inspector",
        "closed": "emergency_manager",
    }
    current = item
    for state in STATES[1:]:
        if current["status"] == target:
            break
        current = service.transition(current["id"], state, current["version"],
                                     "reviewer", roles[state])
    return current


class SnapshotMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    # ------------------------------------------------------------------
    # 幂等
    # ------------------------------------------------------------------
    def test_duplicate_import_uses_first_result(self):
        first = self.service.import_changeset(
            "REQ-1", GENESIS, [change("T-1", "初版")], "actor", "inspector")
        second = self.service.import_changeset(
            "REQ-1", GENESIS, [change("T-1", "初版")], "actor", "inspector")
        self.assertEqual(first, second)
        item = self.repo.get_item_by_external_ref("T-1")
        self.assertEqual(item["version"], 1)
        self.assertEqual(len(self.repo.list_snapshots()), 2)  # 回填 + 本次导入

    def test_import_requires_inspector_role(self):
        with self.assertRaises(PermissionDenied):
            self.service.import_changeset("REQ-X", GENESIS, [change("T-1")],
                                          "actor", "viewer")

    # ------------------------------------------------------------------
    # 按任务号合并
    # ------------------------------------------------------------------
    def test_sequential_import_overwrites_late_version(self):
        self.service.import_changeset("SETUP", GENESIS, [change("T-1", "初版")],
                                      "actor", "inspector")
        cp1 = self.repo.get_latest_checkpoint()
        self.service.import_changeset("REQ-A", cp1, [change("T-1", "A版")],
                                      "actor", "inspector")
        cp2 = self.repo.get_latest_checkpoint()
        result = self.service.import_changeset("REQ-B", cp2, [change("T-1", "B版")],
                                              "actor", "inspector")
        self.assertEqual(result["status"], "applied")
        self.assertEqual(result["conflicts"], [])
        item = self.repo.get_item_by_external_ref("T-1")
        self.assertEqual(item["title"], "B版")  # 顺序提交，晚到覆盖

    def test_concurrent_same_defect_keeps_candidates(self):
        self.service.import_changeset("SETUP", GENESIS, [change("T-1", "初版")],
                                      "actor", "inspector")
        cp1 = self.repo.get_latest_checkpoint()
        self.service.import_changeset("REQ-A", cp1, [change("T-1", "A版")],
                                      "actor", "inspector")
        result = self.service.import_changeset("REQ-B", cp1, [change("T-1", "B版")],
                                               "actor", "inspector")
        self.assertEqual(result["status"], "conflict")
        self.assertEqual(result["conflicts"], ["T-1"])
        # 先到结果生效：线上仍是 A 版
        item = self.repo.get_item_by_external_ref("T-1")
        self.assertEqual(item["title"], "A版")
        # 后到内容按请求号留冲突候选
        conflicts = self.service.list_conflicts("viewer")
        self.assertEqual(len(conflicts), 1)
        conflict = conflicts[0]
        self.assertEqual(conflict["status"], "pending_review")
        request_nos = [c["request_no"] for c in conflict["candidates"]]
        self.assertIn("BASELINE", request_nos)
        self.assertIn("REQ-B", request_nos)
        incoming = next(c for c in conflict["candidates"] if c["request_no"] == "REQ-B")
        self.assertEqual(incoming["payload"]["title"], "B版")

    # ------------------------------------------------------------------
    # 复核前冻结
    # ------------------------------------------------------------------
    def test_frozen_before_review_cannot_close_or_dispatch(self):
        self.service.import_changeset("SETUP", GENESIS, [change("T-1", "初版")],
                                      "actor", "inspector")
        cp1 = self.repo.get_latest_checkpoint()
        self.service.import_changeset("REQ-A", cp1, [change("T-1", "A版")],
                                      "actor", "inspector")
        self.service.import_changeset("REQ-B", cp1, [change("T-1", "B版")],
                                     "actor", "inspector")
        item = self.repo.get_item_by_external_ref("T-1")
        item = advance(self.service, item, "verified")
        with self.assertRaises(ConflictError):
            self.service.transition(item["id"], "closed", item["version"],
                                    "reviewer", "emergency_manager")
        with self.assertRaises(ConflictError):
            self.service.dispatch_emergency(item["id"], "reviewer", "emergency_manager")
        enriched = self.service.get_item(item["id"], "viewer")
        self.assertTrue(enriched["has_pending_conflict"])
        self.assertTrue(enriched["emergency_blocked"])

    def test_review_resolves_and_unfreezes(self):
        self.service.import_changeset("SETUP", GENESIS, [change("T-1", "初版")],
                                      "actor", "inspector")
        cp1 = self.repo.get_latest_checkpoint()
        self.service.import_changeset("REQ-A", cp1, [change("T-1", "A版")],
                                      "actor", "inspector")
        self.service.import_changeset("REQ-B", cp1, [change("T-1", "B版")],
                                     "actor", "inspector")
        item = self.repo.get_item_by_external_ref("T-1")
        item = advance(self.service, item, "verified")
        conflict = self.service.list_conflicts("viewer")[0]
        out = self.service.review_conflict(conflict["id"], "REQ-B", "reviewer",
                                            "dam_engineer")
        self.assertEqual(out["conflict"]["status"], "resolved")
        self.assertEqual(out["conflict"]["winning_request_no"], "REQ-B")
        item = self.repo.get_item_by_external_ref("T-1")
        self.assertEqual(item["title"], "B版")  # 复核后后到版本生效
        enriched = self.service.get_item(item["id"], "viewer")
        self.assertFalse(enriched["has_pending_conflict"])
        # 补一条已关闭证据，关闭缺陷
        self.service.add_record(item["id"], {
            "kind": "evidence", "detail": "复核证据", "status": "closed",
            "external_ref": "EV-1"}, "recorder", "inspector")
        closed = self.service.transition(item["id"], "closed", item["version"],
                                         "reviewer", "emergency_manager")
        self.assertEqual(closed["status"], "closed")

    # ------------------------------------------------------------------
    # 按检查点恢复重试
    # ------------------------------------------------------------------
    def test_recover_after_step_failure(self):
        self.service.import_changeset("SETUP", GENESIS, [change("T-1", "初版")],
                                      "actor", "inspector")
        cp1 = self.repo.get_latest_checkpoint()

        def fail_at_applied(step):
            if step == "applied":
                raise RuntimeError("模拟写入中断")

        failing = Service(self.repo, on_step=fail_at_applied)
        with self.assertRaises(RuntimeError):
            failing.import_changeset("REQ-A", cp1, [change("T-1", "A版")],
                                     "actor", "inspector")
        snap = self.repo.get_snapshot_by_request("REQ-A")
        self.assertEqual(snap["status"], "failed")
        self.assertEqual(snap["step"], "applied")
        # 同请求号重试，从检查点续传
        result = self.service.import_changeset("REQ-A", cp1, [change("T-1", "A版")],
                                               "actor", "inspector")
        self.assertEqual(result["status"], "applied")
        item = self.repo.get_item_by_external_ref("T-1")
        self.assertEqual(item["title"], "A版")
        self.assertEqual(self.repo.get_snapshot_by_request("REQ-A")["status"], "complete")

    def test_recover_after_mid_write_failure(self):
        self.service.import_changeset("SETUP", GENESIS, [change("T-1", "初版")],
                                      "actor", "inspector")
        cp1 = self.repo.get_latest_checkpoint()

        def fail_after_t1(cp, ref, step):
            if ref == "T-1":
                raise RuntimeError("模拟写到 T-1 后崩溃")

        failing = Service(self.repo, on_progress=fail_after_t1)
        with self.assertRaises(RuntimeError):
            failing.import_changeset("REQ-A", cp1, [
                change("T-1", "T1"), change("T-2", "T2"), change("T-3", "T3"),
            ], "actor", "inspector")
        # T-1 已落盘，T-2/T-3 未写入
        self.assertIsNotNone(self.repo.get_item_by_external_ref("T-1"))
        self.assertIsNone(self.repo.get_item_by_external_ref("T-2"))
        # T-1 已在 SETUP 创建(v1)，REQ-A 更新一次(v2)；重试跳过 T-1，不重复累加
        result = self.service.import_changeset("REQ-A", cp1, [
            change("T-1", "T1"), change("T-2", "T2"), change("T-3", "T3"),
        ], "actor", "inspector")
        self.assertEqual(result["status"], "applied")
        t1 = self.repo.get_item_by_external_ref("T-1")
        self.assertEqual(t1["version"], 2)
        self.assertEqual(t1["title"], "T1")
        for ref in ("T-2", "T-3"):
            item = self.repo.get_item_by_external_ref(ref)
            self.assertIsNotNone(item)
            self.assertEqual(item["version"], 1)

    # ------------------------------------------------------------------
    # 回填
    # ------------------------------------------------------------------
    def test_backfill_existing_data_and_audit_still_queryable(self):
        # 直接用 repo 造存量数据：没有快照、没有检查点
        item = self.repo.create_item("存量缺陷", "回填前的缺陷", "major", 5, 10,
                                     "LEG-1", "legacy")
        self.repo.append_audit("create", "item", item["id"], "legacy",
                               {"title": "存量缺陷"})
        self.assertIsNone(self.repo.get_item_by_external_ref("LEG-1")["last_checkpoint"])
        # 启动服务即回填
        service = Service(self.repo)
        snapshots = service.list_snapshots("viewer")
        self.assertTrue(any(s["kind"] == "backfill" for s in snapshots))
        self.assertIsNotNone(self.repo.get_item_by_external_ref("LEG-1")["last_checkpoint"])
        # 审计轨迹仍可查、链仍可验证
        events = service.audit("viewer")
        self.assertTrue(any(e["action"] == "create" for e in events))
        self.assertTrue(self.repo.verify_audit_chain())
        # 回填幂等
        again = service.backfill_snapshot("system", "inspector")
        self.assertFalse(again["backfilled"])

    def test_snapshot_bundles_tasks_defects_and_audit(self):
        self.service.import_changeset("SETUP", GENESIS, [
            change("T-1", "初版", records=[
                {"kind": "evidence", "detail": "现场记录", "status": "closed",
                 "external_ref": "EV-1"}]),
        ], "actor", "inspector")
        snap = self.repo.get_snapshot(self.repo.get_latest_checkpoint())
        payload = snap["payload"]
        self.assertIn("items", payload)
        self.assertIn("records", payload)
        self.assertIn("audit", payload)
        self.assertGreaterEqual(payload["item_count"], 1)
        self.assertGreaterEqual(payload["record_count"], 1)
        self.assertGreaterEqual(payload["audit_count"], 1)
        self.assertTrue(snap["snapshot_hash"])


if __name__ == "__main__":
    unittest.main()
