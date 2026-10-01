import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError, PermissionDenied, SyncWriteError
from src.repository import Repository
from src.service import Service
from src.rules import STATES, TRANSITION_ROLES


def defect_op(ref, title, severity="major", quantity=5.0, threshold=10.0,
              base_version=0, actor="patrol-a", **extra):
    op = {"op": "defect_upsert", "external_ref": ref, "title": title,
          "description": f"desc-{title}", "severity": severity,
          "quantity": quantity, "threshold": threshold,
          "base_version": base_version, "actor": actor}
    op.update(extra)
    return op


def task_op(no, title, status="planned", base_version=0, actor="patrol-a"):
    return {"op": "task_upsert", "task_no": no, "title": title,
            "route": "堤段A", "scheduled_at": "2026-10-01T22:00:00+00:00",
            "status": status, "base_version": base_version, "actor": actor}


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def test_task_merge_by_task_no(self):
        r1 = self.service.submit_sync(
            {"request_no": "R-TASK-1", "node_id": "team-a",
             "ops": [task_op("NX-2026-07", "夜巡七组")]}, "patrol-a", "inspector")
        self.assertEqual(r1["results"][0]["status"], "created")
        # 两队都基于版本1改期：先到生效，后到留候选
        r2 = self.service.submit_sync(
            {"request_no": "R-TASK-2", "node_id": "team-a",
             "ops": [task_op("NX-2026-07", "夜巡七组-A改线", "rescheduled", 1, "patrol-a")]},
            "patrol-a", "inspector")
        self.assertEqual(r2["results"][0]["status"], "updated")
        r3 = self.service.submit_sync(
            {"request_no": "R-TASK-3", "node_id": "team-b",
             "ops": [task_op("NX-2026-07", "夜巡七组-B改线", "rescheduled", 1, "patrol-b")]},
            "patrol-b", "inspector")
        self.assertEqual(r3["results"][0]["status"], "conflict")
        task = self.service.list_tasks("viewer")[0]
        self.assertEqual(task["version"], 2)
        self.assertEqual(task["title"], "夜巡七组-A改线")  # 先到结果未被覆盖
        candidates = self.service.list_candidates("dam_engineer", "pending")
        self.assertEqual(len(candidates), 1)

    def test_defect_both_edited_first_wins_and_review_gates(self):
        self.service.submit_sync(
            {"request_no": "R-1", "node_id": "team-a",
             "ops": [defect_op("DEF-7", "裂缝", quantity=5.0)]}, "a", "inspector")
        item = self.repo.get_item_by_external_ref("DEF-7")
        self.assertEqual(item["version"], 1)
        # 两队同时基于版本1修改
        first = self.service.submit_sync(
            {"request_no": "R-2", "node_id": "team-a",
             "ops": [defect_op("DEF-7", "裂缝-A扩宽", quantity=8.0, base_version=1)]},
            "a", "inspector")
        self.assertEqual(first["results"][0]["status"], "updated")
        second = self.service.submit_sync(
            {"request_no": "R-3", "node_id": "team-b",
             "ops": [defect_op("DEF-7", "裂缝-B渗水", quantity=12.0, base_version=1)]},
            "b", "inspector")
        self.assertEqual(second["results"][0]["status"], "conflict")
        candidate_id = second["conflict_candidates"][0]
        # 先到结果生效，后到内容仅入候选
        item = self.repo.get_item_by_external_ref("DEF-7")
        self.assertEqual(item["quantity"], 8.0)
        self.assertEqual(item["version"], 2)
        # 复核前不能派发应急任务
        with self.assertRaises(ConflictError):
            self.service.dispatch_emergency(
                item["id"], {"reason": "渗水险情"}, "em", "emergency_manager")
        # 复核前不能关闭缺陷：推进到 verified 后关闭被拦截
        current = self.service.enrich(item)
        self.service.add_record(
            item["id"], {"kind": "evidence", "detail": "复检证据", "status": "closed",
                         "external_ref": "EV-X"}, "r", "inspector")
        for target in STATES[1:5]:
            current = self.service.transition(
                current["id"], target, current["version"], "rv",
                TRANSITION_ROLES[target][0])
        with self.assertRaises(ConflictError):
            self.service.transition(
                current["id"], STATES[5], current["version"], "em",
                TRANSITION_ROLES[STATES[5]][0])
        # 复核（驳回后到内容）后闸门解除
        self.service.resolve_candidate(
            candidate_id, {"decision": "reject", "note": "维持先到结果"},
            "eng", "dam_engineer")
        current = self.service.get_item(current["id"], "viewer")
        closed = self.service.transition(
            current["id"], STATES[5], current["version"], "em",
            TRANSITION_ROLES[STATES[5]][0])
        self.assertEqual(closed["status"], "closed")
        dispatch = self.service.dispatch_emergency(
            closed["id"], {"reason": "残余风险监测"}, "em", "emergency_manager")
        self.assertEqual(dispatch["item_id"], closed["id"])

    def test_duplicate_import_uses_first_result(self):
        payload = {"request_no": "R-DUP", "node_id": "team-a",
                   "ops": [defect_op("DUP-1", "重复导入缺陷")]}
        first = self.service.submit_sync(payload, "a", "inspector")
        again = self.service.submit_sync(payload, "a", "inspector")
        self.assertTrue(again["deduplicated"])
        self.assertEqual(again["results"], first["results"])
        self.assertEqual(len(self.repo.list_items()), 1)
        head_after_first = self.repo.snapshot_head()
        third = self.service.submit_sync(payload, "a", "inspector")
        self.assertTrue(third["deduplicated"])
        self.assertEqual(self.repo.snapshot_head(), head_after_first)

    def test_duplicate_defect_record_is_idempotent(self):
        self.service.submit_sync(
            {"request_no": "R-REC-1", "node_id": "team-a", "ops": [
                defect_op("DEF-REC", "记录缺陷"),
                {"op": "defect_record", "external_ref": "DEF-REC",
                 "record_ref": "OBS-1", "kind": "seepage", "detail": "左岸渗流",
                 "actor": "a"},
            ]}, "a", "inspector")
        # 新请求号但同一 record_ref 重复导入：沿用第一次结果，不产生第二条
        retry = self.service.submit_sync(
            {"request_no": "R-REC-2", "node_id": "team-a", "ops": [
                {"op": "defect_record", "external_ref": "DEF-REC",
                 "record_ref": "OBS-1", "kind": "seepage", "detail": "左岸渗流",
                 "actor": "a"},
            ]}, "a", "inspector")
        self.assertEqual(retry["results"][0]["status"], "duplicate")
        item = self.repo.get_item_by_external_ref("DEF-REC")
        self.assertEqual(len(self.repo.list_records(item["id"])), 1)

    def test_write_failure_resumes_from_checkpoint(self):
        calls = {"n": 0}

        def fault(stage, ctx):
            if stage == "apply_before_checkpoint" and ctx["op_index"] == 1:
                calls["n"] += 1
                raise RuntimeError("disk full")

        self.repo.fault_hook = fault
        batch = {"request_no": "R-RESUME", "node_id": "team-a", "ops": [
            defect_op("RSM-1", "第一条"),
            defect_op("RSM-2", "第二条"),
        ]}
        with self.assertRaises(SyncWriteError) as cm:
            self.service.submit_sync(batch, "a", "inspector")
        self.assertEqual(cm.exception.checkpoint, 1)
        self.assertIsNotNone(self.repo.get_item_by_external_ref("RSM-1"))
        self.assertIsNone(self.repo.get_item_by_external_ref("RSM-2"))
        # 回网后按同一请求号续传重试：检查点0沿用结果，操作1补做
        self.repo.fault_hook = None
        done = self.service.submit_sync(batch, "a", "inspector")
        self.assertEqual(done["status"], "completed")
        statuses = [r["status"] for r in done["results"]]
        self.assertEqual(statuses, ["created", "created"])
        self.assertEqual(len(self.repo.list_items()), 2)
        cp0 = self.repo.get_checkpoint("R-RESUME", 0)
        cp1 = self.repo.get_checkpoint("R-RESUME", 1)
        self.assertEqual(cp0["status"], "done")
        self.assertEqual(cp1["status"], "done")
        # 续传完成后再提交同请求号：直接沿用第一次结果
        again = self.service.submit_sync(batch, "a", "inspector")
        self.assertTrue(again["deduplicated"])
        self.assertTrue(self.repo.verify_audit_chain())

    def test_snapshot_backfill_for_legacy_data(self):
        # 模拟旧库：业务数据与审计存在、快照缺失
        self.service.create_item(
            {"title": "legacy", "description": "旧数据", "severity": "major",
             "external_ref": "OLD-1"}, "old", "inspector")
        with self.repo.conn:
            self.repo.conn.execute("DELETE FROM snapshot_log")
        self.assertEqual(self.repo.snapshot_head(), 0)
        # 重新打开已有库时自动回填快照，审计轨迹仍可查
        self.repo.close()
        reopened = Repository(str(Path(self.tmp.name) / "test.db"))
        snap = reopened.get_snapshot(0)
        audit_count = len(reopened.list_audit())
        self.assertGreaterEqual(snap["head"], audit_count)
        self.assertTrue(reopened.verify_audit_chain())
        # 回填幂等（再次打开旧库也不会重复回填）
        reopened.close()
        again_repo = Repository(str(Path(self.tmp.name) / "test.db"))
        again = again_repo.backfill_snapshots()
        self.assertEqual(again["backfilled"], 0)
        again_repo.close()

    def test_snapshot_feed_and_accept_flow(self):
        result = self.service.submit_sync(
            {"request_no": "R-FEED", "node_id": "team-a",
             "ops": [defect_op("FEED-1", "候选采纳", quantity=3.0)]},
            "a", "inspector")
        # 两队都基于版本1修改：A先到生效，B后到留候选
        self.service.submit_sync(
            {"request_no": "R-FEED-2", "node_id": "team-a", "ops": [
                defect_op("FEED-1", "候选采纳-A", quantity=9.0, base_version=1)]},
            "a", "inspector")
        conflict = self.service.submit_sync(
            {"request_no": "R-FEED-3", "node_id": "team-b", "ops": [
                defect_op("FEED-1", "候选采纳-B", quantity=20.0, base_version=1)]},
            "b", "inspector")
        self.assertEqual(conflict["results"][0]["status"], "conflict")
        page1 = self.service.snapshot("viewer", 0)
        self.assertGreaterEqual(page1["head"], 3)
        self.assertIn("request_no", page1["entries"][0])
        page2 = self.service.snapshot("viewer", page1["head"])
        self.assertEqual(page2["entries"], [])
        # 复核采纳后到内容，版本继续递增，候选关闭
        candidate = self.service.list_candidates("dam_engineer")[0]
        resolved = self.service.resolve_candidate(
            candidate["id"], {"decision": "accept", "note": "以现场复核为准"},
            "eng", "emergency_manager")
        self.assertEqual(resolved["candidate"]["status"], "accepted")
        item = self.repo.get_item_by_external_ref("FEED-1")
        self.assertEqual(item["quantity"], 20.0)
        self.assertEqual(resolved["applied"]["version"], 3)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_sync_requires_role(self):
        with self.assertRaises(PermissionDenied):
            self.service.submit_sync(
                {"request_no": "RX", "ops": [task_op("T1", "x")]},
                "v", "viewer")


if __name__ == "__main__":
    unittest.main()
