from __future__ import annotations

from typing import Any, Dict, List

from .domain import (SyncWriteError, ValidationError, ensure_role,
                     normalize_severity, require_number, require_text)
from .rules import SYNC_ROLES, normalize_task_status
from .repository import Repository


class SyncEngine:
    """断网续传：巡检任务、缺陷与审计链共用检查点快照。

    - 同 request_no 重复导入：沿用第一次的批次结果，不再执行任何操作
    - 每条操作以 (request_no, op_index) 记录检查点，已完成的跳过
    - 写入失败后按同一请求号重放，即从检查点继续
    """

    def __init__(self, repository: Repository):
        self.repository = repository

    def submit_batch(self, payload: Dict[str, Any], actor: str,
                     role: str) -> Dict[str, Any]:
        ensure_role(role, SYNC_ROLES)
        actor = require_text(actor, "actor", 100)
        request_no = require_text(payload.get("request_no"), "request_no", 100)
        node_id = require_text(payload.get("node_id", actor), "node_id", 100)
        ops = payload.get("ops")
        if not isinstance(ops, list) or not ops:
            raise ValidationError("ops必须是非空数组")
        if len(ops) > 1000:
            raise ValidationError("单批操作不能超过1000条")

        existing = self.repository.begin_sync_request(request_no, node_id, len(ops))
        if existing["status"] == "completed":
            # 重复导入沿用第一次结果
            import json as _json
            stored = _json.loads(existing["result"]) if existing["result"] else {}
            stored["status"] = "completed"
            stored["deduplicated"] = True
            return stored

        normalized = [self._normalize_op(op, node_id, actor) for op in ops]

        results: List[Dict[str, Any]] = []
        last_done = -1
        try:
            for index, op in enumerate(normalized):
                result = self.repository.apply_sync_op(op, request_no, index, node_id)
                results.append(result)
                last_done = index
        except Exception as exc:
            self.repository.mark_sync_failed(request_no)
            raise SyncWriteError(
                f"同步写入中断，可凭请求号 {request_no} 与检查点续传重试: {exc}",
                request_no=request_no, checkpoint=last_done + 1,
            ) from exc

        summary = self._summarize(results)
        result_payload = {
            "request_no": request_no, "node_id": node_id,
            "total_ops": len(normalized), "results": results, **summary,
        }
        self.repository.complete_sync_request(request_no, result_payload)
        result_payload["status"] = "completed"
        result_payload["deduplicated"] = False
        return result_payload

    def _normalize_op(self, raw: Any, node_id: str, actor: str) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValidationError("每条操作必须是JSON对象")
        kind = raw.get("op")
        if kind == "task_upsert":
            route = raw.get("route", "")
            scheduled_at = raw.get("scheduled_at", "")
            route = require_text(route, "route", 500) if str(route or "").strip() else ""
            scheduled_at = (require_text(scheduled_at, "scheduled_at", 100)
                            if str(scheduled_at or "").strip() else "")
            op = {
                "op": kind,
                "task_no": require_text(raw.get("task_no"), "task_no", 100),
                "title": require_text(raw.get("title"), "title", 200),
                "route": route,
                "scheduled_at": scheduled_at,
                "status": normalize_task_status(raw.get("status", "planned")),
                "base_version": int(require_number(raw.get("base_version", 0), "base_version")),
                "actor": require_text(raw.get("actor", actor), "actor", 100),
            }
        elif kind == "defect_upsert":
            op = {
                "op": kind,
                "external_ref": require_text(raw.get("external_ref"), "external_ref", 100),
                "title": require_text(raw.get("title"), "title", 200),
                "description": require_text(raw.get("description"), "description"),
                "severity": normalize_severity(raw.get("severity")),
                "quantity": require_number(raw.get("quantity", 0), "quantity"),
                "threshold": require_number(raw.get("threshold", 1), "threshold", 0.000001),
                "base_version": int(require_number(raw.get("base_version", 0), "base_version")),
                "actor": require_text(raw.get("actor", actor), "actor", 100),
            }
        elif kind == "defect_record":
            status = raw.get("status", "open")
            if status not in ("open", "closed"):
                raise ValidationError("record status必须是open或closed")
            op = {
                "op": kind,
                "external_ref": require_text(raw.get("external_ref"), "external_ref", 100),
                "record_ref": require_text(raw.get("record_ref"), "record_ref", 100),
                "kind": require_text(raw.get("kind"), "kind", 100),
                "detail": require_text(raw.get("detail"), "detail"),
                "status": status,
                "actor": require_text(raw.get("actor", actor), "actor", 100),
            }
        else:
            raise ValidationError(f"未知同步操作: {kind}")
        return op

    @staticmethod
    def _summarize(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        conflicts = [r for r in results if r.get("status") == "conflict"]
        created = sum(1 for r in results if r.get("status") in ("created", "duplicate"))
        updated = sum(1 for r in results if r.get("status") == "updated")
        unchanged = sum(1 for r in results if r.get("status") == "no_change")
        return {
            "created": created, "updated": updated, "unchanged": unchanged,
            "conflict_count": len(conflicts),
            "conflict_candidates": [r["candidate_id"] for r in conflicts],
        }
