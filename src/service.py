from __future__ import annotations

from typing import Any, Dict, List, Optional

from .domain import (ConflictError, NotFoundError, PermissionDenied,
                     ValidationError, ensure_role, normalize_severity,
                     require_number, require_text)
from .merge import BASELINE_REQUEST_NO, Candidate, is_branched
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, RECORD_ROLES, TITLE,
                    VIEW_ROLES, completion_blockers, escalation_required,
                    priority_score, response_deadline_hours, role_for_transition,
                    validate_transition)
from .snapshot import (GENESIS, STEP_APPLIED, STEP_CLASSIFIED, STEP_FINALIZED,
                       STEP_RECEIVED, snapshot_hash, utc_now)

IMPORT_ROLES = set(["inspector", "dam_engineer"])
REVIEW_ROLES = set(["dam_engineer", "emergency_manager"])
DISPATCH_ROLES = set(["emergency_manager", "dam_engineer"])
TERMINAL_REQUEST_STATUSES = ("applied", "conflict")


class Service:
    def __init__(self, repository: Repository, on_step=None, on_progress=None):
        self.repository = repository
        # 测试接缝：每个检查点步骤落盘后回调，抛异常即模拟写入中断
        self.on_step = on_step
        # 测试接缝：每个缺陷写入进度落盘后回调，抛异常即模拟中途崩溃
        self.on_progress = on_progress
        # 已有数据缺检查点时，启动即回填快照
        self.repository.backfill_snapshot("system")

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    # ------------------------------------------------------------------
    # 基础用例
    # ------------------------------------------------------------------
    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        # 复核前不能关闭：存在待复核冲突候选的缺陷一律冻结
        if target == "closed" and self._has_pending_conflict(item):
            raise ConflictError("该缺陷存在待复核的冲突候选，复核前不能关闭")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def dispatch_emergency(self, item_id: int, actor: str, role: str) -> Dict[str, Any]:
        """派发应急任务；缺陷在冲突复核前冻结，不能派发。"""
        ensure_role(role, DISPATCH_ROLES)
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        if self._has_pending_conflict(item):
            raise ConflictError("该缺陷存在待复核的冲突候选，复核前不能派发应急任务")
        self.repository.append_audit("emergency_dispatch", ENTITY, item_id, actor, {
            "severity": item["severity"],
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(item)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    # ------------------------------------------------------------------
    # 离线导入：可续传快照 + 请求号幂等 + 冲突候选
    # ------------------------------------------------------------------
    def import_changeset(self, request_no: str, base_checkpoint: Optional[str],
                         changes: List[Dict[str, Any]], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, IMPORT_ROLES)
        request_no = require_text(request_no, "request_no", 100)
        actor = require_text(actor, "actor", 100)
        if not isinstance(changes, list) or not changes:
            raise ValidationError("changes必须是非空列表")
        norm_changes = [self._validate_change(c) for c in changes]
        base_checkpoint = self._resolve_base(base_checkpoint)

        # 重复导入沿用第一次结果（请求号幂等）
        existing = self.repository.get_request(request_no)
        if existing is not None and existing["status"] in TERMINAL_REQUEST_STATUSES:
            return existing["result"]

        snap = self.repository.get_snapshot_by_request(request_no)
        if snap is None:
            cp = self.repository.create_pending_snapshot(request_no, base_checkpoint, actor)
            step = STEP_RECEIVED
        else:
            cp = snap["checkpoint"]
            step = snap["step"]
            base_checkpoint = snap["base_checkpoint"]

        clean: List[Dict[str, Any]] = []
        conflicts: List[Dict[str, Any]] = []
        if step == STEP_RECEIVED:
            self.repository.advance_snapshot(cp, STEP_RECEIVED, {"changes": norm_changes})
            self._hook(cp, STEP_RECEIVED)
            clean, conflicts = self._classify(norm_changes, base_checkpoint)
            self.repository.advance_snapshot(cp, STEP_CLASSIFIED, {
                "changes": norm_changes, "clean": clean, "conflicts": conflicts})
            self._hook(cp, STEP_CLASSIFIED)
        elif step == STEP_CLASSIFIED:
            clean = snap["payload"].get("clean", [])
            conflicts = snap["payload"].get("conflicts", [])

        if step in (STEP_RECEIVED, STEP_CLASSIFIED):
            self._apply(cp, clean, conflicts, request_no, actor)
            self.repository.advance_snapshot(cp, STEP_APPLIED)
            self._hook(cp, STEP_APPLIED)

        snap_row = self.repository.get_snapshot(cp)
        bundle = self.repository.bundle()
        h = snapshot_hash(cp, base_checkpoint, bundle)
        self.repository.finalize_snapshot(cp, bundle, h)
        self._hook(cp, STEP_FINALIZED)

        status = "conflict" if conflicts else "applied"
        result = {
            "request_no": request_no,
            "checkpoint": cp,
            "base_checkpoint": base_checkpoint,
            "status": status,
            "clean": [c["external_ref"] for c in clean],
            "conflicts": [c["external_ref"] for c in conflicts],
            "snapshot_hash": h,
        }
        self.repository.finish_request(request_no, status, result)
        return result

    def list_snapshots(self, role: str) -> list:
        self._view(role)
        return self.repository.list_snapshots()

    def backfill_snapshot(self, actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, IMPORT_ROLES)
        actor = require_text(actor, "actor", 100)
        snap = self.repository.backfill_snapshot(actor)
        return {"backfilled": snap is not None, "snapshot": snap}

    # ------------------------------------------------------------------
    # 冲突复核
    # ------------------------------------------------------------------
    def list_conflicts(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return self.repository.list_conflicts(status)

    def review_conflict(self, conflict_id: int, winning_request_no: str,
                        actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, REVIEW_ROLES)
        actor = require_text(actor, "actor", 100)
        conflict = self.repository.get_conflict(conflict_id)
        if conflict is None:
            raise NotFoundError("冲突不存在")
        if conflict["status"] != "pending_review":
            raise ConflictError("该冲突已复核")
        winning_request_no = require_text(winning_request_no, "winning_request_no", 100)
        candidate = next((c for c in conflict["candidates"]
                          if c["request_no"] == winning_request_no), None)
        if candidate is None:
            raise ValidationError("获胜请求号不在候选中")

        base = self.repository.get_latest_checkpoint()
        resolve_request_no = f"__resolve__{conflict_id}__{winning_request_no}"
        cp = self.repository.create_pending_snapshot(resolve_request_no, base, actor,
                                                     kind="resolution")
        item = self.repository.get_item_by_external_ref(conflict["external_ref"])
        if item is not None and winning_request_no != BASELINE_REQUEST_NO:
            self.repository.update_import_item(item["id"], candidate["payload"], cp)
        bundle = self.repository.bundle()
        h = snapshot_hash(cp, base, bundle)
        self.repository.finalize_snapshot(cp, bundle, h)
        self.repository.finish_request(resolve_request_no, "resolved", {
            "conflict_id": conflict_id, "winning_request_no": winning_request_no})

        resolution = {
            "winning_request_no": winning_request_no,
            "reviewed_by": actor,
            "reviewed_at": utc_now(),
            "checkpoint": cp,
        }
        self.repository.resolve_conflict(conflict_id, winning_request_no, resolution)
        if item is not None:
            self.repository.append_audit("conflict_resolved", ENTITY, item["id"], actor,
                                         {"conflict_id": conflict_id,
                                          "winning_request_no": winning_request_no})
        return {"conflict": self.repository.get_conflict(conflict_id),
                "item": self.enrich(item) if item else None}

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------
    def _hook(self, cp: str, step: str) -> None:
        if self.on_step is not None:
            try:
                self.on_step(step)
            except Exception:
                self.repository.fail_snapshot(cp)
                raise

    def _progress_hook(self, cp: str, external_ref: str, step: str) -> None:
        if self.on_progress is not None:
            try:
                self.on_progress(cp, external_ref, step)
            except Exception:
                self.repository.fail_snapshot(cp)
                raise

    def _resolve_base(self, base_checkpoint: Optional[str]) -> str:
        if not base_checkpoint:
            return GENESIS
        base_checkpoint = require_text(base_checkpoint, "base_checkpoint", 100)
        if base_checkpoint != GENESIS and not self.repository.snapshot_exists(base_checkpoint):
            raise ValidationError("未知base_checkpoint")
        return base_checkpoint

    def _validate_change(self, ch: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(ch, dict):
            raise ValidationError("change必须是对象")
        ref = require_text(ch.get("external_ref"), "external_ref", 100)
        title = require_text(ch.get("title"), "title", 200)
        description = require_text(ch.get("description"), "description")
        severity = normalize_severity(ch.get("severity"))
        quantity = require_number(ch.get("quantity", 0), "quantity")
        threshold = require_number(ch.get("threshold", 1), "threshold", 0.000001)
        records = ch.get("records", [])
        norm_records: List[Dict[str, Any]] = []
        if records is not None:
            if not isinstance(records, list):
                raise ValidationError("records必须是列表")
            for rec in records:
                if not isinstance(rec, dict):
                    raise ValidationError("record必须是对象")
                kind = require_text(rec.get("kind"), "kind", 100)
                detail = require_text(rec.get("detail"), "detail")
                status = rec.get("status", "open")
                if status not in ("open", "closed"):
                    raise ValidationError("record status必须是open或closed")
                rref = rec.get("external_ref")
                if rref is not None:
                    rref = require_text(rref, "external_ref", 100)
                norm_records.append({"kind": kind, "detail": detail,
                                     "status": status, "external_ref": rref})
        return {"external_ref": ref, "title": title, "description": description,
                "severity": severity, "quantity": quantity, "threshold": threshold,
                "records": norm_records}

    def _classify(self, changes: List[Dict[str, Any]], base: str):
        clean: List[Dict[str, Any]] = []
        conflicts: List[Dict[str, Any]] = []
        for ch in changes:
            existing = self.repository.get_item_by_external_ref(ch["external_ref"])
            last_cp = existing["last_checkpoint"] if existing else None
            if is_branched(last_cp, base):
                conflicts.append(ch)
            else:
                clean.append(ch)
        return clean, conflicts

    def _apply(self, cp: str, clean: List[Dict[str, Any]],
               conflicts: List[Dict[str, Any]], request_no: str, actor: str) -> None:
        snap = self.repository.get_snapshot(cp)
        snap_id = snap["id"]
        for ch in clean:
            ref = ch["external_ref"]
            if self.repository.has_progress(cp, ref, "written"):
                continue
            existing = self.repository.get_item_by_external_ref(ref)
            if existing is None:
                item = self.repository.create_import_item(ch, cp, actor)
            else:
                item = self.repository.update_import_item(existing["id"], ch, cp)
            for rec in ch.get("records", []):
                self.repository.add_import_record(item["id"], rec, cp, actor)
            self.repository.mark_progress(cp, ref, "written")
            self._progress_hook(cp, ref, "written")
        for ch in conflicts:
            ref = ch["external_ref"]
            if self.repository.has_progress(cp, ref, "conflict"):
                continue
            existing = self.repository.get_item_by_external_ref(ref)
            conflict = self.repository.get_conflict_by_ref(ref)
            incoming = Candidate(request_no, actor, ch, utc_now()).to_dict()
            if conflict is None:
                baseline = Candidate(
                    BASELINE_REQUEST_NO,
                    existing["created_by"] if existing else actor,
                    self._item_payload(existing) if existing else ch,
                    existing["updated_at"] if existing else utc_now(),
                    baseline=True).to_dict()
                self.repository.insert_conflict(ref, existing["id"] if existing else None,
                                                [baseline, incoming])
            else:
                self.repository.add_conflict_candidate(conflict["id"], incoming)
            self.repository.mark_progress(cp, ref, "conflict")
            self._progress_hook(cp, ref, "conflict")
        self.repository.append_audit("import", "snapshot", snap_id, actor, {
            "request_no": request_no,
            "base_checkpoint": snap["base_checkpoint"],
            "clean": [c["external_ref"] for c in clean],
            "conflicts": [c["external_ref"] for c in conflicts],
        })
        for ch in conflicts:
            existing = self.repository.get_item_by_external_ref(ch["external_ref"])
            if existing is not None:
                self.repository.append_audit("conflict_detected", ENTITY, existing["id"], actor, {
                    "request_no": request_no, "external_ref": ch["external_ref"],
                })

    def _item_payload(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "external_ref": item["external_ref"],
            "title": item["title"],
            "description": item["description"],
            "severity": item["severity"],
            "quantity": item["quantity"],
            "threshold": item["threshold"],
        }

    def _has_pending_conflict(self, item: Dict[str, Any]) -> bool:
        ref = item.get("external_ref")
        if not ref:
            return False
        conflict = self.repository.get_conflict_by_ref(ref)
        return conflict is not None and conflict["status"] == "pending_review"

    def enrich(self, item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        result["last_checkpoint"] = item.get("last_checkpoint")
        result["has_pending_conflict"] = self._has_pending_conflict(item)
        result["emergency_blocked"] = result["has_pending_conflict"]
        return result
