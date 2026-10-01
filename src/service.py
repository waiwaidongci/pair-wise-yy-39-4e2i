from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import (ConflictError, ensure_role, normalize_severity,
                     require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, DISPATCH_ROLES, ENTITY,
                    RECORD_ROLES, REVIEW_ROLES, TERMINAL_STATES, VIEW_ROLES,
                    completion_blockers, escalation_required, priority_score,
                    response_deadline_hours, review_blockers,
                    role_for_transition, validate_transition)
from .sync import SyncEngine


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository
        self.sync = SyncEngine(repository)

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

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
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if target in TERMINAL_STATES:
            blockers += review_blockers(self.repository.pending_candidate_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

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
    # 断网续传
    # ------------------------------------------------------------------
    def submit_sync(self, payload: Dict[str, Any], actor: str,
                    role: str) -> Dict[str, Any]:
        return self.sync.submit_batch(payload, actor, role)

    def list_tasks(self, role: str) -> list:
        self._view(role)
        return self.repository.list_tasks()

    def list_candidates(self, role: str,
                        status: Optional[str] = None) -> list:
        ensure_role(role, REVIEW_ROLES)
        return self.repository.list_candidates(status)

    def resolve_candidate(self, candidate_id: int, payload: Dict[str, Any],
                          actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, REVIEW_ROLES)
        actor = require_text(actor, "actor", 100)
        decision = payload.get("decision")
        if decision not in ("accept", "reject"):
            from .domain import ValidationError
            raise ValidationError("decision必须是accept或reject")
        note = payload.get("note", "")
        if note is None:
            note = ""
        elif not isinstance(note, str):
            from .domain import ValidationError
            raise ValidationError("note必须是字符串")
        note = note.strip()
        if len(note) > 500:
            from .domain import ValidationError
            raise ValidationError("note不能超过500个字符")
        return self.repository.resolve_candidate(candidate_id, decision, actor, note)

    def dispatch_emergency(self, item_id: int, payload: Dict[str, Any],
                           actor: str, role: str,
                           request_no: Optional[str] = None) -> Dict[str, Any]:
        ensure_role(role, DISPATCH_ROLES)
        actor = require_text(actor, "actor", 100)
        reason = require_text(payload.get("reason"), "reason", 2000)
        item = self.repository.get_item(item_id)
        blockers = review_blockers(self.repository.pending_candidate_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        return self.repository.create_dispatch(item_id, reason, actor, request_no)

    def list_dispatches(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, DISPATCH_ROLES.union(AUDIT_ROLES))
        return self.repository.list_dispatches(item_id)

    def snapshot(self, role: str, after: int = 0) -> Dict[str, Any]:
        self._view(role)
        if not isinstance(after, int) or after < 0:
            from .domain import ValidationError
            raise ValidationError("after必须是非负整数")
        return self.repository.get_snapshot(after)

    def backfill_snapshots(self, role: str, actor: str = "system") -> Dict[str, int]:
        ensure_role(role, REVIEW_ROLES)
        return self.repository.backfill_snapshots(actor)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
