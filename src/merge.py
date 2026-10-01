"""离线合并的纯领域逻辑。

两队夜巡断网各自改任务，回网后按任务号（external_ref）合并缺陷。
两边都改过同一条缺陷时保留候选：先到的版本先生效，后到的内容按
请求号留冲突，复核前不能关闭或派发应急任务。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .snapshot import is_newer

CONFLICT_PENDING = "pending_review"
CONFLICT_RESOLVED = "resolved"

# 冲突中代表“先到版本”的候选请求号（基线，非某次导入）
BASELINE_REQUEST_NO = "BASELINE"


@dataclass
class Candidate:
    request_no: str
    actor: str
    payload: Dict[str, Any]
    submitted_at: str
    baseline: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_no": self.request_no,
            "actor": self.actor,
            "payload": self.payload,
            "submitted_at": self.submitted_at,
            "baseline": self.baseline,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Candidate":
        return cls(
            request_no=data["request_no"],
            actor=data.get("actor", ""),
            payload=data["payload"],
            submitted_at=data["submitted_at"],
            baseline=bool(data.get("baseline", False)),
        )


@dataclass
class Conflict:
    external_ref: str
    item_id: Optional[int]
    candidates: List[Candidate] = field(default_factory=list)
    status: str = CONFLICT_PENDING
    winning_request_no: Optional[str] = None
    resolution: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "external_ref": self.external_ref,
            "item_id": self.item_id,
            "candidates": [c.to_dict() for c in self.candidates],
            "status": self.status,
            "winning_request_no": self.winning_request_no,
            "resolution": self.resolution,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Conflict":
        return cls(
            external_ref=data["external_ref"],
            item_id=data.get("item_id"),
            candidates=[Candidate.from_dict(c) for c in data.get("candidates", [])],
            status=data.get("status", CONFLICT_PENDING),
            winning_request_no=data.get("winning_request_no"),
            resolution=data.get("resolution"),
        )

    def has_candidate(self, request_no: str) -> bool:
        return any(c.request_no == request_no for c in self.candidates)

    def candidate(self, request_no: str) -> Optional[Candidate]:
        for c in self.candidates:
            if c.request_no == request_no:
                return c
        return None


def is_branched(last_checkpoint: Optional[str], base_checkpoint: str) -> bool:
    """该缺陷在队伍分支之后是否又被别的检查点改过。

    分支点 base 已包含的改动不算冲突；只有 base 之后的改动才说明两边
    独立修改了同一条缺陷。
    """
    if not last_checkpoint:
        return False
    return is_newer(last_checkpoint, base_checkpoint)


def classify_changes(changes: List[Dict[str, Any]], base_checkpoint: str,
                     last_checkpoint_of, pending_conflict_refs) -> "tuple[list, list]":
    """把变更分成可干净应用的与需要保留候选冲突的。

    last_checkpoint_of(ref) -> 该缺陷最近一次被导入写入的检查点（可空）。
    pending_conflict_refs -> 已有待复核冲突的任务号集合。
    """
    clean: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []
    for change in changes:
        ref = change["external_ref"]
        if is_branched(last_checkpoint_of(ref), base_checkpoint):
            conflicts.append(change)
        else:
            clean.append(change)
    return clean, conflicts
