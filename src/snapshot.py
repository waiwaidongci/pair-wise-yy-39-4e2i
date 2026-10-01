"""可续传快照：巡检任务、缺陷与审计链共用同一份快照。

快照是一次离线变更合并的检查点载体。每次导入（或回填）都会生成一个
单调递增的检查点 ``checkpoint``，把当时的任务、缺陷和审计链整体打包。
写入按步骤推进并落盘，失败后可凭检查点从断点续传，而不是从头再来。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

GENESIS = "GENESIS"

# 导入写入的推进步骤（检查点）
STEP_RECEIVED = "received"
STEP_CLASSIFIED = "classified"
STEP_APPLIED = "applied"
STEP_FINALIZED = "finalized"

STATUS_BUILDING = "building"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"

KIND_IMPORT = "import"
KIND_BACKFILL = "backfill"
KIND_RESOLUTION = "resolution"

CP_PREFIX = "cp-"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_checkpoint(seq: int) -> str:
    return f"{CP_PREFIX}{seq:012d}"


def cp_number(checkpoint: str) -> int:
    """从检查点字符串中解析单调序号；GENESIS 或非法值返回 0。"""
    if not checkpoint or checkpoint == GENESIS or not checkpoint.startswith(CP_PREFIX):
        return 0
    try:
        return int(checkpoint[len(CP_PREFIX):])
    except ValueError:
        return 0


def is_newer(candidate: str, base: str) -> bool:
    """candidate 检查点是否晚于 base（即分支之后又被改过）。"""
    return cp_number(candidate) > cp_number(base)


def snapshot_hash(checkpoint: str, base_checkpoint: str, payload: Dict[str, Any]) -> str:
    """对快照内容做哈希，形成快照自身的链。"""
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    head = f"{checkpoint}|{base_checkpoint}|".encode("utf-8")
    return hashlib.sha256(head + raw).hexdigest()


def build_bundle(items: List[Dict[str, Any]], records: List[Dict[str, Any]],
                 audit_events: List[Dict[str, Any]], audit_head_hash: str) -> Dict[str, Any]:
    """把任务、缺陷和审计链打包成同一份快照载荷。"""
    return {
        "items": items,
        "records": records,
        "audit": audit_events,
        "audit_head_hash": audit_head_hash,
        "item_count": len(items),
        "record_count": len(records),
        "audit_count": len(audit_events),
    }
