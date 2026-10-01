from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ENTITY_SNAPSHOT, STATES, TASK_STATUSES


def canonical_hash(payload: Any) -> str:
    # 与 audit.calculate_hash 保持一致的规范化序列化
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        # 测试用故障注入：fn(stage, context)，抛出异常即模拟写入失败
        self.fault_hook: Optional[Callable[[str, dict], None]] = None
        self._create_schema()
        # 已有数据缺检查点/快照时自动回填
        self.backfill_snapshots()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        task_statuses = ",".join("'" + s + "'" for s in TASK_STATUSES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_no TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    route TEXT NOT NULL DEFAULT '',
                    scheduled_at TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL CHECK(status IN ({task_statuses})),
                    base_version INTEGER NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS emergency_dispatches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id),
                    request_no TEXT,
                    reason TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_requests (
                    request_no TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL CHECK(status IN ('processing','completed','failed')),
                    total_ops INTEGER NOT NULL DEFAULT 0,
                    result TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_checkpoints (
                    request_no TEXT NOT NULL,
                    op_index INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('done','failed')),
                    result TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(request_no, op_index)
                );
                CREATE TABLE IF NOT EXISTS sync_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL CHECK(entity_type IN ('巡检任务','大坝缺陷')),
                    entity_key TEXT NOT NULL,
                    task_no TEXT,
                    item_id INTEGER,
                    request_no TEXT NOT NULL,
                    op_index INTEGER NOT NULL,
                    node_id TEXT NOT NULL DEFAULT '',
                    actor TEXT NOT NULL DEFAULT '',
                    base_version INTEGER,
                    incoming TEXT NOT NULL,
                    current TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','accepted','rejected')),
                    resolution TEXT,
                    created_by TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    UNIQUE(entity_type, entity_key, request_no, op_index)
                );
                CREATE TABLE IF NOT EXISTS snapshot_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    op TEXT NOT NULL,
                    request_no TEXT,
                    payload_hash TEXT NOT NULL,
                    actor TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
            """)

    # ------------------------------------------------------------------
    # 内部原语：调用方必须已持有 self._lock 且处于事务中
    # ------------------------------------------------------------------
    def _maybe_fault(self, stage: str, context: dict) -> None:
        if callable(self.fault_hook):
            self.fault_hook(stage, context)

    def _insert_audit_locked(self, action: str, entity_type: str, entity_id: int,
                             actor: str, detail: dict) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous)
        self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"], event["created_at"]),
        )
        event["id"] = self.conn.execute(
            "SELECT id FROM audit_events WHERE entry_hash=?", (event["entry_hash"],)
        ).fetchone()["id"]
        return event

    def _append_snapshot_locked(self, entity_type: str, entity_id: Any, op: str,
                                payload_hash: str, actor: str = "",
                                request_no: Optional[str] = None,
                                created_at: Optional[str] = None) -> int:
        cur = self.conn.execute(
            """INSERT INTO snapshot_log(entity_type, entity_id, op, request_no,
               payload_hash, actor, created_at) VALUES(?,?,?,?,?,?,?)""",
            (entity_type, str(entity_id), op, request_no, payload_hash, actor,
             created_at or utc_now()),
        )
        return int(cur.lastrowid)

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    # ------------------------------------------------------------------
    # 缺陷（items）
    # ------------------------------------------------------------------
    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def get_item_by_external_ref(self, external_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE external_ref=?", (external_ref,)
            ).fetchone()
        return self._item(row) if row else None

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------
    # 审计链：审计事件与续传快照在同一事务落库
    # ------------------------------------------------------------------
    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            event = self._insert_audit_locked(
                action, entity_type, entity_id, actor, detail)
            self._append_snapshot_locked(
                entity_type, entity_id, action, event["entry_hash"], actor,
                created_at=event["created_at"])
        return event

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    # ------------------------------------------------------------------
    # 巡检任务
    # ------------------------------------------------------------------
    def get_task(self, task_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM tasks WHERE task_no=?", (task_no,)
            ).fetchone()
        return dict(row) if row else None

    def list_tasks(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM tasks ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # 应急任务派发
    # ------------------------------------------------------------------
    def create_dispatch(self, item_id: int, reason: str, actor: str,
                        request_no: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO emergency_dispatches(item_id, request_no, reason,
                   created_by, created_at) VALUES(?,?,?,?,?)""",
                (item_id, request_no, reason, actor, now),
            )
            dispatch_id = int(cur.lastrowid)
            event = self._insert_audit_locked(
                "dispatch", "应急任务", item_id, actor,
                {"dispatch_id": dispatch_id, "reason": reason, "request_no": request_no})
            self._append_snapshot_locked(
                "应急任务", item_id, "dispatch", event["entry_hash"], actor,
                request_no=request_no, created_at=event["created_at"])
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM emergency_dispatches WHERE id=?", (dispatch_id,)
            ).fetchone()
        return dict(row)

    def list_dispatches(self, item_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM emergency_dispatches"
        params: tuple = ()
        if item_id is not None:
            sql += " WHERE item_id=?"
            params = (item_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # 同步请求与检查点（可续传）
    # ------------------------------------------------------------------
    def begin_sync_request(self, request_no: str, node_id: str,
                           total_ops: int) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """INSERT OR IGNORE INTO sync_requests(request_no, node_id, status,
                   total_ops, created_at, updated_at) VALUES(?,?, 'processing', ?,?,?)""",
                (request_no, node_id, total_ops, now, now),
            )
            row = self.conn.execute(
                "SELECT * FROM sync_requests WHERE request_no=?", (request_no,)
            ).fetchone()
        return dict(row)

    def get_sync_request(self, request_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM sync_requests WHERE request_no=?", (request_no,)
            ).fetchone()
        return dict(row) if row else None

    def get_checkpoint(self, request_no: str, op_index: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM sync_checkpoints WHERE request_no=? AND op_index=?",
                (request_no, op_index),
            ).fetchone()
        return dict(row) if row else None

    def mark_sync_failed(self, request_no: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE sync_requests SET status='failed', updated_at=?
                   WHERE request_no=? AND status='processing'""",
                (utc_now(), request_no),
            )

    def complete_sync_request(self, request_no: str, result: dict) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM sync_requests WHERE request_no=?", (request_no,)
            ).fetchone()
            if row is None:
                raise NotFoundError("同步请求不存在")
            if row["status"] == "completed":
                return dict(row)
            self._maybe_fault("complete_before_commit", {"request_no": request_no})
            self.conn.execute(
                """UPDATE sync_requests SET status='completed', result=?, updated_at=?
                   WHERE request_no=?""",
                (json.dumps(result, ensure_ascii=False, default=str), now, request_no),
            )
            row = self.conn.execute(
                "SELECT * FROM sync_requests WHERE request_no=?", (request_no,)
            ).fetchone()
        return dict(row)

    def apply_sync_op(self, op: Dict[str, Any], request_no: str, op_index: int,
                      node_id: str) -> Dict[str, Any]:
        kind = op.get("op")
        with self._lock, self.conn:
            checkpoint = self.conn.execute(
                "SELECT * FROM sync_checkpoints WHERE request_no=? AND op_index=?",
                (request_no, op_index),
            ).fetchone()
            if checkpoint is not None and checkpoint["status"] == "done":
                # 续传：检查点已完成，沿用第一次结果
                return json.loads(checkpoint["result"])
            if kind == "task_upsert":
                result = self._apply_task_upsert_locked(op, request_no, op_index, node_id)
            elif kind == "defect_upsert":
                result = self._apply_defect_upsert_locked(op, request_no, op_index, node_id)
            elif kind == "defect_record":
                result = self._apply_defect_record_locked(op, request_no, op_index, node_id)
            else:
                from .domain import ValidationError
                raise ValidationError(f"未知同步操作: {kind}")
            self._maybe_fault("apply_before_checkpoint", {
                "request_no": request_no, "op_index": op_index, "op": kind,
            })
            now = utc_now()
            self.conn.execute(
                """INSERT INTO sync_checkpoints(request_no, op_index, status, result,
                   updated_at) VALUES(?,?,'done',?,?)
                   ON CONFLICT(request_no, op_index) DO UPDATE SET
                     status='done', result=excluded.result, updated_at=excluded.updated_at""",
                (request_no, op_index,
                 json.dumps(result, ensure_ascii=False, default=str), now),
            )
        return result

    # ------------------------------------------------------------------
    # 同步操作的具体合并逻辑（调用方持有锁与事务）
    # ------------------------------------------------------------------
    @staticmethod
    def _task_content(op: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "title": op["title"], "route": op.get("route", ""),
            "scheduled_at": op.get("scheduled_at", ""), "status": op["status"],
        }

    @staticmethod
    def _defect_content(op: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "title": op["title"], "description": op["description"],
            "severity": op["severity"], "quantity": op["quantity"],
            "threshold": op["threshold"],
        }

    def _retain_candidate_locked(self, entity_type: str, entity_key: str,
                                 request_no: str, op_index: int, node_id: str,
                                 op: Dict[str, Any], current: Dict[str, Any],
                                 base_version: int, task_no: Optional[str],
                                 item_id: Optional[int]) -> int:
        now = utc_now()
        cur = self.conn.execute(
            """INSERT INTO sync_candidates(entity_type, entity_key, task_no, item_id,
               request_no, op_index, node_id, actor, base_version, incoming, current,
               created_by, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (entity_type, entity_key, task_no, item_id, request_no, op_index,
             node_id, op.get("actor", ""), base_version,
             json.dumps(op, ensure_ascii=False, default=str),
             json.dumps(current, ensure_ascii=False, default=str),
             op.get("actor", ""), now),
        )
        return int(cur.lastrowid)

    def _apply_task_upsert_locked(self, op: Dict[str, Any], request_no: str,
                                  op_index: int, node_id: str) -> Dict[str, Any]:
        from .rules import ENTITY_TASK
        task_no = op["task_no"]
        base_version = int(op.get("base_version", 0))
        incoming_content = self._task_content(op)
        row = self.conn.execute("SELECT * FROM tasks WHERE task_no=?", (task_no,)).fetchone()
        actor = op.get("actor", node_id)
        if row is None:
            now = utc_now()
            cur = self.conn.execute(
                """INSERT INTO tasks(task_no, title, route, scheduled_at, status,
                   base_version, version, created_by, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (task_no, incoming_content["title"], incoming_content["route"],
                 incoming_content["scheduled_at"], incoming_content["status"],
                 base_version, 1, actor, now, now),
            )
            task_id = int(cur.lastrowid)
            event = self._insert_audit_locked(
                "task_create", ENTITY_TASK, task_id, actor,
                {"task_no": task_no, "request_no": request_no, "op_index": op_index,
                 "version": 1})
            self._append_snapshot_locked(
                ENTITY_TASK, task_no, "task_create", canonical_hash(incoming_content),
                actor, request_no=request_no, created_at=event["created_at"])
            return {"op": "task_upsert", "task_no": task_no, "status": "created",
                    "version": 1}
        current = dict(row)
        current_content = {k: current[k] for k in incoming_content}
        if current_content == incoming_content:
            return {"op": "task_upsert", "task_no": task_no, "status": "no_change",
                    "version": current["version"]}
        if base_version == current["version"]:
            now = utc_now()
            self.conn.execute(
                """UPDATE tasks SET title=?, route=?, scheduled_at=?, status=?,
                   base_version=?, version=version+1, updated_at=? WHERE id=?""",
                (incoming_content["title"], incoming_content["route"],
                 incoming_content["scheduled_at"], incoming_content["status"],
                 base_version, now, current["id"]),
            )
            new_version = current["version"] + 1
            event = self._insert_audit_locked(
                "task_update", ENTITY_TASK, current["id"], actor,
                {"task_no": task_no, "request_no": request_no, "op_index": op_index,
                 "from_version": current["version"], "to_version": new_version})
            self._append_snapshot_locked(
                ENTITY_TASK, task_no, "task_update", canonical_hash(incoming_content),
                actor, request_no=request_no, created_at=event["created_at"])
            return {"op": "task_upsert", "task_no": task_no, "status": "updated",
                    "version": new_version}
        # 两边都改过（或基准版本对不上）：保留候选，先到结果不动
        candidate_id = self._retain_candidate_locked(
            ENTITY_TASK, task_no, request_no, op_index, node_id, op, current_content,
            base_version, task_no, None)
        event = self._insert_audit_locked(
            "candidate_retained", "冲突候选", current["id"], actor,
            {"candidate_id": candidate_id, "entity_type": ENTITY_TASK,
             "entity_key": task_no, "request_no": request_no, "op_index": op_index,
             "base_version": base_version, "current_version": current["version"]})
        self._append_snapshot_locked(
            "冲突候选", candidate_id, "candidate_retained",
            canonical_hash({"incoming": incoming_content, "current": current_content}),
            actor, request_no=request_no, created_at=event["created_at"])
        return {"op": "task_upsert", "task_no": task_no, "status": "conflict",
                "candidate_id": candidate_id, "version": current["version"]}

    def _apply_defect_upsert_locked(self, op: Dict[str, Any], request_no: str,
                                    op_index: int, node_id: str) -> Dict[str, Any]:
        from .rules import ENTITY
        ref = op["external_ref"]
        base_version = int(op.get("base_version", 0))
        incoming_content = self._defect_content(op)
        row = self.conn.execute(
            "SELECT * FROM items WHERE external_ref=?", (ref,)
        ).fetchone()
        actor = op.get("actor", node_id)
        if row is None:
            now = utc_now()
            cur = self.conn.execute(
                """INSERT INTO items(title, description, severity, quantity, threshold,
                   status, version, external_ref, created_by, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (incoming_content["title"], incoming_content["description"],
                 incoming_content["severity"], incoming_content["quantity"],
                 incoming_content["threshold"], STATES[0], 1, ref, actor, now, now),
            )
            item_id = int(cur.lastrowid)
            event = self._insert_audit_locked(
                "defect_create", ENTITY, item_id, actor,
                {"external_ref": ref, "request_no": request_no, "op_index": op_index,
                 "version": 1})
            self._append_snapshot_locked(
                ENTITY, item_id, "defect_create", canonical_hash(incoming_content),
                actor, request_no=request_no, created_at=event["created_at"])
            return {"op": "defect_upsert", "external_ref": ref, "item_id": item_id,
                    "status": "created", "version": 1}
        current = dict(row)
        current_content = {k: current[k] for k in incoming_content}
        if current_content == incoming_content:
            return {"op": "defect_upsert", "external_ref": ref, "item_id": current["id"],
                    "status": "no_change", "version": current["version"]}
        if base_version == current["version"]:
            now = utc_now()
            self.conn.execute(
                """UPDATE items SET title=?, description=?, severity=?, quantity=?,
                   threshold=?, version=version+1, updated_at=? WHERE id=?""",
                (incoming_content["title"], incoming_content["description"],
                 incoming_content["severity"], incoming_content["quantity"],
                 incoming_content["threshold"], now, current["id"]),
            )
            new_version = current["version"] + 1
            event = self._insert_audit_locked(
                "defect_update", ENTITY, current["id"], actor,
                {"external_ref": ref, "request_no": request_no, "op_index": op_index,
                 "from_version": current["version"], "to_version": new_version})
            self._append_snapshot_locked(
                ENTITY, current["id"], "defect_update",
                canonical_hash(incoming_content), actor, request_no=request_no,
                created_at=event["created_at"])
            return {"op": "defect_upsert", "external_ref": ref, "item_id": current["id"],
                    "status": "updated", "version": new_version}
        # 同一缺陷两边都改过：先到结果保留生效，后到内容按请求号留冲突候选
        candidate_id = self._retain_candidate_locked(
            ENTITY, ref, request_no, op_index, node_id, op, current_content,
            base_version, None, current["id"])
        event = self._insert_audit_locked(
            "candidate_retained", "冲突候选", current["id"], actor,
            {"candidate_id": candidate_id, "entity_type": ENTITY,
             "entity_key": ref, "request_no": request_no, "op_index": op_index,
             "base_version": base_version, "current_version": current["version"]})
        self._append_snapshot_locked(
            "冲突候选", candidate_id, "candidate_retained",
            canonical_hash({"incoming": incoming_content, "current": current_content}),
            actor, request_no=request_no, created_at=event["created_at"])
        return {"op": "defect_upsert", "external_ref": ref, "item_id": current["id"],
                "status": "conflict", "candidate_id": candidate_id,
                "version": current["version"]}

    def _apply_defect_record_locked(self, op: Dict[str, Any], request_no: str,
                                    op_index: int, node_id: str) -> Dict[str, Any]:
        from .domain import ConflictError as _Conflict
        from .rules import ENTITY
        ref = op["external_ref"]
        row = self.conn.execute(
            "SELECT * FROM items WHERE external_ref=?", (ref,)
        ).fetchone()
        if row is None:
            from .domain import ValidationError
            raise ValidationError(f"缺陷不存在: {ref}")
        item = dict(row)
        actor = op.get("actor", node_id)
        existing = self.conn.execute(
            "SELECT * FROM records WHERE item_id=? AND external_ref=?",
            (item["id"], op["record_ref"]),
        ).fetchone()
        if existing is not None:
            # 重复导入沿用第一次结果：内容一致幂等返回，内容不同拒绝
            if existing["kind"] == op["kind"] and existing["detail"] == op["detail"]:
                return {"op": "defect_record", "external_ref": ref,
                        "record_id": existing["id"], "status": "duplicate"}
            raise _Conflict("记录唯一标识已被不同内容占用")
        now = utc_now()
        cur = self.conn.execute(
            """INSERT INTO records(item_id, kind, detail, status, external_ref,
               created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
            (item["id"], op["kind"], op["detail"], op.get("status", "open"),
             op["record_ref"], actor, now),
        )
        record_id = int(cur.lastrowid)
        event = self._insert_audit_locked(
            "record", ENTITY, item["id"], actor,
            {"record_id": record_id, "kind": op["kind"], "status": op.get("status", "open"),
             "request_no": request_no, "op_index": op_index})
        self._append_snapshot_locked(
            ENTITY, item["id"], "record", event["entry_hash"], actor,
            request_no=request_no, created_at=event["created_at"])
        return {"op": "defect_record", "external_ref": ref, "record_id": record_id,
                "item_id": item["id"], "status": "created"}

    # ------------------------------------------------------------------
    # 冲突候选与复核
    # ------------------------------------------------------------------
    def list_candidates(self, status: Optional[str] = None,
                        item_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM sync_candidates"
        clauses = []
        params: List[Any] = []
        if status:
            clauses.append("status=?")
            params.append(status)
        if item_id is not None:
            clauses.append("item_id=?")
            params.append(item_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def get_candidate(self, candidate_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM sync_candidates WHERE id=?", (candidate_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("冲突候选不存在")
        return dict(row)

    def pending_candidate_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM sync_candidates WHERE item_id=? AND status='pending'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def resolve_candidate(self, candidate_id: int, decision: str, actor: str,
                          note: str = "") -> Dict[str, Any]:
        from .rules import ENTITY, ENTITY_TASK
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM sync_candidates WHERE id=?", (candidate_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("冲突候选不存在")
            candidate = dict(row)
            if candidate["status"] != "pending":
                raise ConflictError("冲突候选已复核")
            now = utc_now()
            applied: Dict[str, Any] = {}
            if decision == "accept":
                incoming = json.loads(candidate["incoming"])
                if candidate["entity_type"] == ENTITY:
                    content = self._defect_content(incoming)
                    item_row = self.conn.execute(
                        "SELECT * FROM items WHERE id=?", (candidate["item_id"],)
                    ).fetchone()
                    if item_row is None:
                        raise NotFoundError("缺陷已不存在")
                    item = dict(item_row)
                    self.conn.execute(
                        """UPDATE items SET title=?, description=?, severity=?, quantity=?,
                           threshold=?, version=version+1, updated_at=? WHERE id=?""",
                        (content["title"], content["description"], content["severity"],
                         content["quantity"], content["threshold"], now, item["id"]),
                    )
                    new_version = item["version"] + 1
                    event = self._insert_audit_locked(
                        "candidate_accepted", ENTITY, item["id"], actor,
                        {"candidate_id": candidate_id, "request_no": candidate["request_no"],
                         "from_version": item["version"], "to_version": new_version,
                         "note": note})
                    self._append_snapshot_locked(
                        ENTITY, item["id"], "candidate_accepted",
                        canonical_hash(content), actor,
                        request_no=candidate["request_no"], created_at=event["created_at"])
                    applied = {"item_id": item["id"], "version": new_version}
                else:
                    content = self._task_content(incoming)
                    task_row = self.conn.execute(
                        "SELECT * FROM tasks WHERE task_no=?", (candidate["task_no"],)
                    ).fetchone()
                    if task_row is None:
                        raise NotFoundError("巡检任务已不存在")
                    task = dict(task_row)
                    self.conn.execute(
                        """UPDATE tasks SET title=?, route=?, scheduled_at=?, status=?,
                           version=version+1, updated_at=? WHERE id=?""",
                        (content["title"], content["route"], content["scheduled_at"],
                         content["status"], now, task["id"]),
                    )
                    new_version = task["version"] + 1
                    event = self._insert_audit_locked(
                        "candidate_accepted", ENTITY_TASK, task["id"], actor,
                        {"candidate_id": candidate_id, "task_no": task["task_no"],
                         "request_no": candidate["request_no"],
                         "from_version": task["version"], "to_version": new_version,
                         "note": note})
                    self._append_snapshot_locked(
                        ENTITY_TASK, task["task_no"], "candidate_accepted",
                        canonical_hash(content), actor,
                        request_no=candidate["request_no"], created_at=event["created_at"])
                    applied = {"task_no": task["task_no"], "version": new_version}
            else:
                target_type = candidate["entity_type"]
                target_id = candidate["item_id"] if target_type == ENTITY else candidate["task_no"]
                event = self._insert_audit_locked(
                    "candidate_rejected", target_type, target_id or 0, actor,
                    {"candidate_id": candidate_id, "request_no": candidate["request_no"],
                     "note": note})
                self._append_snapshot_locked(
                    "冲突候选", candidate_id, "candidate_rejected",
                    event["entry_hash"], actor,
                    request_no=candidate["request_no"], created_at=event["created_at"])
            self.conn.execute(
                "UPDATE sync_candidates SET status=?, resolution=?, resolved_at=? WHERE id=?",
                ("accepted" if decision == "accept" else "rejected", note, now,
                 candidate_id),
            )
        return {"candidate": self.get_candidate(candidate_id), "applied": applied}

    # ------------------------------------------------------------------
    # 共用续传快照
    # ------------------------------------------------------------------
    def snapshot_head(self) -> int:
        with self._lock:
            row = self.conn.execute("SELECT MAX(seq) AS s FROM snapshot_log").fetchone()
        return int(row["s"] or 0)

    def get_snapshot(self, after: int = 0, limit: int = 1000) -> Dict[str, Any]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM snapshot_log WHERE seq>? ORDER BY seq LIMIT ?",
                (after, limit),
            ).fetchall()
            head = self.conn.execute("SELECT MAX(seq) AS s FROM snapshot_log").fetchone()["s"]
        return {"head": int(head or 0), "entries": [dict(row) for row in rows]}

    def backfill_snapshots(self, actor: str = "system") -> Dict[str, int]:
        """已有数据缺少快照检查点时幂等回填；审计事件逐条对应一条快照。"""
        with self._lock, self.conn:
            count_row = self.conn.execute("SELECT COUNT(*) AS n FROM snapshot_log").fetchone()
            if int(count_row["n"]) > 0:
                return {"backfilled": 0}
            events = self.conn.execute(
                "SELECT * FROM audit_events ORDER BY id").fetchall()
            for event_row in events:
                self.conn.execute(
                    """INSERT INTO snapshot_log(entity_type, entity_id, op, request_no,
                       payload_hash, actor, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (event_row["entity_type"], str(event_row["entity_id"]),
                     event_row["action"],
                     json.loads(event_row["detail"]).get("request_no"),
                     event_row["entry_hash"], event_row["actor"], event_row["created_at"]),
                )
            backfilled = len(events)
            if backfilled:
                marker = self._insert_audit_locked(
                    "snapshot_backfilled", ENTITY_SNAPSHOT, 0, actor,
                    {"events": backfilled})
                self._append_snapshot_locked(
                    ENTITY_SNAPSHOT, 0, "snapshot_backfilled", marker["entry_hash"],
                    actor, created_at=marker["created_at"])
                backfilled += 1
        return {"backfilled": backfilled}

    def close(self) -> None:
        with self._lock:
            self.conn.close()
