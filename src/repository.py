from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ID_PREFIX, STATES


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()
        self._migrate()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
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
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    checkpoint TEXT NOT NULL UNIQUE,
                    base_checkpoint TEXT NOT NULL DEFAULT 'GENESIS',
                    request_no TEXT,
                    kind TEXT NOT NULL DEFAULT 'import',
                    status TEXT NOT NULL DEFAULT 'building',
                    step TEXT NOT NULL DEFAULT 'received',
                    payload TEXT NOT NULL DEFAULT '{{}}',
                    snapshot_hash TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS request_log (
                    request_no TEXT PRIMARY KEY,
                    checkpoint TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'building',
                    result TEXT NOT NULL DEFAULT '{{}}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS defect_conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    external_ref TEXT NOT NULL,
                    item_id INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending_review',
                    candidates TEXT NOT NULL DEFAULT '[]',
                    winning_request_no TEXT,
                    resolution TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_defect_conflicts_ref
                    ON defect_conflicts(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS snapshot_progress (
                    checkpoint TEXT NOT NULL,
                    external_ref TEXT NOT NULL,
                    step TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (checkpoint, external_ref, step)
                );
            """)

    def _migrate(self) -> None:
        """为已有库补齐 last_checkpoint 列（可续传快照上线前的数据）。"""
        with self._lock, self.conn:
            cols = [row[1] for row in self.conn.execute("PRAGMA table_info(items)").fetchall()]
            if "last_checkpoint" not in cols:
                self.conn.execute("ALTER TABLE items ADD COLUMN last_checkpoint TEXT")

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

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

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
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
    # 可续传快照
    # ------------------------------------------------------------------
    def _bundle_locked(self) -> Dict[str, Any]:
        items = [dict(r) for r in self.conn.execute("SELECT * FROM items ORDER BY id").fetchall()]
        records = [dict(r) for r in self.conn.execute("SELECT * FROM records ORDER BY id").fetchall()]
        audit = [dict(r) for r in self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()]
        for event in audit:
            event["detail"] = json.loads(event["detail"])
        head = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1").fetchone()
        audit_head_hash = head["entry_hash"] if head else ""
        from .snapshot import build_bundle
        return build_bundle(items, records, audit, audit_head_hash)

    def bundle(self) -> Dict[str, Any]:
        with self._lock:
            return self._bundle_locked()

    def create_pending_snapshot(self, request_no: str, base_checkpoint: str,
                                actor: str, kind: str = "import") -> str:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO snapshots(checkpoint, base_checkpoint, request_no, kind,
                   status, step, payload, snapshot_hash, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                ("", base_checkpoint, request_no, kind, "building", "received",
                 "{}", "", now, now),
            )
            snap_id = int(cur.lastrowid)
            from .snapshot import make_checkpoint
            cp = make_checkpoint(snap_id)
            self.conn.execute("UPDATE snapshots SET checkpoint=? WHERE id=?", (cp, snap_id))
            self.conn.execute(
                """INSERT INTO request_log(request_no, checkpoint, status, result,
                   created_at, updated_at) VALUES(?,?,?,?,?,?)""",
                (request_no, cp, "building", "{}", now, now),
            )
        return cp

    def get_snapshot(self, checkpoint: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM snapshots WHERE checkpoint=?", (checkpoint,)).fetchone()
        return self._snapshot_row(row) if row else None

    def get_snapshot_by_request(self, request_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM snapshots WHERE request_no=? ORDER BY id DESC LIMIT 1",
                (request_no,)).fetchone()
        return self._snapshot_row(row) if row else None

    @staticmethod
    def _snapshot_row(row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        data["payload"] = json.loads(data["payload"])
        return data

    def advance_snapshot(self, checkpoint: str, step: str,
                         payload: Optional[Dict[str, Any]] = None) -> None:
        now = utc_now()
        with self._lock, self.conn:
            if payload is not None:
                self.conn.execute(
                    "UPDATE snapshots SET step=?, status='building', payload=?, updated_at=? WHERE checkpoint=?",
                    (step, json.dumps(payload, ensure_ascii=False, sort_keys=True), now, checkpoint))
            else:
                self.conn.execute(
                    "UPDATE snapshots SET step=?, status='building', updated_at=? WHERE checkpoint=?",
                    (step, now, checkpoint))

    def finalize_snapshot(self, checkpoint: str, payload: Dict[str, Any],
                          snapshot_hash: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE snapshots SET status='complete', step='finalized', payload=?,
                   snapshot_hash=?, updated_at=? WHERE checkpoint=?""",
                (json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 snapshot_hash, now, checkpoint))

    def fail_snapshot(self, checkpoint: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE snapshots SET status='failed', updated_at=? WHERE checkpoint=?",
                (now, checkpoint))

    def list_snapshots(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM snapshots ORDER BY id").fetchall()
        return [self._snapshot_row(r) for r in rows]

    def get_latest_checkpoint(self) -> str:
        with self._lock:
            row = self.conn.execute(
                "SELECT checkpoint FROM snapshots WHERE status='complete' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return row["checkpoint"] if row else "GENESIS"

    def snapshot_exists(self, checkpoint: str) -> bool:
        with self._lock:
            row = self.conn.execute(
                "SELECT 1 FROM snapshots WHERE checkpoint=? AND status='complete'",
                (checkpoint,)).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # 请求号幂等
    # ------------------------------------------------------------------
    def get_request(self, request_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM request_log WHERE request_no=?", (request_no,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["result"] = json.loads(data["result"])
        return data

    def finish_request(self, request_no: str, status: str, result: Dict[str, Any]) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE request_log SET status=?, result=?, updated_at=? WHERE request_no=?",
                (status, json.dumps(result, ensure_ascii=False, sort_keys=True), now, request_no))

    # ------------------------------------------------------------------
    # 缺陷冲突候选
    # ------------------------------------------------------------------
    def insert_conflict(self, external_ref: str, item_id: Optional[int],
                        candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO defect_conflicts(external_ref, item_id, status, candidates,
                   winning_request_no, resolution, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (external_ref, item_id, "pending_review",
                 json.dumps(candidates, ensure_ascii=False, sort_keys=True),
                 None, None, now, now))
            conflict_id = int(cur.lastrowid)
        return self.get_conflict(conflict_id)

    def get_conflict(self, conflict_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM defect_conflicts WHERE id=?", (conflict_id,)).fetchone()
        return self._conflict_row(row) if row else None

    def get_conflict_by_ref(self, external_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM defect_conflicts WHERE external_ref=?", (external_ref,)).fetchone()
        return self._conflict_row(row) if row else None

    def list_conflicts(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM defect_conflicts"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._conflict_row(r) for r in rows]

    def add_conflict_candidate(self, conflict_id: int, candidate: Dict[str, Any]) -> None:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT candidates FROM defect_conflicts WHERE id=?", (conflict_id,)).fetchone()
            candidates = json.loads(row["candidates"])
            if not any(c.get("request_no") == candidate["request_no"] for c in candidates):
                candidates.append(candidate)
                self.conn.execute(
                    "UPDATE defect_conflicts SET candidates=?, updated_at=? WHERE id=?",
                    (json.dumps(candidates, ensure_ascii=False, sort_keys=True),
                     now, conflict_id))

    def resolve_conflict(self, conflict_id: int, winning_request_no: str,
                         resolution: Dict[str, Any]) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE defect_conflicts SET status='resolved', winning_request_no=?,
                   resolution=?, updated_at=? WHERE id=?""",
                (winning_request_no,
                 json.dumps(resolution, ensure_ascii=False, sort_keys=True),
                 now, conflict_id))

    @staticmethod
    def _conflict_row(row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        data["candidates"] = json.loads(data["candidates"])
        if data.get("resolution"):
            data["resolution"] = json.loads(data["resolution"])
        return data

    # ------------------------------------------------------------------
    # 写入进度（按缺陷续传）
    # ------------------------------------------------------------------
    def mark_progress(self, checkpoint: str, external_ref: str, step: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """INSERT OR IGNORE INTO snapshot_progress(checkpoint, external_ref, step, created_at)
                   VALUES(?,?,?,?)""",
                (checkpoint, external_ref, step, now))

    def has_progress(self, checkpoint: str, external_ref: str, step: str) -> bool:
        with self._lock:
            row = self.conn.execute(
                "SELECT 1 FROM snapshot_progress WHERE checkpoint=? AND external_ref=? AND step=?",
                (checkpoint, external_ref, step)).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # 导入用的任务/缺陷写入
    # ------------------------------------------------------------------
    def get_item_by_external_ref(self, external_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE external_ref=?", (external_ref,)).fetchone()
        return self._item(row) if row else None

    def create_import_item(self, change: Dict[str, Any], checkpoint: str,
                           actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO items(title, description, severity, quantity, threshold,
                   status, version, external_ref, created_by, created_at, updated_at,
                   last_checkpoint) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (change["title"], change["description"], change["severity"],
                 change["quantity"], change["threshold"], STATES[0], 1,
                 change["external_ref"], actor, now, now, checkpoint),
            )
            item_id = int(cur.lastrowid)
        return self.get_item(item_id)

    def update_import_item(self, item_id: int, change: Dict[str, Any],
                           checkpoint: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE items SET title=?, description=?, severity=?, quantity=?,
                   threshold=?, version=version+1, updated_at=?, last_checkpoint=?
                   WHERE id=?""",
                (change["title"], change["description"], change["severity"],
                 change["quantity"], change["threshold"], now, checkpoint, item_id))
        return self.get_item(item_id)

    def add_import_record(self, item_id: int, record: Dict[str, Any],
                          checkpoint: str, actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO records(item_id, kind, detail, status, external_ref,
                   created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                (item_id, record["kind"], record["detail"],
                 record.get("status", "open"), record.get("external_ref"), actor, now),
            )
            record_id = int(cur.lastrowid)
            # 记录变更加入缺陷后，同样推进该缺陷的检查点
            self.conn.execute(
                "UPDATE items SET last_checkpoint=?, updated_at=? WHERE id=?",
                (checkpoint, now, item_id))
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    # ------------------------------------------------------------------
    # 回填：已有数据缺检查点时补快照
    # ------------------------------------------------------------------
    def backfill_snapshot(self, actor: str = "system") -> Optional[Dict[str, Any]]:
        """为没有检查点的存量数据补一份快照；幂等，已覆盖则返回 None。"""
        with self._lock, self.conn:
            missing = self.conn.execute(
                "SELECT COUNT(*) AS n FROM items WHERE last_checkpoint IS NULL"
            ).fetchone()["n"]
            has_snapshot = self.conn.execute(
                "SELECT 1 FROM snapshots WHERE status='complete' LIMIT 1").fetchone()
            if missing == 0 and has_snapshot is not None:
                return None
            now = utc_now()
            cur = self.conn.execute(
                """INSERT INTO snapshots(checkpoint, base_checkpoint, request_no, kind,
                   status, step, payload, snapshot_hash, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                ("", "GENESIS", None, "backfill", "building", "received",
                 "{}", "", now, now),
            )
            snap_id = int(cur.lastrowid)
            from .snapshot import make_checkpoint, snapshot_hash
            cp = make_checkpoint(snap_id)
            self.conn.execute("UPDATE snapshots SET checkpoint=? WHERE id=?", (cp, snap_id))
            # 给缺检查点的存量任务/缺陷回填检查点
            self.conn.execute(
                "UPDATE items SET last_checkpoint=? WHERE last_checkpoint IS NULL", (cp,))
            bundle = self._bundle_locked()
            h = snapshot_hash(cp, "GENESIS", bundle)
            self.conn.execute(
                """UPDATE snapshots SET status='complete', step='finalized', payload=?,
                   snapshot_hash=?, updated_at=? WHERE id=?""",
                (json.dumps(bundle, ensure_ascii=False, sort_keys=True), h, now, snap_id))
            row = self.conn.execute("SELECT * FROM snapshots WHERE id=?", (snap_id,)).fetchone()
        return self._snapshot_row(row)

    def close(self) -> None:
        with self._lock:
            self.conn.close()
