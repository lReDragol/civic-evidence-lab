"""At-least-once transfer; the inbox and receiver mutation commit together."""
import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone


def _now():
    return datetime.now(timezone.utc).isoformat()


def enqueue_transfer(conn, *, destination, event_type, subject_key, input_revision, payload):
    if not conn.in_transaction:
        raise RuntimeError("Outbox insertion must share the producer transaction")
    identity = json.dumps([destination, event_type, subject_key, input_revision], separators=(",", ":"))
    message_id = hashlib.sha256(identity.encode()).hexdigest()
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    conn.execute("""INSERT OR IGNORE INTO transfer_outbox
        (message_id,destination,event_type,subject_key,input_revision,payload_json,created_at)
        VALUES(?,?,?,?,?,?,?)""", (message_id,destination,event_type,subject_key,input_revision,serialized,_now()))
    stored = conn.execute("SELECT payload_json FROM transfer_outbox WHERE message_id=?", (message_id,)).fetchone()[0]
    if stored != serialized:
        raise ValueError("An outbox identity cannot describe different immutable input")
    return message_id


def receive_transfer(conn, envelope, handler):
    if conn.in_transaction:
        raise RuntimeError("Receiver requires a clean connection")
    digest = hashlib.sha256(envelope["payload_json"].encode()).hexdigest()
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute("SELECT payload_hash FROM transfer_inbox WHERE message_id=?", (envelope["message_id"],)).fetchone()
        if existing:
            if existing[0] != digest:
                raise ValueError("Replayed message payload differs")
            conn.commit()
            return False
        conn.execute("INSERT INTO transfer_inbox VALUES(?,?,?)", (envelope["message_id"],_now(),digest))
        # Prevent a handler from committing midway through inbox acceptance.
        conn.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in
                            (sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT, sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH)
                            else sqlite3.SQLITE_OK)
        try:
            handler(conn, envelope["event_type"], json.loads(envelope["payload_json"]))
        finally:
            conn.set_authorizer(None)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def relay_pending(source, destination, *, destination_name, handler, limit=100):
    """A crash after receiver commit but before ACK is safe to replay."""
    if source.in_transaction:
        raise RuntimeError("Producer writes must commit before relay")
    cursor = source.execute("SELECT * FROM transfer_outbox WHERE delivered_at IS NULL AND destination=? ORDER BY created_at,message_id LIMIT ?", (destination_name,max(1,min(1000,limit))))
    columns = [d[0] for d in cursor.description]
    rows = [dict(zip(columns,r)) for r in cursor.fetchall()]
    for envelope in rows:
        receive_transfer(destination,envelope,handler)
        with source:
            source.execute("UPDATE transfer_outbox SET delivered_at=? WHERE message_id=?", (_now(),envelope["message_id"]))
    return {"acknowledged": len(rows)}
