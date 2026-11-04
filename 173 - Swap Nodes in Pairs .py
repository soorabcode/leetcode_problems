'''
Login System

1. Requirement Analysis
1A. Functional Requirements
- Login from multiple sources.
- Real-time streaming.
- Validate logins.

1B. Non-Functional Requirements
- Scale: 10M+
- CAP Theorem: Availability >>>> Consistency
- Latency: Low (near-real-time)

2. Identifying Users
- Log Event
- Ingestion Job
- Agent

3. API Design
3A. Login : 
POST /v1/agents/activity = {login | logout | register | user ID}
3B. Search : 
POST /v1/search = {query} → Pagination
GET /logs/{logsID}
3C. Ingestion
POST /v1/logs = {ingest + batch + status}

4. High level design 
user -> api gateway -> sever ( client onboarding , file ingestion and search ) -> database ( clientDB and logDB )

5. low level design 
uploaded image
convert this intoi python code that gives interface and gets run in colab itself having inbuilt html css such that it provides interface to our user based upon the  designed system havuing proper 
functionality of login platform having no login policy 

Login System

1. Requirement Analysis
1A. Functional Requirements
- Login from multiple sources.
- Real-time streaming.
- Validate logins.

1B. Non-Functional Requirements
- Scale: 10M+
- CAP Theorem: Availability >>>> Consistency
- Latency: Low (near-real-time)

2. Identifying Users
- Log Event
- Ingestion Job
- Agent

3. API Design
3A. Login : 
POST /v1/agents/activity = {login | logout | register | user ID}
3B. Search : 
POST /v1/search = {query} → Pagination
GET /logs/{logsID}
3C. Ingestion
POST /v1/logs = {ingest + batch + status}

4. High level design 
user -> api gateway -> sever ( client onboarding , file ingestion and search ) -> database ( clientDB and logDB )

5. low level design 
uploaded image

'''
import csv
import io
import ipaddress
import json
import queue
import random
import re
import sqlite3
import threading
import time
import uuid
import zlib
from collections import defaultdict, deque
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from werkzeug.serving import make_server

PORT = 5057
NUM_PARTITIONS = 3
PROC_DELAY = 0.003
VALID_ACTIONS = {"login", "logout", "register"}
SOURCES = ["web", "android", "ios", "desktop", "sso", "api", "kiosk"]


_prev = globals().get("_LOGIN_RUNTIME")
if _prev:
    _prev["stop"].set()
    try:
        _prev["server"].shutdown()
        _prev["server"].server_close()
    except Exception:
        pass
    time.sleep(0.8)

STOP = threading.Event()


def now():
    return time.time()


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def parse_ts(value, default):
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return default


class ClientDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.lock = threading.Lock()
        self.conn.execute(
            "CREATE TABLE clients (id TEXT PRIMARY KEY, name TEXT, token TEXT UNIQUE, created_at REAL)"
        )
        self.conn.commit()

    def create(self, name):
        client = {
            "id": "CL-" + uuid.uuid4().hex[:6].upper(),
            "name": name,
            "token": "tok_" + uuid.uuid4().hex,
            "created_at": now(),
        }
        with self.lock:
            self.conn.execute(
                "INSERT INTO clients VALUES (?, ?, ?, ?)",
                (client["id"], client["name"], client["token"], client["created_at"]),
            )
            self.conn.commit()
        return client

    def by_token(self, token):
        if not token:
            return None
        with self.lock:
            row = self.conn.execute(
                "SELECT id, name, token, created_at FROM clients WHERE token = ?", (token,)
            ).fetchone()
        return dict(zip(("id", "name", "token", "created_at"), row)) if row else None

    def all(self):
        with self.lock:
            rows = self.conn.execute(
                "SELECT id, name, token, created_at FROM clients ORDER BY created_at"
            ).fetchall()
        return [dict(zip(("id", "name", "token", "created_at"), r)) for r in rows]


LOG_COLUMNS = (
    "seq", "id", "client_id", "client_name", "user_id", "action", "source", "ip",
    "status", "reason", "event_ts", "ingest_ts", "processed_ts", "latency_ms", "batch_id",
)


class LogDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.lock = threading.Lock()
        self.conn.execute(
            """CREATE TABLE logs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, client_id TEXT,
                client_name TEXT, user_id TEXT, action TEXT, source TEXT, ip TEXT,
                status TEXT, reason TEXT, event_ts REAL, ingest_ts REAL,
                processed_ts REAL, latency_ms REAL, batch_id TEXT)"""
        )
        self.conn.execute("CREATE INDEX idx_client ON logs (client_id, seq)")
        self.conn.commit()

    @staticmethod
    def _row(row):
        d = dict(zip(LOG_COLUMNS, row))
        d["event_time"] = iso(d["event_ts"])
        d["ingest_time"] = iso(d["ingest_ts"])
        d["processed_time"] = iso(d["processed_ts"])
        return d

    def insert(self, rec):
        with self.lock:
            cur = self.conn.execute(
                """INSERT INTO logs (id, client_id, client_name, user_id, action, source, ip,
                   status, reason, event_ts, ingest_ts, processed_ts, latency_ms, batch_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rec["id"], rec["client_id"], rec["client_name"], rec["user_id"], rec["action"],
                    rec["source"], rec["ip"], rec["status"], rec["reason"], rec["event_ts"],
                    rec["ingest_ts"], rec["processed_ts"], rec["latency_ms"], rec["batch_id"],
                ),
            )
            self.conn.commit()
            return cur.lastrowid

    def get(self, log_id):
        with self.lock:
            row = self.conn.execute(
                "SELECT %s FROM logs WHERE id = ?" % ",".join(LOG_COLUMNS), (log_id,)
            ).fetchone()
        return self._row(row) if row else None

    def get_many(self, ids):
        if not ids:
            return []
        marks = ",".join("?" * len(ids))
        with self.lock:
            rows = self.conn.execute(
                "SELECT %s FROM logs WHERE id IN (%s)" % (",".join(LOG_COLUMNS), marks), ids
            ).fetchall()
        by_id = {r[1]: self._row(r) for r in rows}
        return [by_id[i] for i in ids if i in by_id]

    def since(self, client_id, seq, limit=100):
        with self.lock:
            rows = self.conn.execute(
                "SELECT %s FROM logs WHERE client_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?"
                % ",".join(LOG_COLUMNS),
                (client_id, seq, limit),
            ).fetchall()
        return [self._row(r) for r in rows]

    def latest_seq(self, client_id):
        with self.lock:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM logs WHERE client_id = ?", (client_id,)
            ).fetchone()
        return row[0]

    def count(self, client_id=None):
        with self.lock:
            if client_id:
                return self.conn.execute(
                    "SELECT COUNT(*) FROM logs WHERE client_id = ?", (client_id,)
                ).fetchone()[0]
            return self.conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]


class Elastic:
    FIELDS = ("user_id", "action", "source", "status", "ip", "reason", "client_name")

    def __init__(self):
        self.index = defaultdict(set)
        self.docs = {}
        self.lock = threading.Lock()

    @staticmethod
    def tokens(value):
        text = str(value or "").lower()
        parts = {text} | set(re.findall(r"[a-z0-9]+", text))
        return {p for p in parts if p}

    def add(self, doc):
        with self.lock:
            self.docs[doc["id"]] = {"seq": doc["seq"], "client_id": doc["client_id"]}
            for field in self.FIELDS:
                for tok in self.tokens(doc.get(field)):
                    self.index[tok].add(doc["id"])
                    self.index[field + ":" + tok].add(doc["id"])

    def search(self, client_id, query, page, size):
        terms = [t for t in (query or "").lower().split() if t]
        with self.lock:
            if terms:
                keys = list(self.index.keys())
                result = None
                for term in terms:
                    matched = set()
                    for key in keys:
                        if key.startswith(term):
                            matched |= self.index[key]
                    result = matched if result is None else (result & matched)
                    if not result:
                        break
                ids = list(result or [])
            else:
                ids = list(self.docs.keys())
            ids = [i for i in ids if self.docs[i]["client_id"] == client_id]
            ids.sort(key=lambda i: self.docs[i]["seq"], reverse=True)
        total = len(ids)
        pages = max(1, -(-total // size))
        page = max(1, min(page, pages))
        start = (page - 1) * size
        return {"total": total, "pages": pages, "page": page, "ids": ids[start:start + size]}


class Kafka:
    def __init__(self, partitions):
        self.parts = [queue.Queue() for _ in range(partitions)]
        self.produced = 0
        self.consumed = 0
        self.lock = threading.Lock()

    def produce(self, key, msg):
        pid = zlib.crc32(key.encode()) % len(self.parts)
        self.parts[pid].put(msg)
        with self.lock:
            self.produced += 1

    def mark_consumed(self):
        with self.lock:
            self.consumed += 1

    def depths(self):
        return [q.qsize() for q in self.parts]


client_db = ClientDB()
log_db = LogDB()
elastic = Elastic()
kafka = Kafka(NUM_PARTITIONS)

inflight = set()
batches = {}
batch_order = deque(maxlen=50)
stats_lock = threading.Lock()
counters = {"valid": 0, "suspicious": 0, "invalid": 0, "processed": 0, "ingested": 0}
processed_times = deque(maxlen=5000)
latencies = deque(maxlen=300)
window_events = deque()
START_TIME = now()


registered_users = set()
active_sessions = {}
login_times = defaultdict(lambda: deque(maxlen=20))


def flink_validate(evt):
    user = str(evt.get("user_id") or "").strip()
    action = str(evt.get("action") or "").strip().lower()
    source = str(evt.get("source") or "").strip().lower()
    ip = str(evt.get("ip") or "").strip()
    key = "%s:%s" % (evt["client_id"], user.lower())

    if not user:
        return "INVALID", "missing user_id"
    if action not in VALID_ACTIONS:
        return "INVALID", "unsupported action"
    if not source:
        return "INVALID", "missing source"
    if ip:
        try:
            ipaddress.ip_address(ip)
        except ValueError:
            return "INVALID", "malformed ip address"

    if action == "register":
        if key in registered_users:
            return "INVALID", "user already registered"
        registered_users.add(key)
        return "VALID", ""

    if action == "login":
        stamps = login_times[key]
        t = now()
        stamps.append(t)
        recent = [s for s in stamps if t - s <= 60]
        status, reason = "VALID", ""
        if len(recent) > 5:
            status, reason = "SUSPICIOUS", "rapid repeated logins (more than 5 per minute)"
        elif key in active_sessions and active_sessions[key] != source:
            status, reason = "SUSPICIOUS", "concurrent session from a different source"
        active_sessions[key] = source
        return status, reason

    if key not in active_sessions:
        return "INVALID", "logout without an active session"
    active_sessions.pop(key, None)
    return "VALID", ""


def flink_process(evt):
    status, reason = flink_validate(evt)
    time.sleep(PROC_DELAY)
    done = now()
    record = {
        "id": evt["id"],
        "client_id": evt["client_id"],
        "client_name": evt["client_name"],
        "user_id": str(evt.get("user_id") or ""),
        "action": str(evt.get("action") or "").lower(),
        "source": str(evt.get("source") or "").lower(),
        "ip": str(evt.get("ip") or ""),
        "status": status,
        "reason": reason,
        "event_ts": evt["event_ts"],
        "ingest_ts": evt["ingest_ts"],
        "processed_ts": done,
        "latency_ms": round((done - evt["ingest_ts"]) * 1000, 1),
        "batch_id": evt.get("batch_id") or "",
    }
    record["seq"] = log_db.insert(record)
    elastic.add(record)
    inflight.discard(evt["id"])

    with stats_lock:
        counters["processed"] += 1
        counters[status.lower()] += 1
        processed_times.append(done)
        latencies.append(record["latency_ms"])
        window_events.append((done, record["action"] or "unknown", record["source"] or "unknown", status))
        batch = batches.get(evt.get("batch_id"))
        if batch:
            batch["processed"] += 1
            if batch["processed"] >= batch["accepted"]:
                batch["status"] = "completed"


def flink_worker(pid):
    q = kafka.parts[pid]
    while not STOP.is_set():
        try:
            evt = q.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            flink_process(evt)
        except Exception as exc:
            print("flink error:", exc)
        finally:
            kafka.mark_consumed()
            q.task_done()


for _pid in range(NUM_PARTITIONS):
    threading.Thread(target=flink_worker, args=(_pid,), daemon=True).start()


app = Flask(__name__)


def current_client():
    token = request.headers.get("X-Client-Token", "")
    if not token:
        body = request.get_json(silent=True)
        if isinstance(body, dict):
            token = body.get("token", "")
    if not token:
        token = request.form.get("token", "") or request.args.get("token", "")
    return client_db.by_token(token)


def unauthorized():
    return jsonify({"error": "invalid or missing client token"}), 401


def enqueue(client, raw_events, batch_id=None):
    ingest_ts = now()
    ids = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            raw = {}
        event_id = "LOG-" + uuid.uuid4().hex[:12].upper()
        evt = {
            "id": event_id,
            "client_id": client["id"],
            "client_name": client["name"],
            "user_id": raw.get("user_id", raw.get("userId", raw.get("user"))),
            "action": raw.get("action"),
            "source": raw.get("source"),
            "ip": raw.get("ip"),
            "event_ts": parse_ts(raw.get("ts"), ingest_ts),
            "ingest_ts": ingest_ts,
            "batch_id": batch_id,
        }
        inflight.add(event_id)
        kafka.produce("%s:%s" % (client["id"], str(evt["user_id"] or "").lower()), evt)
        ids.append(event_id)
    with stats_lock:
        counters["ingested"] += len(ids)
    return ids


def new_batch(client, total, kind):
    batch_id = "BATCH-" + uuid.uuid4().hex[:8].upper()
    with stats_lock:
        batches[batch_id] = {
            "id": batch_id, "client": client["name"], "kind": kind, "accepted": total,
            "processed": 0, "status": "processing" if total else "completed", "created_at": now(),
        }
        batch_order.append(batch_id)
    return batch_id


@app.route("/v1/clients", methods=["POST"])
def create_client():
    body = request.get_json(silent=True) or {}
    name = str(body.get("name", "")).strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    return jsonify(client_db.create(name)), 201


@app.route("/v1/clients", methods=["GET"])
def list_clients():
    out = []
    for c in client_db.all():
        c["events"] = log_db.count(c["id"])
        out.append(c)
    return jsonify({"clients": out})


@app.route("/v1/agents/activity", methods=["POST"])
def agent_activity():
    client = current_client()
    if not client:
        return unauthorized()
    body = request.get_json(silent=True) or {}
    ids = enqueue(client, [body])
    return jsonify({"status": "accepted", "log_id": ids[0], "note": "queued to Kafka; processed asynchronously"}), 202


@app.route("/v1/logs", methods=["POST"])
def ingest_logs():
    client = current_client()
    if not client:
        return unauthorized()
    body = request.get_json(silent=True) or {}
    events = body.get("batch")
    if not isinstance(events, list) or not events:
        return jsonify({"error": "batch must be a non-empty list"}), 400
    if len(events) > 50000:
        return jsonify({"error": "batch too large (max 50000)"}), 413
    batch_id = new_batch(client, len(events), "api batch")
    enqueue(client, events, batch_id)
    return jsonify({"status": "accepted", "batch_id": batch_id, "accepted": len(events)}), 202


@app.route("/v1/logs/file", methods=["POST"])
def ingest_file():
    client = current_client()
    if not client:
        return unauthorized()
    upload = request.files.get("file")
    if not upload:
        return jsonify({"error": "file is required"}), 400
    text = upload.read().decode("utf-8", errors="replace").strip()
    events = []
    try:
        if text.startswith("["):
            events = json.loads(text)
        elif text.startswith("{") and "\n" not in text:
            parsed = json.loads(text)
            events = parsed.get("batch", [parsed]) if isinstance(parsed, dict) else []
        elif text.startswith("{"):
            events = [json.loads(line) for line in text.splitlines() if line.strip()]
        else:
            events = list(csv.DictReader(io.StringIO(text)))
    except Exception as exc:
        return jsonify({"error": "could not parse file: %s" % exc}), 400
    if not events:
        return jsonify({"error": "no events found in file"}), 400
    events = events[:50000]
    batch_id = new_batch(client, len(events), "file: " + (upload.filename or "upload"))
    enqueue(client, events, batch_id)
    return jsonify({"status": "accepted", "batch_id": batch_id, "accepted": len(events)}), 202


@app.route("/v1/logs/status/<batch_id>", methods=["GET"])
def batch_status(batch_id):
    batch = batches.get(batch_id)
    if not batch:
        return jsonify({"error": "unknown batch"}), 404
    return jsonify(batch)


@app.route("/v1/simulate", methods=["POST"])
def simulate():
    client = current_client()
    if not client:
        return unauthorized()
    body = request.get_json(silent=True) or {}
    count = max(1, min(int(body.get("count", 100)), 20000))
    names = ["asha", "bikram", "sita", "ram", "gita", "hari", "maya", "nabin", "pooja", "suman",
             "anil", "rina", "kiran", "dipesh", "sunita"]
    online, events = {}, []
    for _ in range(count):
        roll = random.random()
        source = random.choice(SOURCES)
        ip = "103.%d.%d.%d" % (random.randint(1, 250), random.randint(1, 250), random.randint(1, 250))
        if roll < 0.03:
            events.append(random.choice([
                {"action": "login", "source": source, "ip": ip},
                {"action": "teleport", "user_id": "ghost", "source": source},
                {"action": "login", "user_id": "bad.ip", "source": source, "ip": "999.1.1.1"},
            ]))
        elif online and roll < 0.40:
            user = random.choice(list(online))
            events.append({"action": "logout", "user_id": user, "source": online.pop(user), "ip": ip})
        elif roll < 0.48:
            events.append({"action": "register", "user_id": "%s%d@example.com" % (random.choice(names), random.randint(1, 400)),
                           "source": source, "ip": ip})
        else:
            user = "%s%d@example.com" % (random.choice(names), random.randint(1, 120))
            online[user] = online.get(user, source)
            events.append({"action": "login", "user_id": user, "source": online[user], "ip": ip})
    batch_id = new_batch(client, len(events), "simulated load")
    enqueue(client, events, batch_id)
    return jsonify({"status": "accepted", "batch_id": batch_id, "accepted": len(events)}), 202


@app.route("/v1/search", methods=["POST"])
def search_logs():
    client = current_client()
    if not client:
        return unauthorized()
    body = request.get_json(silent=True) or {}
    query = str(body.get("query", ""))
    size = max(1, min(int(body.get("size", 10)), 100))
    page = max(1, int(body.get("page", 1)))
    hit = elastic.search(client["id"], query, page, size)
    results = log_db.get_many(hit["ids"])
    return jsonify({
        "query": query, "total": hit["total"], "page": hit["page"], "pages": hit["pages"],
        "size": size, "results": results,
    })


@app.route("/logs/<log_id>", methods=["GET"])
def get_log(log_id):
    client = current_client()
    if not client:
        return unauthorized()
    row = log_db.get(log_id)
    if row and row["client_id"] == client["id"]:
        return jsonify(row)
    if log_id in inflight:
        return jsonify({"id": log_id, "status": "PENDING", "note": "accepted, not yet processed (eventual consistency)"}), 202
    return jsonify({"error": "log not found"}), 404


@app.route("/v1/live", methods=["GET"])
def live_stream():
    client = current_client()
    if not client:
        return unauthorized()
    since = request.args.get("since", type=int)
    if since is None:
        return jsonify({"events": [], "last_seq": log_db.latest_seq(client["id"])})
    events = log_db.since(client["id"], since)
    last = events[-1]["seq"] if events else since
    return jsonify({"events": events, "last_seq": last})


@app.route("/v1/stats", methods=["GET"])
def platform_stats():
    t = now()
    with stats_lock:
        while window_events and t - window_events[0][0] > 60:
            window_events.popleft()
        actions, sources, status = defaultdict(int), defaultdict(int), defaultdict(int)
        for _, action, source, st in window_events:
            actions[action] += 1
            sources[source] += 1
            status[st] += 1
        recent = [x for x in processed_times if t - x <= 5]
        avg_latency = round(sum(latencies) / len(latencies), 1) if latencies else 0
        data = {
            "totals": dict(counters),
            "kafka": {"partitions": kafka.depths(), "produced": kafka.produced, "consumed": kafka.consumed},
            "flink": {"throughput": round(len(recent) / 5, 1), "avg_latency_ms": avg_latency, "workers": NUM_PARTITIONS},
            "clients": len(client_db.all()),
            "es_docs": len(elastic.docs),
            "log_rows": log_db.count(),
            "window": {"actions": actions, "sources": sources, "status": status},
            "batches": [batches[b] for b in list(batch_order)[-6:]][::-1],
            "uptime_s": int(t - START_TIME),
        }
    data["totals"]["lag"] = max(0, kafka.produced - kafka.consumed)
    return jsonify(data)


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>AuthPulse - Login Activity Platform</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        :root {
            --bg: #f4f7fb;
            --card: #ffffff;
            --ink: #14213d;
            --muted: #5b6b86;
            --line: #dbe3f0;
            --blue: #2563eb;
            --blue-soft: #dbeafe;
            --green: #16a34a;
            --green-soft: #dcfce7;
            --orange: #ea580c;
            --orange-soft: #ffedd5;
            --purple: #7c3aed;
            --purple-soft: #ede9fe;
            --red: #dc2626;
            --red-soft: #fee2e2;
            --amber: #d97706;
            --amber-soft: #fef3c7;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: var(--bg);
            color: var(--ink);
            font-size: 14px;
        }

        header {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            padding: 14px 24px;
            background: #14213d;
            color: #ffffff;
        }

        header h1 {
            margin: 0;
            font-size: 20px;
            letter-spacing: 0.3px;
        }

        header small {
            display: block;
            color: #b6c4e0;
            margin-top: 2px;
        }

        .head-right {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .note {
            font-size: 12px;
            color: #b6c4e0;
        }

        select,
        input,
        textarea,
        button {
            font: inherit;
        }

        select,
        input,
        textarea {
            padding: 8px 10px;
            border: 1px solid var(--line);
            border-radius: 4px;
            background: #ffffff;
            color: var(--ink);
        }

        textarea {
            width: 100%;
            font-family: Consolas, Menlo, monospace;
            font-size: 12px;
        }

        button {
            padding: 8px 14px;
            border: none;
            border-radius: 4px;
            background: var(--blue);
            color: #ffffff;
            font-weight: 600;
            cursor: pointer;
        }

        button.ghost {
            background: #eef2fb;
            color: var(--ink);
        }

        nav {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            padding: 10px 24px 0;
        }

        nav button {
            background: transparent;
            color: var(--muted);
            border-radius: 4px 4px 0 0;
            border-bottom: 3px solid transparent;
        }

        nav button.active {
            color: var(--blue);
            border-bottom-color: var(--blue);
            background: #ffffff;
        }

        main {
            padding: 18px 24px 40px;
        }

        section.tab {
            display: none;
        }

        section.tab.active {
            display: block;
        }

        .card {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 4px;
            padding: 16px;
            margin-bottom: 16px;
        }

        .card h2 {
            margin: 0 0 10px;
            font-size: 16px;
        }

        .card p.hint {
            margin: 0 0 12px;
            color: var(--muted);
        }

        .grid {
            display: grid;
            gap: 14px;
        }

        .kpis {
            grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
            margin-bottom: 16px;
        }

        .kpi {
            border-left: 3px solid var(--line);
            padding: 2px 12px;
        }

        .kpi span {
            display: block;
            color: var(--muted);
            font-size: 12px;
        }

        .kpi strong {
            font-size: 24px;
        }

        .two {
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        }

        .row {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            align-items: flex-end;
            margin-bottom: 10px;
        }

        .row label {
            display: flex;
            flex-direction: column;
            gap: 4px;
            font-size: 12px;
            color: var(--muted);
        }

        .arch {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 10px 22px;
            align-items: start;
            max-width: 980px;
            margin: 0 auto;
        }

        .arch .span3 {
            grid-column: 1 / span 3;
            justify-self: center;
            width: 46%;
        }

        .col {
            display: flex;
            flex-direction: column;
            align-items: stretch;
            gap: 6px;
        }

        .node {
            border: 2px solid;
            border-radius: 4px;
            padding: 10px 12px;
            text-align: center;
            font-weight: 700;
        }

        .node small {
            display: block;
            font-weight: 500;
            color: var(--muted);
            margin-top: 3px;
        }

        .node.blue {
            background: var(--blue-soft);
            border-color: var(--blue);
        }

        .node.green {
            background: var(--green-soft);
            border-color: var(--green);
        }

        .node.orange {
            background: var(--orange-soft);
            border-color: var(--orange);
        }

        .node.purple {
            background: var(--purple-soft);
            border-color: var(--purple);
        }

        .arrow {
            text-align: center;
            color: #1e3a8a;
            font-size: 18px;
            line-height: 1;
        }

        .schema {
            border: 1px dashed #94a3b8;
            border-radius: 4px;
            padding: 8px 12px;
            background: #f1f5fb;
            font-size: 12px;
        }

        .schema b {
            display: block;
            background: #14213d;
            color: #ffffff;
            border-radius: 4px;
            padding: 3px 8px;
            margin-bottom: 4px;
        }

        .parts {
            display: flex;
            gap: 4px;
            margin-top: 6px;
        }

        .parts i {
            flex: 1;
            font-style: normal;
            font-size: 11px;
            background: #ffffff;
            border: 1px solid var(--orange);
            border-radius: 4px;
            padding: 2px 0;
        }

        .bar-row {
            display: flex;
            align-items: center;
            gap: 8px;
            margin: 5px 0;
            font-size: 12px;
        }

        .bar-row span.k {
            width: 80px;
            color: var(--muted);
        }

        .bar-row .track {
            flex: 1;
            background: #eef2fb;
            border-radius: 4px;
            height: 12px;
            overflow: hidden;
        }

        .bar-row .fill {
            height: 100%;
            background: var(--blue);
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th,
        td {
            text-align: left;
            padding: 7px 8px;
            border-bottom: 1px solid var(--line);
            font-size: 13px;
            vertical-align: top;
        }

        th {
            color: var(--muted);
            font-weight: 600;
            font-size: 12px;
        }

        .scroll {
            overflow-x: auto;
        }

        code,
        .mono {
            font-family: Consolas, Menlo, monospace;
            font-size: 12px;
        }

        .badge {
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 700;
        }

        .badge.VALID,
        .badge.completed {
            background: var(--green-soft);
            color: var(--green);
        }

        .badge.SUSPICIOUS,
        .badge.processing,
        .badge.PENDING {
            background: var(--amber-soft);
            color: var(--amber);
        }

        .badge.INVALID {
            background: var(--red-soft);
            color: var(--red);
        }

        a.link {
            color: var(--blue);
            cursor: pointer;
            text-decoration: underline;
        }

        pre {
            background: #0f172a;
            color: #e2e8f0;
            border-radius: 4px;
            padding: 12px;
            overflow: auto;
            max-height: 260px;
            font-size: 12px;
            margin: 10px 0 0;
        }

        .pager {
            display: flex;
            gap: 8px;
            align-items: center;
            margin-top: 10px;
        }

        .live-item {
            display: grid;
            grid-template-columns: 150px 1fr 90px 90px;
            gap: 8px;
            padding: 6px 8px;
            border-bottom: 1px solid var(--line);
            font-size: 13px;
        }

        #toast {
            position: fixed;
            bottom: 18px;
            right: 18px;
            background: #14213d;
            color: #ffffff;
            padding: 10px 16px;
            border-radius: 4px;
            opacity: 0;
            max-width: 340px;
            z-index: 10;
        }

        #toast.show {
            opacity: 1;
        }

        ul.plain {
            margin: 6px 0 0;
            padding-left: 18px;
        }

        ul.plain li {
            margin: 4px 0;
        }
    </style>
</head>
<body>
    <header>
        <div>
            <h1>AuthPulse &middot; Login Activity Platform</h1>
            <small>Multi-source login ingestion &middot; real-time validation &middot; searchable audit logs</small>
        </div>
        <div class="head-right">
            <span class="note">No login required</span>
            <label for="clientSelect" style="color:#b6c4e0">Client</label>
            <select id="clientSelect"></select>
        </div>
    </header>

    <nav id="tabs">
        <button data-tab="dashboard" class="active">Dashboard</button>
        <button data-tab="clients">Client Onboarding</button>
        <button data-tab="activity">Agent Activity</button>
        <button data-tab="ingest">File Ingestion</button>
        <button data-tab="search">Search</button>
        <button data-tab="live">Live Stream</button>
        <button data-tab="design">System Design</button>
    </nav>

    <main>
        <section id="tab-dashboard" class="tab active">
            <div class="grid kpis">
                <div class="kpi"><span>Ingested</span><strong id="kIngested">0</strong></div>
                <div class="kpi"><span>Processed</span><strong id="kProcessed">0</strong></div>
                <div class="kpi"><span>Valid</span><strong id="kValid" style="color:var(--green)">0</strong></div>
                <div class="kpi"><span>Suspicious</span><strong id="kSuspicious" style="color:var(--amber)">0</strong></div>
                <div class="kpi"><span>Invalid</span><strong id="kInvalid" style="color:var(--red)">0</strong></div>
                <div class="kpi"><span>Kafka lag</span><strong id="kLag">0</strong></div>
                <div class="kpi"><span>Events / sec</span><strong id="kRate">0</strong></div>
                <div class="kpi"><span>Avg latency (ms)</span><strong id="kLatency">0</strong></div>
            </div>

            <div class="card">
                <h2>Live architecture</h2>
                <p class="hint">Every number below is updated once per second from the running pipeline.</p>
                <div class="arch">
                    <div class="node blue span3">User / Agent</div>
                    <div class="node blue span3">API Gateway<small>auth by client token &middot; returns 202 immediately</small></div>

                    <div class="col">
                        <div class="node green">Client Onboarding<small id="nClients">0 clients</small></div>
                        <div class="arrow">&darr;</div>
                        <div class="node green">Client DB</div>
                        <div class="schema">
                            <b>Client Info (Postgres)</b>
                            &bull; ID<br>&bull; Name<br>&bull; token
                        </div>
                    </div>

                    <div class="col">
                        <div class="node orange">File Ingestion<small id="nIngest">0 events accepted</small></div>
                        <div class="arrow">&darr;</div>
                        <div class="node orange">Kafka<small id="nKafka">produced 0 / consumed 0</small>
                            <div class="parts" id="partBoxes"></div>
                        </div>
                        <div class="arrow">&darr;</div>
                        <div class="node purple">Apache Flink<small id="nFlink">validate &middot; enrich &middot; sink</small></div>
                        <div class="arrow">&darr;</div>
                        <div class="node blue">Log DB<small id="nLogDb">0 rows</small></div>
                    </div>

                    <div class="col">
                        <div class="node green">Search<small>query &middot; paginate</small></div>
                        <div class="arrow">&darr;</div>
                        <div class="node green">Elastic Search<small id="nEs">0 documents</small></div>
                        <div class="arrow">&swarr; hydrate via Flink / Log DB</div>
                    </div>
                </div>
            </div>

            <div class="grid two">
                <div class="card">
                    <h2>Last 60 seconds &middot; by action</h2>
                    <div id="barsActions"></div>
                    <h2 style="margin-top:14px">By source</h2>
                    <div id="barsSources"></div>
                </div>
                <div class="card">
                    <h2>Last 60 seconds &middot; validation outcome</h2>
                    <div id="barsStatus"></div>
                    <h2 style="margin-top:14px">Recent batches</h2>
                    <div class="scroll">
                        <table>
                            <thead>
                                <tr><th>Batch</th><th>Kind</th><th>Progress</th><th>Status</th></tr>
                            </thead>
                            <tbody id="batchRows"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </section>

        <section id="tab-clients" class="tab">
            <div class="card">
                <h2>Onboard a new client</h2>
                <p class="hint">A client is an app or company that reports login activity. Onboarding issues a token used by agents and ingestion jobs.</p>
                <div class="row">
                    <label>Client name
                        <input id="clientName" placeholder="e.g. Himalayan Bank Mobile" style="width:280px">
                    </label>
                    <button id="btnCreateClient">Onboard client</button>
                </div>
            </div>
            <div class="card">
                <h2>Client DB</h2>
                <div class="scroll">
                    <table>
                        <thead>
                            <tr><th>ID</th><th>Name</th><th>Token</th><th>Events logged</th></tr>
                        </thead>
                        <tbody id="clientRows"></tbody>
                    </table>
                </div>
            </div>
        </section>

        <section id="tab-activity" class="tab">
            <div class="card">
                <h2>Report login activity <code>POST /v1/agents/activity</code></h2>
                <p class="hint">Acts as an agent embedded in a client app. The event is queued to Kafka and validated by Flink: logout needs an active session, duplicate registrations are rejected, concurrent sessions from different sources are flagged.</p>
                <div class="row">
                    <label>Action
                        <select id="actAction">
                            <option>login</option>
                            <option>logout</option>
                            <option>register</option>
                        </select>
                    </label>
                    <label>User ID
                        <input id="actUser" value="asha@example.com" style="width:220px">
                    </label>
                    <label>Source
                        <select id="actSource"></select>
                    </label>
                    <label>IP address
                        <input id="actIp" value="103.10.20.30" style="width:150px">
                    </label>
                    <button id="btnActivity">Send event</button>
                </div>
                <div class="row">
                    <button class="ghost" data-quick="register">Quick: register</button>
                    <button class="ghost" data-quick="login">Quick: login</button>
                    <button class="ghost" data-quick="logout">Quick: logout</button>
                    <button class="ghost" id="btnBrute">Simulate 7 rapid logins</button>
                </div>
                <pre id="actOut">Response will appear here.</pre>
            </div>
        </section>

        <section id="tab-ingest" class="tab">
            <div class="grid two">
                <div class="card">
                    <h2>Batch ingest <code>POST /v1/logs</code></h2>
                    <p class="hint">Send a batch of events as JSON. You get a batch ID and can follow its status.</p>
                    <textarea id="batchText" rows="12"></textarea>
                    <div class="row" style="margin-top:10px">
                        <button id="btnBatch">Send batch</button>
                    </div>
                    <pre id="batchOut">Response will appear here.</pre>
                </div>
                <div class="card">
                    <h2>File ingestion</h2>
                    <p class="hint">Upload a CSV, JSON array or NDJSON file. CSV headers: <code>user_id,action,source,ip,ts</code></p>
                    <div class="row">
                        <input type="file" id="fileInput" accept=".csv,.json,.ndjson,.txt">
                        <button id="btnFile">Upload</button>
                        <button class="ghost" id="btnSampleCsv">Download sample CSV</button>
                    </div>
                    <h2 style="margin-top:18px">Load generator</h2>
                    <p class="hint">Push synthetic multi-source traffic through the whole pipeline and watch Kafka lag drain on the dashboard.</p>
                    <div class="row">
                        <button class="ghost" data-sim="100">100 events</button>
                        <button class="ghost" data-sim="1000">1,000 events</button>
                        <button class="ghost" data-sim="5000">5,000 events</button>
                    </div>
                    <pre id="fileOut">Response will appear here.</pre>
                </div>
            </div>
        </section>

        <section id="tab-search" class="tab">
            <div class="card">
                <h2>Search logs <code>POST /v1/search</code></h2>
                <p class="hint">Free text plus <code>field:value</code> terms, e.g. <code>asha status:suspicious</code>, <code>action:login source:ios</code>. Prefix matching is supported.</p>
                <div class="row">
                    <label>Query
                        <input id="searchQuery" placeholder="user, action:login, status:invalid ..." style="width:320px">
                    </label>
                    <label>Page size
                        <select id="searchSize">
                            <option>10</option>
                            <option>25</option>
                            <option>50</option>
                        </select>
                    </label>
                    <button id="btnSearch">Search</button>
                </div>
                <div id="searchMeta" class="hint"></div>
                <div class="scroll">
                    <table>
                        <thead>
                            <tr><th>Time (UTC)</th><th>User</th><th>Action</th><th>Source</th><th>IP</th><th>Status</th><th>Log ID</th></tr>
                        </thead>
                        <tbody id="searchRows"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button class="ghost" id="btnPrev">&larr; Prev</button>
                    <span id="pageInfo">Page 1</span>
                    <button class="ghost" id="btnNext">Next &rarr;</button>
                </div>
            </div>
            <div class="card">
                <h2>Log detail <code>GET /logs/{logID}</code></h2>
                <pre id="logDetail">Click a Log ID to load it.</pre>
            </div>
        </section>

        <section id="tab-live" class="tab">
            <div class="card">
                <h2>Real-time login stream</h2>
                <p class="hint">Events appear as soon as Flink has validated and written them (near-real-time, eventually consistent).</p>
                <div class="row">
                    <button id="btnPause" class="ghost">Pause</button>
                    <button id="btnClear" class="ghost">Clear</button>
                </div>
                <div class="live-item" style="font-weight:700;color:var(--muted)">
                    <span>Time</span><span>User / Reason</span><span>Action</span><span>Status</span>
                </div>
                <div id="liveList"></div>
            </div>
        </section>

        <section id="tab-design" class="tab">
            <div class="grid two">
                <div class="card">
                    <h2>1. Requirements</h2>
                    <b>Functional</b>
                    <ul class="plain">
                        <li>Login from multiple sources (web, mobile, SSO, API, kiosk)</li>
                        <li>Real-time streaming of login events</li>
                        <li>Validate logins</li>
                    </ul>
                    <b>Non-functional</b>
                    <ul class="plain">
                        <li>Scale 10M+ events</li>
                        <li>CAP: Availability &gt;&gt;&gt;&gt; Consistency &mdash; the gateway always answers 202 and the search index catches up</li>
                        <li>Low, near-real-time latency</li>
                    </ul>
                </div>
                <div class="card">
                    <h2>2. Identifying users</h2>
                    <ul class="plain">
                        <li><b>Log Event</b> &mdash; a single login / logout / register record</li>
                        <li><b>Ingestion Job</b> &mdash; batch or file uploads of historical logs</li>
                        <li><b>Agent</b> &mdash; SDK inside a client app reporting activity live</li>
                    </ul>
                </div>
                <div class="card">
                    <h2>3. API design</h2>
                    <ul class="plain">
                        <li><code>POST /v1/agents/activity</code> &mdash; {login | logout | register | user ID}</li>
                        <li><code>POST /v1/search</code> &mdash; {query} with pagination</li>
                        <li><code>GET /logs/{logsID}</code></li>
                        <li><code>POST /v1/logs</code> &mdash; {ingest + batch + status}; <code>GET /v1/logs/status/{batchID}</code></li>
                        <li><code>POST /v1/clients</code> &mdash; onboarding (issues token)</li>
                    </ul>
                </div>
                <div class="card">
                    <h2>4. High level design</h2>
                    <p>User &rarr; API Gateway &rarr; servers (Client Onboarding, File Ingestion, Search) &rarr; databases (Client DB, Log DB).</p>
                    <h2>5. Low level design</h2>
                    <p>Ingestion writes to Kafka (partitioned by client + user so per-user ordering holds). Flink validates and sinks to Log DB and Elasticsearch. Search queries Elasticsearch and hydrates documents from Log DB.</p>
                </div>
            </div>
        </section>
    </main>

    <div id="toast"></div>

    <script>
        const SOURCES = ["web", "android", "ios", "desktop", "sso", "api", "kiosk"];
        const state = { clients: [], token: "", page: 1, pages: 1, lastSeq: null, paused: false };
        const $ = (id) => document.getElementById(id);

        const esc = (s) => String(s === null || s === undefined ? "" : s).replace(/[&<>"']/g, (c) => (
            { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
        ));

        function toast(message) {
            const el = $("toast");
            el.textContent = message;
            el.classList.add("show");
            setTimeout(() => el.classList.remove("show"), 2600);
        }

        async function api(path, options) {
            const opts = options || {};
            const headers = Object.assign({}, opts.headers || {});
            if (state.token) {
                headers["X-Client-Token"] = state.token;
            }
            if (opts.body && !(opts.body instanceof FormData)) {
                headers["Content-Type"] = "application/json";
            }
            const res = await fetch(path, Object.assign({}, opts, { headers: headers }));
            let data = null;
            try {
                data = await res.json();
            } catch (err) {
                data = null;
            }
            return { ok: res.ok, status: res.status, data: data };
        }

        const show = (el, res) => {
            $(el).textContent = "HTTP " + res.status + "\n" + JSON.stringify(res.data, null, 2);
        };

        document.querySelectorAll("#tabs button").forEach((btn) => {
            btn.addEventListener("click", () => {
                document.querySelectorAll("#tabs button").forEach((b) => b.classList.remove("active"));
                document.querySelectorAll("section.tab").forEach((s) => s.classList.remove("active"));
                btn.classList.add("active");
                $("tab-" + btn.dataset.tab).classList.add("active");
                if (btn.dataset.tab === "clients") {
                    loadClients();
                }
            });
        });

        async function loadClients() {
            const res = await api("/v1/clients");
            state.clients = res.data.clients;
            const select = $("clientSelect");
            const previous = state.token;
            select.innerHTML = state.clients.map((c) => (
                '<option value="' + esc(c.token) + '">' + esc(c.name) + " (" + esc(c.id) + ")</option>"
            )).join("");
            if (previous && state.clients.some((c) => c.token === previous)) {
                select.value = previous;
            }
            state.token = select.value;
            $("clientRows").innerHTML = state.clients.map((c) => (
                "<tr><td>" + esc(c.id) + "</td><td>" + esc(c.name) + '</td><td class="mono">' +
                esc(c.token) + "</td><td>" + c.events + "</td></tr>"
            )).join("");
        }

        $("clientSelect").addEventListener("change", (e) => {
            state.token = e.target.value;
            state.lastSeq = null;
            $("liveList").innerHTML = "";
            toast("Now viewing " + $("clientSelect").selectedOptions[0].textContent);
        });

        $("btnCreateClient").addEventListener("click", async () => {
            const name = $("clientName").value.trim();
            if (!name) {
                toast("Enter a client name");
                return;
            }
            const res = await api("/v1/clients", { method: "POST", body: JSON.stringify({ name: name }) });
            if (res.ok) {
                state.token = res.data.token;
                $("clientName").value = "";
                await loadClients();
                $("clientSelect").value = res.data.token;
                state.lastSeq = null;
                toast("Client onboarded: " + res.data.id);
            }
        });

        $("actSource").innerHTML = SOURCES.map((s) => "<option>" + s + "</option>").join("");

        async function sendActivity(action) {
            const body = {
                action: action || $("actAction").value,
                user_id: $("actUser").value.trim(),
                source: $("actSource").value,
                ip: $("actIp").value.trim()
            };
            const res = await api("/v1/agents/activity", { method: "POST", body: JSON.stringify(body) });
            show("actOut", res);
            return res;
        }

        $("btnActivity").addEventListener("click", () => sendActivity());

        document.querySelectorAll("[data-quick]").forEach((btn) => {
            btn.addEventListener("click", () => {
                $("actAction").value = btn.dataset.quick;
                sendActivity(btn.dataset.quick);
            });
        });

        $("btnBrute").addEventListener("click", async () => {
            for (let i = 0; i < 7; i++) {
                await sendActivity("login");
            }
            toast("7 logins queued; the last ones should show as suspicious in Live Stream");
        });

        $("batchText").value = JSON.stringify([
            { action: "register", user_id: "sita@example.com", source: "web", ip: "103.1.2.3" },
            { action: "login", user_id: "sita@example.com", source: "web", ip: "103.1.2.3" },
            { action: "login", user_id: "sita@example.com", source: "ios", ip: "103.9.9.9" },
            { action: "logout", user_id: "ram@example.com", source: "android" },
            { action: "login", user_id: "", source: "sso" },
            { action: "logout", user_id: "sita@example.com", source: "ios" }
        ], null, 4);

        $("btnBatch").addEventListener("click", async () => {
            let batch;
            try {
                batch = JSON.parse($("batchText").value);
            } catch (err) {
                toast("Batch is not valid JSON");
                return;
            }
            const res = await api("/v1/logs", { method: "POST", body: JSON.stringify({ batch: batch }) });
            show("batchOut", res);
        });

        $("btnFile").addEventListener("click", async () => {
            const file = $("fileInput").files[0];
            if (!file) {
                toast("Choose a CSV, JSON or NDJSON file first");
                return;
            }
            const form = new FormData();
            form.append("file", file);
            const res = await api("/v1/logs/file", { method: "POST", body: form });
            show("fileOut", res);
        });

        $("btnSampleCsv").addEventListener("click", () => {
            const csv = "user_id,action,source,ip\n" +
                "kiran@example.com,register,web,103.4.4.4\n" +
                "kiran@example.com,login,web,103.4.4.4\n" +
                "kiran@example.com,logout,web,103.4.4.4\n" +
                "dipesh@example.com,logout,android,103.5.5.5\n";
            const link = document.createElement("a");
            link.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
            link.download = "sample_logins.csv";
            link.click();
        });

        document.querySelectorAll("[data-sim]").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const res = await api("/v1/simulate", {
                    method: "POST",
                    body: JSON.stringify({ count: Number(btn.dataset.sim) })
                });
                show("fileOut", res);
                toast("Queued " + btn.dataset.sim + " events; Kafka lag is on the Dashboard");
            });
        });

        function statusBadge(status) {
            return '<span class="badge ' + esc(status) + '">' + esc(status) + "</span>";
        }

        async function runSearch(page) {
            state.page = page || 1;
            const res = await api("/v1/search", {
                method: "POST",
                body: JSON.stringify({
                    query: $("searchQuery").value,
                    page: state.page,
                    size: Number($("searchSize").value)
                })
            });
            if (!res.ok) {
                toast("Search failed: " + (res.data && res.data.error ? res.data.error : "HTTP " + res.status));
                return;
            }
            const d = res.data;
            state.page = d.page;
            state.pages = d.pages;
            $("searchMeta").textContent = d.total + " matching log(s)";
            $("pageInfo").textContent = "Page " + d.page + " of " + d.pages;
            $("searchRows").innerHTML = d.results.map((r) => (
                "<tr><td>" + esc(r.event_time) + "</td><td>" + esc(r.user_id) + "</td><td>" + esc(r.action) +
                "</td><td>" + esc(r.source) + "</td><td>" + esc(r.ip) + "</td><td>" + statusBadge(r.status) +
                '</td><td><a class="link mono" data-log="' + esc(r.id) + '">' + esc(r.id) + "</a></td></tr>"
            )).join("") || '<tr><td colspan="7">No logs match this query. Try fewer terms or a field:value filter such as status:invalid.</td></tr>';
        }

        $("btnSearch").addEventListener("click", () => runSearch(1));
        $("searchQuery").addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                runSearch(1);
            }
        });
        $("btnPrev").addEventListener("click", () => {
            if (state.page > 1) {
                runSearch(state.page - 1);
            }
        });
        $("btnNext").addEventListener("click", () => {
            if (state.page < state.pages) {
                runSearch(state.page + 1);
            }
        });

        document.addEventListener("click", async (e) => {
            const id = e.target.dataset ? e.target.dataset.log : null;
            if (id) {
                const res = await api("/logs/" + encodeURIComponent(id));
                show("logDetail", res);
            }
        });

        $("btnPause").addEventListener("click", () => {
            state.paused = !state.paused;
            $("btnPause").textContent = state.paused ? "Resume" : "Pause";
        });
        $("btnClear").addEventListener("click", () => {
            $("liveList").innerHTML = "";
        });

        async function pollLive() {
            if (state.paused || !state.token) {
                return;
            }
            const url = state.lastSeq === null ? "/v1/live" : "/v1/live?since=" + state.lastSeq;
            const res = await api(url);
            if (!res.ok) {
                return;
            }
            state.lastSeq = res.data.last_seq;
            const list = $("liveList");
            res.data.events.forEach((ev) => {
                const row = document.createElement("div");
                row.className = "live-item";
                row.innerHTML = "<span>" + esc(ev.event_time.slice(11)) + "</span><span>" + esc(ev.user_id || "(none)") +
                    (ev.reason ? ' <small style="color:var(--muted)">- ' + esc(ev.reason) + "</small>" : "") +
                    "</span><span>" + esc(ev.action || "?") + " / " + esc(ev.source || "?") + "</span><span>" + statusBadge(ev.status) + "</span>";
                list.insertBefore(row, list.firstChild);
            });
            while (list.children.length > 100) {
                list.removeChild(list.lastChild);
            }
        }

        function bars(elId, obj) {
            const entries = Object.entries(obj).sort((a, b) => b[1] - a[1]);
            const max = Math.max(1, ...entries.map((e) => e[1]));
            $(elId).innerHTML = entries.map((e) => (
                '<div class="bar-row"><span class="k">' + esc(e[0]) + '</span><div class="track"><div class="fill" style="width:' +
                (e[1] / max * 100) + '%"></div></div><span>' + e[1] + "</span></div>"
            )).join("") || '<span class="hint">No events processed in the last 60 seconds.</span>';
        }

        async function refreshStats() {
            const res = await api("/v1/stats");
            if (!res.ok) {
                return;
            }
            const s = res.data;
            $("kIngested").textContent = s.totals.ingested;
            $("kProcessed").textContent = s.totals.processed;
            $("kValid").textContent = s.totals.valid;
            $("kSuspicious").textContent = s.totals.suspicious;
            $("kInvalid").textContent = s.totals.invalid;
            $("kLag").textContent = s.totals.lag;
            $("kRate").textContent = s.flink.throughput;
            $("kLatency").textContent = s.flink.avg_latency_ms;
            $("nClients").textContent = s.clients + " client(s)";
            $("nIngest").textContent = s.totals.ingested + " events accepted";
            $("nKafka").textContent = "produced " + s.kafka.produced + " / consumed " + s.kafka.consumed;
            $("nFlink").textContent = s.flink.workers + " workers \u00b7 " + s.flink.throughput + " ev/s";
            $("nLogDb").textContent = s.log_rows + " rows";
            $("nEs").textContent = s.es_docs + " documents";
            $("partBoxes").innerHTML = s.kafka.partitions.map((d, i) => "<i>P" + i + ": " + d + "</i>").join("");
            bars("barsActions", s.window.actions);
            bars("barsSources", s.window.sources);
            bars("barsStatus", s.window.status);
            $("batchRows").innerHTML = s.batches.map((b) => (
                '<tr><td class="mono">' + esc(b.id) + "</td><td>" + esc(b.kind) + "</td><td>" + b.processed + " / " + b.accepted +
                '</td><td><span class="badge ' + esc(b.status) + '">' + esc(b.status) + "</span></td></tr>"
            )).join("") || '<tr><td colspan="4">No batches yet. Send one from the File Ingestion tab.</td></tr>';
        }

        (async function boot() {
            await loadClients();
            await refreshStats();
            setInterval(refreshStats, 1000);
            setInterval(pollLive, 1000);
        })();
    </script>
</body>
</html>
"""


@app.route("/")
def index():
    return INDEX_HTML


client_db.create("Demo Bank App")


SERVER = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=SERVER.serve_forever, daemon=True).start()
_LOGIN_RUNTIME = {"stop": STOP, "server": SERVER}

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height="950")
    print("AuthPulse is running inside Colab (port %d)." % PORT)
except Exception:
    print("AuthPulse is running at http://127.0.0.1:%d" % PORT)