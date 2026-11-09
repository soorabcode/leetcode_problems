'''
Cloud Storage
1. Requirement analysis
1A. functional requrement
- Create account
- Upload files/folder
- System synchronization
- Ability to share
- Delete folders
1B. non functional requrement
- Scale : Million of users
- Cap theroem - Available >>>>> Consistency
- Support all files
- Upload latency < 200ms

2. Identifying Entities
- User
- Files

3. API Design 
3A. Create Account
POST /v1/account/user = {login + logout}
3B. Upload
POST /api/v1/user={userID}/upload/{folderID}
3C. Sync
POST /v1/userID/sync
3D. Share URL
GET /v1/files/uploads/chunk-url/{uploadID}
3E. Delete
PUT DEL /v1/files/{filesID}/Delete


4. High level design 
user -> api gateway -> sever ( user , fileuploader and filemetadata ) -> database ( userDB , metadataDB, cache )

5. low level design 
![Cloud Storage Low-Level Design](Cloud-Storage.jpg)
https://github.com/soorabcode/System-Design-Problems/tree/main/Cloud-Storage

'''
import os
import re
import time
import uuid
import hashlib
import threading
import secrets
import sqlite3
import mimetypes
from functools import wraps

from flask import Flask, request, jsonify, send_file, Response, g
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.serving import make_server
from google.colab import output


PORT = 5050
ROOT_DIR = "/content/nimbusdrive"
BLOB_DIR = os.path.join(ROOT_DIR, "blobs")
DB_PATH = os.path.join(ROOT_DIR, "metadata.db")

QUOTA_BYTES = 500 * 1024 * 1024
SESSION_TTL = 12 * 60 * 60
CACHE_TTL = 30
MAX_FAILED_LOGINS = 5
LOCKOUT_SECONDS = 300

os.makedirs(BLOB_DIR, exist_ok=True)


_lock = threading.RLock()
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row

with _lock:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id         TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            email      TEXT NOT NULL UNIQUE,
            pw_hash    TEXT NOT NULL,
            counter    INTEGER NOT NULL DEFAULT 0,
            created    REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token      TEXT PRIMARY KEY,
            user_id    TEXT NOT NULL,
            expires    REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS items (
            id          TEXT PRIMARY KEY,
            user_id     TEXT NOT NULL,
            parent_id   TEXT NOT NULL,
            name        TEXT NOT NULL,
            is_folder   INTEGER NOT NULL,
            size        INTEGER NOT NULL DEFAULT 0,
            mime        TEXT,
            sha256      TEXT,
            version     INTEGER NOT NULL,
            deleted     INTEGER NOT NULL DEFAULT 0,
            share_token TEXT,
            created     REAL NOT NULL,
            updated     REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_items_parent ON items (user_id, parent_id, deleted);
        CREATE INDEX IF NOT EXISTS idx_items_version ON items (user_id, version);
        CREATE INDEX IF NOT EXISTS idx_items_share ON items (share_token);
    """)
    conn.commit()


def q(sql, args=()):
    """Run one statement under a lock, commit, and return all rows."""
    with _lock:
        cur = conn.execute(sql, args)
        rows = cur.fetchall()
        conn.commit()
        return rows


def bump(uid):
    """Increase the per-user change counter (drives sync)."""
    with _lock:
        conn.execute("UPDATE users SET counter = counter + 1 WHERE id = ?", (uid,))
        row = conn.execute("SELECT counter FROM users WHERE id = ?", (uid,)).fetchone()
        conn.commit()
        return row["counter"]


CACHE = {}


def cache_get(key):
    hit = CACHE.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    return None


def cache_set(key, value):
    CACHE[key] = (time.time(), value)


def cache_invalidate(uid):
    for key in [k for k in CACHE if k[0] == uid]:
        CACHE.pop(key, None)


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
FAILS = {}


def clean_name(name):
    name = re.sub(r"[\x00-\x1f]", "", name or "").strip()
    name = name.replace("/", "_").replace("\\", "_")
    return name[:200] or "untitled"


def item_json(row):
    return {
        "id": row["id"],
        "name": row["name"],
        "is_folder": bool(row["is_folder"]),
        "size": row["size"],
        "mime": row["mime"],
        "parent_id": row["parent_id"],
        "updated": row["updated"],
        "shared": bool(row["share_token"]),
        "version": row["version"],
    }


def usage(uid):
    row = q(
        "SELECT COALESCE(SUM(size), 0) AS used FROM items "
        "WHERE user_id = ? AND is_folder = 0 AND deleted = 0",
        (uid,),
    )[0]
    return row["used"]


def folder_exists(uid, folder_id):
    if folder_id == "root":
        return True
    rows = q(
        "SELECT id FROM items WHERE id = ? AND user_id = ? AND is_folder = 1 AND deleted = 0",
        (folder_id, uid),
    )
    return bool(rows)


def unique_name(uid, parent, name, is_folder):
    base, ext = os.path.splitext(name) if not is_folder else (name, "")
    candidate = name
    n = 1
    while q(
        "SELECT 1 FROM items WHERE user_id = ? AND parent_id = ? AND name = ? AND deleted = 0",
        (uid, parent, candidate),
    ):
        candidate = "%s (%d)%s" % (base, n, ext)
        n += 1
    return candidate


def make_folder(uid, parent, name):
    version = bump(uid)
    fid = uuid.uuid4().hex
    now = time.time()
    q(
        "INSERT INTO items (id, user_id, parent_id, name, is_folder, version, created, updated) "
        "VALUES (?, ?, ?, ?, 1, ?, ?, ?)",
        (fid, uid, parent, name, version, now, now),
    )
    return fid


def ensure_path(uid, parent, parts):
    """Create (or reuse) nested folders for folder uploads."""
    for part in parts:
        existing = q(
            "SELECT id FROM items WHERE user_id = ? AND parent_id = ? AND name = ? "
            "AND is_folder = 1 AND deleted = 0",
            (uid, parent, part),
        )
        parent = existing[0]["id"] if existing else make_folder(uid, parent, part)
    return parent


def breadcrumbs(uid, folder_id):
    trail = []
    current = folder_id
    while current != "root":
        rows = q("SELECT id, name, parent_id FROM items WHERE id = ? AND user_id = ?", (current, uid))
        if not rows:
            break
        trail.append({"id": rows[0]["id"], "name": rows[0]["name"]})
        current = rows[0]["parent_id"]
    trail.append({"id": "root", "name": "My Drive"})
    trail.reverse()
    return trail


def blob_path(uid, item_id):
    folder = os.path.join(BLOB_DIR, uid)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, item_id)


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024


def auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        header = request.headers.get("Authorization", "")
        token = header[7:] if header.startswith("Bearer ") else request.args.get("token", "")
        rows = q(
            "SELECT s.user_id, s.expires, u.name, u.email FROM sessions s "
            "JOIN users u ON u.id = s.user_id WHERE s.token = ?",
            (token,),
        )
        if not rows or rows[0]["expires"] < time.time():
            return jsonify(error="Session expired. Please log in again."), 401
        g.uid = rows[0]["user_id"]
        g.token = token
        if "uid" in kwargs and kwargs["uid"] != g.uid:
            return jsonify(error="Forbidden"), 403
        return fn(*args, **kwargs)
    return wrapper


@app.after_request
def no_store(resp):
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/v1/account/user", methods=["POST"])
def account():
    body = request.get_json(silent=True) or {}
    action = body.get("action")

    if action == "register":
        name = clean_name(body.get("name"))
        email = (body.get("email") or "").strip().lower()
        password = body.get("password") or ""
        if len(name) < 2:
            return jsonify(error="Name must be at least 2 characters."), 400
        if not EMAIL_RE.match(email):
            return jsonify(error="Enter a valid email address."), 400
        if len(password) < 8 or len(password) > 72 or not re.search(r"[A-Za-z]", password) \
                or not re.search(r"\d", password):
            return jsonify(error="Password needs 8+ characters with letters and numbers."), 400
        if q("SELECT 1 FROM users WHERE email = ?", (email,)):
            return jsonify(error="An account with this email already exists."), 409
        uid = uuid.uuid4().hex
        q(
            "INSERT INTO users (id, name, email, pw_hash, created) VALUES (?, ?, ?, ?, ?)",
            (uid, name, email, generate_password_hash(password), time.time()),
        )
        return jsonify(message="Account created. You can log in now."), 201

    if action == "login":
        email = (body.get("email") or "").strip().lower()
        password = body.get("password") or ""
        count, locked_until = FAILS.get(email, (0, 0))
        if locked_until > time.time():
            wait = int(locked_until - time.time())
            return jsonify(error="Too many attempts. Try again in %d seconds." % wait), 429
        rows = q("SELECT * FROM users WHERE email = ?", (email,))
        if not rows or not check_password_hash(rows[0]["pw_hash"], password):
            count += 1
            locked = time.time() + LOCKOUT_SECONDS if count >= MAX_FAILED_LOGINS else 0
            FAILS[email] = (0 if locked else count, locked)
            return jsonify(error="Invalid email or password."), 401
        FAILS.pop(email, None)
        token = secrets.token_urlsafe(32)
        q(
            "INSERT INTO sessions (token, user_id, expires) VALUES (?, ?, ?)",
            (token, rows[0]["id"], time.time() + SESSION_TTL),
        )
        return jsonify(
            token=token,
            user={"id": rows[0]["id"], "name": rows[0]["name"], "email": rows[0]["email"]},
            quota=QUOTA_BYTES,
        )

    if action == "logout":
        header = request.headers.get("Authorization", "")
        token = header[7:] if header.startswith("Bearer ") else ""
        q("DELETE FROM sessions WHERE token = ?", (token,))
        return jsonify(message="Logged out.")

    return jsonify(error="Unknown action."), 400


@app.route("/v1/user/<uid>/folder/<folder_id>", methods=["GET"])
@auth
def list_folder(uid, folder_id):
    if not folder_exists(uid, folder_id):
        return jsonify(error="Folder not found."), 404
    key = (uid, folder_id)
    cached = cache_get(key)
    hit = cached is not None
    if not hit:
        rows = q(
            "SELECT * FROM items WHERE user_id = ? AND parent_id = ? AND deleted = 0 "
            "ORDER BY is_folder DESC, name COLLATE NOCASE",
            (uid, folder_id),
        )
        cached = [item_json(r) for r in rows]
        cache_set(key, cached)
    resp = jsonify(
        items=cached,
        path=breadcrumbs(uid, folder_id),
        used=usage(uid),
        quota=QUOTA_BYTES,
        cache="HIT" if hit else "MISS",
    )
    resp.headers["X-Cache"] = "HIT" if hit else "MISS"
    return resp


@app.route("/v1/user/<uid>/folder", methods=["POST"])
@auth
def create_folder(uid):
    body = request.get_json(silent=True) or {}
    parent = body.get("parent_id") or "root"
    if not folder_exists(uid, parent):
        return jsonify(error="Parent folder not found."), 404
    name = unique_name(uid, parent, clean_name(body.get("name")), True)
    fid = make_folder(uid, parent, name)
    cache_invalidate(uid)
    return jsonify(id=fid, name=name), 201


@app.route("/api/v1/user=<uid>/upload/<folder_id>", methods=["POST"])
@auth
def upload(uid, folder_id):
    started = time.time()
    if not folder_exists(uid, folder_id):
        return jsonify(error="Destination folder not found."), 404

    files = request.files.getlist("files")
    paths = request.form.getlist("paths")
    if not files:
        return jsonify(error="No files received."), 400

    used = usage(uid)
    saved, failed = [], []

    for index, storage in enumerate(files):
        rel = paths[index] if index < len(paths) else storage.filename
        parts = [clean_name(p) for p in rel.replace("\\", "/").split("/") if p not in ("", ".", "..")]
        if not parts:
            continue
        filename, folders = parts[-1], parts[:-1]

        item_id = uuid.uuid4().hex
        target = blob_path(uid, item_id)
        digest = hashlib.sha256()
        size = 0
        with open(target, "wb") as out:
            while True:
                chunk = storage.stream.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                digest.update(chunk)
                size += len(chunk)

        if used + size > QUOTA_BYTES:
            os.remove(target)
            failed.append({"name": filename, "error": "Storage quota exceeded."})
            continue

        parent = ensure_path(uid, folder_id, folders)
        final_name = unique_name(uid, parent, filename, False)
        version = bump(uid)
        now = time.time()
        mime = mimetypes.guess_type(final_name)[0] or "application/octet-stream"
        q(
            "INSERT INTO items (id, user_id, parent_id, name, is_folder, size, mime, sha256, "
            "version, created, updated) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)",
            (item_id, uid, parent, final_name, size, mime, digest.hexdigest(), version, now, now),
        )
        used += size
        saved.append({"id": item_id, "name": final_name, "size": size})

    cache_invalidate(uid)
    return jsonify(
        saved=saved,
        failed=failed,
        server_ms=round((time.time() - started) * 1000, 1),
    ), 201


@app.route("/v1/<uid>/sync", methods=["POST"])
@auth
def sync(uid):
    body = request.get_json(silent=True) or {}
    try:
        cursor = int(body.get("cursor", 0))
    except (TypeError, ValueError):
        cursor = 0
    rows = q(
        "SELECT * FROM items WHERE user_id = ? AND version > ? ORDER BY version",
        (uid, cursor),
    )
    counter = q("SELECT counter FROM users WHERE id = ?", (uid,))[0]["counter"]
    changes = []
    for r in rows:
        entry = item_json(r)
        entry["deleted"] = bool(r["deleted"])
        changes.append(entry)
    return jsonify(cursor=counter, changes=changes, server_time=time.time())


@app.route("/v1/files/<file_id>/share", methods=["POST"])
@auth
def share(file_id):
    body = request.get_json(silent=True) or {}
    rows = q(
        "SELECT * FROM items WHERE id = ? AND user_id = ? AND is_folder = 0 AND deleted = 0",
        (file_id, g.uid),
    )
    if not rows:
        return jsonify(error="File not found."), 404
    enable = bool(body.get("enable", True))
    token = rows[0]["share_token"] or secrets.token_urlsafe(16) if enable else None
    version = bump(g.uid)
    q(
        "UPDATE items SET share_token = ?, version = ?, updated = ? WHERE id = ?",
        (token, version, time.time(), file_id),
    )
    cache_invalidate(g.uid)
    return jsonify(shared=enable, url=(request.host_url.rstrip("/") + "/d/" + token) if token else None)


@app.route("/v1/files/uploads/chunk-url/<upload_id>", methods=["GET"])
@auth
def chunk_url(upload_id):
    rows = q(
        "SELECT * FROM items WHERE id = ? AND user_id = ? AND is_folder = 0 AND deleted = 0",
        (upload_id, g.uid),
    )
    if not rows:
        return jsonify(error="Upload not found."), 404
    token = rows[0]["share_token"]
    return jsonify(
        upload_id=upload_id,
        name=rows[0]["name"],
        size=rows[0]["size"],
        sha256=rows[0]["sha256"],
        shared=bool(token),
        url=(request.host_url.rstrip("/") + "/d/" + token) if token else None,
    )


@app.route("/d/<token>", methods=["GET"])
def public_download(token):
    rows = q("SELECT * FROM items WHERE share_token = ? AND deleted = 0", (token,))
    if not rows:
        return Response("This link is invalid or sharing was turned off.", status=404)
    row = rows[0]
    return send_file(
        blob_path(row["user_id"], row["id"]),
        as_attachment=True,
        download_name=row["name"],
        mimetype=row["mime"] or "application/octet-stream",
    )


@app.route("/v1/files/<file_id>/download", methods=["GET"])
@auth
def download(file_id):
    rows = q(
        "SELECT * FROM items WHERE id = ? AND user_id = ? AND is_folder = 0 AND deleted = 0",
        (file_id, g.uid),
    )
    if not rows:
        return jsonify(error="File not found."), 404
    row = rows[0]
    return send_file(
        blob_path(g.uid, row["id"]),
        as_attachment=True,
        download_name=row["name"],
        mimetype=row["mime"] or "application/octet-stream",
    )


@app.route("/v1/files/<file_id>/Delete", methods=["PUT", "DELETE"])
@auth
def delete_item(file_id):
    rows = q("SELECT * FROM items WHERE id = ? AND user_id = ? AND deleted = 0", (file_id, g.uid))
    if not rows:
        return jsonify(error="Item not found."), 404

    queue, removed = [file_id], 0
    while queue:
        current = queue.pop()
        row = q("SELECT * FROM items WHERE id = ? AND user_id = ?", (current, g.uid))[0]
        if row["is_folder"]:
            children = q(
                "SELECT id FROM items WHERE parent_id = ? AND user_id = ? AND deleted = 0",
                (current, g.uid),
            )
            queue.extend(c["id"] for c in children)
        else:
            try:
                os.remove(blob_path(g.uid, current))
            except OSError:
                pass
        version = bump(g.uid)
        q(
            "UPDATE items SET deleted = 1, share_token = NULL, version = ?, updated = ? WHERE id = ?",
            (version, time.time(), current),
        )
        removed += 1

    cache_invalidate(g.uid)
    return jsonify(message="Deleted.", removed=removed)


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NimbusDrive</title>
    <style>
        :root {
            --bg: #f4f6fb;
            --card: #ffffff;
            --ink: #1b2437;
            --muted: #6b7690;
            --line: #e3e8f2;
            --brand: #3b6cf6;
            --brand-dark: #2c55cc;
            --brand-soft: #e8eefe;
            --danger: #e2455a;
            --ok: #1fa971;
            --radius: 12px;
            --shadow: 0 6px 24px rgba(30, 50, 100, 0.08);
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: var(--bg);
            color: var(--ink);
        }

        button {
            font: inherit;
            cursor: pointer;
        }

        .hidden {
            display: none !important;
        }

        .auth-wrap {
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 24px;
            background: linear-gradient(135deg, #e8eefe 0%, #f4f6fb 60%);
        }

        .auth-card {
            width: 100%;
            max-width: 400px;
            background: var(--card);
            border-radius: 16px;
            box-shadow: var(--shadow);
            padding: 32px;
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 22px;
            font-weight: 700;
            margin-bottom: 6px;
        }

        .logo-mark {
            width: 36px;
            height: 36px;
            border-radius: 10px;
            background: var(--brand);
            color: #fff;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
        }

        .tagline {
            color: var(--muted);
            font-size: 14px;
            margin: 0 0 20px;
        }

        .tabs {
            display: flex;
            background: var(--bg);
            border-radius: 10px;
            padding: 4px;
            margin-bottom: 18px;
        }

        .tab {
            flex: 1;
            border: 0;
            background: transparent;
            padding: 9px;
            border-radius: 8px;
            color: var(--muted);
            font-weight: 600;
        }

        .tab.active {
            background: var(--card);
            color: var(--brand);
            box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);
        }

        label {
            display: block;
            font-size: 13px;
            font-weight: 600;
            margin: 12px 0 5px;
        }

        input[type="text"],
        input[type="email"],
        input[type="password"] {
            width: 100%;
            padding: 11px 12px;
            border: 1px solid var(--line);
            border-radius: 9px;
            font: inherit;
            outline: none;
        }

        input:focus {
            border-color: var(--brand);
            box-shadow: 0 0 0 3px var(--brand-soft);
        }

        .policy {
            font-size: 12px;
            color: var(--muted);
            margin: 8px 0 0;
            line-height: 1.5;
        }

        .btn {
            border: 0;
            border-radius: 9px;
            padding: 10px 16px;
            font-weight: 600;
            background: var(--brand);
            color: #fff;
        }

        .btn:hover {
            background: var(--brand-dark);
        }

        .btn.block {
            width: 100%;
            margin-top: 18px;
            padding: 12px;
        }

        .btn.ghost {
            background: var(--brand-soft);
            color: var(--brand);
        }

        .btn.ghost:hover {
            background: #d8e2fd;
        }

        .btn.danger {
            background: var(--danger);
        }

        .btn.plain {
            background: transparent;
            color: var(--muted);
            border: 1px solid var(--line);
        }

        .form-msg {
            margin-top: 14px;
            font-size: 13px;
            min-height: 18px;
        }

        .form-msg.error {
            color: var(--danger);
        }

        .form-msg.ok {
            color: var(--ok);
        }

        .shell {
            display: flex;
            min-height: 100vh;
        }

        .sidebar {
            width: 250px;
            background: var(--card);
            border-right: 1px solid var(--line);
            padding: 22px 18px;
            display: flex;
            flex-direction: column;
            gap: 18px;
        }

        .user-box {
            background: var(--bg);
            border-radius: var(--radius);
            padding: 12px;
        }

        .user-name {
            font-weight: 700;
        }

        .user-email {
            font-size: 12px;
            color: var(--muted);
            word-break: break-all;
        }

        .meter {
            height: 8px;
            border-radius: 99px;
            background: var(--line);
            overflow: hidden;
            margin: 8px 0 6px;
        }

        .meter-fill {
            height: 100%;
            width: 0;
            background: var(--brand);
            transition: width 0.3s;
        }

        .small {
            font-size: 12px;
            color: var(--muted);
        }

        .sync-box {
            border: 1px solid var(--line);
            border-radius: var(--radius);
            padding: 12px;
        }

        .sync-row {
            display: flex;
            align-items: center;
            gap: 8px;
            font-weight: 600;
            font-size: 14px;
        }

        .dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: var(--ok);
        }

        .dot.busy {
            background: #f0a500;
        }

        .dot.err {
            background: var(--danger);
        }

        .main {
            flex: 1;
            padding: 24px 28px;
            min-width: 0;
        }

        .toolbar {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            align-items: center;
            margin-bottom: 16px;
        }

        .crumbs {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 6px;
            font-size: 15px;
            flex: 1;
            min-width: 200px;
        }

        .crumb {
            border: 0;
            background: transparent;
            color: var(--brand);
            font-weight: 600;
            padding: 4px 6px;
            border-radius: 6px;
        }

        .crumb:hover {
            background: var(--brand-soft);
        }

        .crumb.current {
            color: var(--ink);
            cursor: default;
        }

        .crumb.current:hover {
            background: transparent;
        }

        .dropzone {
            border: 2px dashed var(--line);
            border-radius: var(--radius);
            background: var(--card);
            padding: 18px;
            text-align: center;
            color: var(--muted);
            font-size: 14px;
            margin-bottom: 16px;
            transition: all 0.15s;
        }

        .dropzone.over {
            border-color: var(--brand);
            background: var(--brand-soft);
            color: var(--brand);
        }

        .progress {
            height: 6px;
            border-radius: 99px;
            background: var(--line);
            margin-top: 10px;
            overflow: hidden;
        }

        .progress-fill {
            height: 100%;
            width: 0;
            background: var(--ok);
            transition: width 0.15s;
        }

        .table-card {
            background: var(--card);
            border-radius: var(--radius);
            box-shadow: var(--shadow);
            overflow: hidden;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th {
            text-align: left;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: var(--muted);
            padding: 12px 16px;
            background: #fafbfe;
            border-bottom: 1px solid var(--line);
        }

        td {
            padding: 12px 16px;
            border-bottom: 1px solid var(--line);
            font-size: 14px;
        }

        tr:last-child td {
            border-bottom: 0;
        }

        tbody tr:hover {
            background: #fafbfe;
        }

        .name-cell {
            display: flex;
            align-items: center;
            gap: 10px;
            font-weight: 600;
        }

        .name-link {
            cursor: pointer;
            color: var(--ink);
            background: transparent;
            border: 0;
            padding: 0;
            font-weight: 600;
            text-align: left;
        }

        .name-link:hover {
            color: var(--brand);
        }

        .badge {
            font-size: 11px;
            background: var(--brand-soft);
            color: var(--brand);
            padding: 2px 8px;
            border-radius: 99px;
            font-weight: 600;
        }

        .actions {
            display: flex;
            gap: 6px;
            justify-content: flex-end;
        }

        .icon-btn {
            border: 1px solid var(--line);
            background: var(--card);
            border-radius: 8px;
            padding: 6px 10px;
            font-size: 12px;
            font-weight: 600;
            color: var(--ink);
        }

        .icon-btn:hover {
            border-color: var(--brand);
            color: var(--brand);
        }

        .icon-btn.del:hover {
            border-color: var(--danger);
            color: var(--danger);
        }

        .empty {
            text-align: center;
            padding: 48px 16px;
            color: var(--muted);
        }

        .stats {
            margin-top: 12px;
            font-size: 12px;
            color: var(--muted);
        }

        .overlay {
            position: fixed;
            inset: 0;
            background: rgba(20, 30, 60, 0.45);
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
            z-index: 50;
        }

        .modal {
            width: 100%;
            max-width: 440px;
            background: var(--card);
            border-radius: 14px;
            padding: 24px;
            box-shadow: var(--shadow);
        }

        .modal h3 {
            margin: 0 0 10px;
        }

        .modal p {
            margin: 0 0 14px;
            color: var(--muted);
            font-size: 14px;
            word-break: break-word;
        }

        .modal-actions {
            display: flex;
            gap: 8px;
            justify-content: flex-end;
            margin-top: 16px;
        }

        .toast {
            position: fixed;
            bottom: 22px;
            left: 50%;
            transform: translateX(-50%);
            background: var(--ink);
            color: #fff;
            padding: 11px 18px;
            border-radius: 10px;
            font-size: 14px;
            z-index: 60;
            box-shadow: var(--shadow);
        }

        @media (max-width: 760px) {
            .shell {
                flex-direction: column;
            }

            .sidebar {
                width: 100%;
                border-right: 0;
                border-bottom: 1px solid var(--line);
            }

            .col-hide {
                display: none;
            }
        }
    </style>
</head>
<body>

    <div id="authView" class="auth-wrap">
        <div class="auth-card">
            <div class="logo">
                <div class="logo-mark">&#9729;</div>
                NimbusDrive
            </div>
            <p class="tagline">Your files, synced everywhere.</p>

            <div class="tabs">
                <button class="tab active" id="tabLogin" onclick="showTab('login')">Log in</button>
                <button class="tab" id="tabRegister" onclick="showTab('register')">Create account</button>
            </div>

            <div id="nameField" class="hidden">
                <label for="name">Full name</label>
                <input type="text" id="name" autocomplete="name">
            </div>

            <label for="email">Email</label>
            <input type="email" id="email" autocomplete="email">

            <label for="password">Password</label>
            <input type="password" id="password" autocomplete="current-password">

            <p class="policy" id="policyText">
                Login policy: 5 wrong attempts lock the account for 5 minutes. Sessions last 12 hours.
            </p>

            <button class="btn block" id="authBtn" onclick="submitAuth()">Log in</button>
            <div class="form-msg" id="authMsg"></div>
        </div>
    </div>

    <div id="appView" class="shell hidden">

        <aside class="sidebar">
            <div class="logo">
                <div class="logo-mark">&#9729;</div>
                NimbusDrive
            </div>

            <div class="user-box">
                <div class="user-name" id="userName"></div>
                <div class="user-email" id="userEmail"></div>
            </div>

            <div>
                <div class="small">Storage used</div>
                <div class="meter">
                    <div class="meter-fill" id="meterFill"></div>
                </div>
                <div class="small" id="meterText"></div>
            </div>

            <div class="sync-box">
                <div class="sync-row">
                    <span class="dot" id="syncDot"></span>
                    <span id="syncLabel">Synced</span>
                </div>
                <div class="small" id="syncDetail">Waiting for first sync...</div>
                <button class="btn ghost" style="margin-top:10px;width:100%" onclick="syncNow(true)">Sync now</button>
            </div>

            <button class="btn plain" style="margin-top:auto" onclick="logout()">Log out</button>
        </aside>

        <main class="main">
            <div class="toolbar">
                <div class="crumbs" id="crumbs"></div>
                <button class="btn ghost" onclick="newFolder()">+ New folder</button>
                <button class="btn" onclick="document.getElementById('filePick').click()">Upload files</button>
                <button class="btn" onclick="document.getElementById('folderPick').click()">Upload folder</button>
                <input type="file" id="filePick" multiple class="hidden">
                <input type="file" id="folderPick" webkitdirectory directory multiple class="hidden">
            </div>

            <div class="dropzone" id="dropzone">
                Drag and drop files here to upload them to this folder
                <div class="progress hidden" id="progress">
                    <div class="progress-fill" id="progressFill"></div>
                </div>
            </div>

            <div class="table-card">
                <table>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th class="col-hide">Size</th>
                            <th class="col-hide">Modified</th>
                            <th style="text-align:right">Actions</th>
                        </tr>
                    </thead>
                    <tbody id="rows"></tbody>
                </table>
                <div class="empty hidden" id="empty">This folder is empty. Upload something to get started.</div>
            </div>

            <div class="stats" id="stats"></div>
        </main>
    </div>

    <div id="modalHost"></div>
    <div id="toast" class="toast hidden"></div>

    <script>
        let token = null;
        let user = null;
        let quota = 0;
        let folderId = 'root';
        let cursor = 0;
        let authMode = 'login';
        let syncTimer = null;
        let stats = { cache: '-', upload: '-' };

        function esc(text) {
            const div = document.createElement('div');
            div.textContent = text == null ? '' : String(text);
            return div.innerHTML;
        }

        function fmtSize(bytes) {
            if (bytes < 1024) return bytes + ' B';
            const units = ['KB', 'MB', 'GB'];
            let value = bytes / 1024;
            let i = 0;
            while (value >= 1024 && i < units.length - 1) {
                value /= 1024;
                i++;
            }
            return value.toFixed(value < 10 ? 1 : 0) + ' ' + units[i];
        }

        function fmtDate(ts) {
            return new Date(ts * 1000).toLocaleString([], {
                month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
            });
        }

        function icon(item) {
            if (item.is_folder) return '\uD83D\uDCC1';
            const mime = item.mime || '';
            if (mime.startsWith('image/')) return '\uD83D\uDDBC\uFE0F';
            if (mime.startsWith('video/')) return '\uD83C\uDFAC';
            if (mime.startsWith('audio/')) return '\uD83C\uDFB5';
            if (mime.includes('pdf')) return '\uD83D\uDCD5';
            if (mime.includes('zip') || mime.includes('compressed')) return '\uD83D\uDDDC\uFE0F';
            return '\uD83D\uDCC4';
        }

        function toast(message) {
            const el = document.getElementById('toast');
            el.textContent = message;
            el.classList.remove('hidden');
            clearTimeout(toast.timer);
            toast.timer = setTimeout(() => el.classList.add('hidden'), 2800);
        }

        function storeSession() {
            try {
                localStorage.setItem('nimbus', JSON.stringify({ token, user, quota }));
            } catch (e) { }
        }

        function loadSession() {
            try {
                const saved = JSON.parse(localStorage.getItem('nimbus') || 'null');
                if (saved && saved.token) {
                    token = saved.token;
                    user = saved.user;
                    quota = saved.quota;
                    return true;
                }
            } catch (e) { }
            return false;
        }

        function clearSession() {
            try { localStorage.removeItem('nimbus'); } catch (e) { }
            token = null;
            user = null;
        }

        async function api(path, options = {}) {
            const headers = Object.assign({}, options.headers || {});
            if (token) headers['Authorization'] = 'Bearer ' + token;
            if (options.json) {
                headers['Content-Type'] = 'application/json';
                options.body = JSON.stringify(options.json);
            }
            const response = await fetch(path, Object.assign({}, options, { headers }));
            if (response.status === 401 && token) {
                showAuth('Your session expired. Please log in again.');
                throw new Error('unauthorized');
            }
            return response;
        }

        function openModal(title, bodyHtml, buttons) {
            const host = document.getElementById('modalHost');
            host.innerHTML =
                '<div class="overlay">' +
                '    <div class="modal">' +
                '        <h3>' + esc(title) + '</h3>' +
                '        <div>' + bodyHtml + '</div>' +
                '        <div class="modal-actions" id="modalButtons"></div>' +
                '    </div>' +
                '</div>';
            const bar = document.getElementById('modalButtons');
            buttons.forEach(function (b) {
                const btn = document.createElement('button');
                btn.className = 'btn ' + (b.cls || 'plain');
                btn.textContent = b.label;
                btn.onclick = function () {
                    if (b.keepOpen !== true) closeModal();
                    if (b.action) b.action();
                };
                bar.appendChild(btn);
            });
        }

        function closeModal() {
            document.getElementById('modalHost').innerHTML = '';
        }

        function showTab(mode) {
            authMode = mode;
            document.getElementById('tabLogin').classList.toggle('active', mode === 'login');
            document.getElementById('tabRegister').classList.toggle('active', mode === 'register');
            document.getElementById('nameField').classList.toggle('hidden', mode !== 'register');
            document.getElementById('authBtn').textContent = mode === 'login' ? 'Log in' : 'Create account';
            document.getElementById('policyText').textContent = mode === 'login'
                ? 'Login policy: 5 wrong attempts lock the account for 5 minutes. Sessions last 12 hours.'
                : 'Password policy: at least 8 characters, with letters and numbers.';
            setAuthMsg('', '');
        }

        function setAuthMsg(text, kind) {
            const el = document.getElementById('authMsg');
            el.textContent = text;
            el.className = 'form-msg ' + (kind || '');
        }

        async function submitAuth() {
            const email = document.getElementById('email').value;
            const password = document.getElementById('password').value;
            const body = { action: authMode, email: email, password: password };
            if (authMode === 'register') body.name = document.getElementById('name').value;

            const response = await fetch('/v1/account/user', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            const data = await response.json();

            if (!response.ok) {
                setAuthMsg(data.error || 'Something went wrong.', 'error');
                return;
            }
            if (authMode === 'register') {
                showTab('login');
                setAuthMsg(data.message, 'ok');
                return;
            }
            token = data.token;
            user = data.user;
            quota = data.quota;
            storeSession();
            enterApp();
        }

        async function logout() {
            try { await api('/v1/account/user', { method: 'POST', json: { action: 'logout' } }); } catch (e) { }
            showAuth('You have been logged out.');
        }

        function showAuth(message) {
            clearSession();
            clearInterval(syncTimer);
            closeModal();
            document.getElementById('appView').classList.add('hidden');
            document.getElementById('authView').classList.remove('hidden');
            document.getElementById('password').value = '';
            showTab('login');
            if (message) setAuthMsg(message, '');
        }

        function enterApp() {
            document.getElementById('authView').classList.add('hidden');
            document.getElementById('appView').classList.remove('hidden');
            document.getElementById('userName').textContent = user.name;
            document.getElementById('userEmail').textContent = user.email;
            folderId = 'root';
            cursor = 0;
            loadFolder();
            syncNow(false);
            clearInterval(syncTimer);
            syncTimer = setInterval(function () { syncNow(false); }, 6000);
        }

        async function loadFolder(id) {
            if (id) folderId = id;
            const response = await api('/v1/user/' + user.id + '/folder/' + folderId);
            const data = await response.json();
            if (!response.ok) {
                toast(data.error || 'Could not load folder.');
                if (folderId !== 'root') loadFolder('root');
                return;
            }
            stats.cache = data.cache;
            renderCrumbs(data.path);
            renderRows(data.items);
            renderUsage(data.used, data.quota);
            renderStats();
        }

        function renderCrumbs(path) {
            const host = document.getElementById('crumbs');
            host.innerHTML = '';
            path.forEach(function (node, index) {
                const last = index === path.length - 1;
                const btn = document.createElement('button');
                btn.className = 'crumb' + (last ? ' current' : '');
                btn.textContent = node.name;
                if (!last) btn.onclick = function () { loadFolder(node.id); };
                host.appendChild(btn);
                if (!last) {
                    const sep = document.createElement('span');
                    sep.textContent = '/';
                    sep.className = 'small';
                    host.appendChild(sep);
                }
            });
        }

        function renderRows(items) {
            const body = document.getElementById('rows');
            body.innerHTML = '';
            document.getElementById('empty').classList.toggle('hidden', items.length > 0);

            items.forEach(function (item) {
                const tr = document.createElement('tr');
                const nameBtn = '<button class="name-link" data-open="' + esc(item.id) + '">' + esc(item.name) + '</button>';
                const sharedBadge = item.shared ? '<span class="badge">Shared</span>' : '';
                let actions = '';
                if (!item.is_folder) {
                    actions += '<button class="icon-btn" data-dl="' + esc(item.id) + '">Download</button>';
                    actions += '<button class="icon-btn" data-share="' + esc(item.id) + '">Share</button>';
                }
                actions += '<button class="icon-btn del" data-del="' + esc(item.id) + '">Delete</button>';

                tr.innerHTML =
                    '<td><div class="name-cell"><span>' + icon(item) + '</span>' + nameBtn + sharedBadge + '</div></td>' +
                    '<td class="col-hide">' + (item.is_folder ? '-' : fmtSize(item.size)) + '</td>' +
                    '<td class="col-hide">' + fmtDate(item.updated) + '</td>' +
                    '<td><div class="actions">' + actions + '</div></td>';

                tr.querySelector('[data-open]').onclick = function () {
                    if (item.is_folder) loadFolder(item.id);
                    else downloadFile(item.id, item.name);
                };
                const dl = tr.querySelector('[data-dl]');
                if (dl) dl.onclick = function () { downloadFile(item.id, item.name); };
                const sh = tr.querySelector('[data-share]');
                if (sh) sh.onclick = function () { shareFile(item); };
                tr.querySelector('[data-del]').onclick = function () { confirmDelete(item); };
                body.appendChild(tr);
            });
        }

        function renderUsage(used, total) {
            const pct = Math.min(100, (used / total) * 100);
            document.getElementById('meterFill').style.width = pct + '%';
            document.getElementById('meterText').textContent = fmtSize(used) + ' of ' + fmtSize(total);
        }

        function renderStats() {
            document.getElementById('stats').textContent =
                'Folder cache: ' + stats.cache + '   |   Last upload: ' + stats.upload;
        }

        function newFolder() {
            openModal('New folder', '<input type="text" id="folderName" placeholder="Folder name">', [
                { label: 'Cancel' },
                { label: 'Create', cls: '', keepOpen: true }
            ]);
            const buttons = document.querySelectorAll('#modalButtons .btn');
            buttons[1].onclick = async function () {
                const name = document.getElementById('folderName').value.trim();
                if (!name) return;
                closeModal();
                const response = await api('/v1/user/' + user.id + '/folder', {
                    method: 'POST',
                    json: { name: name, parent_id: folderId }
                });
                const data = await response.json();
                toast(response.ok ? 'Folder "' + data.name + '" created' : data.error);
                loadFolder();
                syncNow(false);
            };
            document.getElementById('folderName').focus();
        }

        function uploadFiles(fileList) {
            if (!fileList || !fileList.length) return;
            const form = new FormData();
            for (const file of fileList) {
                form.append('files', file);
                form.append('paths', file.webkitRelativePath || file.name);
            }

            const progress = document.getElementById('progress');
            const fill = document.getElementById('progressFill');
            progress.classList.remove('hidden');
            fill.style.width = '0%';

            const started = performance.now();
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/api/v1/user=' + user.id + '/upload/' + folderId);
            xhr.setRequestHeader('Authorization', 'Bearer ' + token);

            xhr.upload.onprogress = function (event) {
                if (event.lengthComputable) fill.style.width = (event.loaded / event.total * 100) + '%';
            };

            xhr.onload = function () {
                progress.classList.add('hidden');
                let data = {};
                try { data = JSON.parse(xhr.responseText); } catch (e) { }
                if (xhr.status === 401) { showAuth('Your session expired. Please log in again.'); return; }
                if (xhr.status >= 400) { toast(data.error || 'Upload failed.'); return; }

                const total = Math.round(performance.now() - started);
                stats.upload = data.saved.length + ' item(s), ' + total + ' ms total (server ' + data.server_ms + ' ms)';
                if (data.failed && data.failed.length) {
                    toast(data.failed[0].name + ': ' + data.failed[0].error);
                } else {
                    toast('Uploaded ' + data.saved.length + ' file(s)');
                }
                loadFolder();
                syncNow(false);
            };

            xhr.onerror = function () {
                progress.classList.add('hidden');
                toast('Network error during upload.');
            };

            xhr.send(form);
        }

        document.getElementById('filePick').addEventListener('change', function (e) {
            uploadFiles(e.target.files);
            e.target.value = '';
        });

        document.getElementById('folderPick').addEventListener('change', function (e) {
            uploadFiles(e.target.files);
            e.target.value = '';
        });

        const zone = document.getElementById('dropzone');
        ['dragenter', 'dragover'].forEach(function (name) {
            zone.addEventListener(name, function (e) {
                e.preventDefault();
                zone.classList.add('over');
            });
        });
        ['dragleave', 'drop'].forEach(function (name) {
            zone.addEventListener(name, function (e) {
                e.preventDefault();
                zone.classList.remove('over');
            });
        });
        zone.addEventListener('drop', function (e) {
            uploadFiles(e.dataTransfer.files);
        });

        async function downloadFile(id, name) {
            const response = await api('/v1/files/' + id + '/download');
            if (!response.ok) {
                toast('Download failed.');
                return;
            }
            const blob = await response.blob();
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = name;
            document.body.appendChild(link);
            link.click();
            link.remove();
        }

        async function shareFile(item) {
            const response = await api('/v1/files/' + item.id + '/share', {
                method: 'POST',
                json: { enable: true }
            });
            const data = await response.json();
            if (!response.ok) {
                toast(data.error);
                return;
            }
            loadFolder();
            openModal('Share "' + item.name + '"',
                '<p>Anyone with this link can download the file:</p>' +
                '<input type="text" id="shareUrl" readonly value="' + esc(data.url) + '">',
                [
                    {
                        label: 'Stop sharing', cls: 'danger', action: async function () {
                            await api('/v1/files/' + item.id + '/share', { method: 'POST', json: { enable: false } });
                            toast('Sharing turned off');
                            loadFolder();
                        }
                    },
                    {
                        label: 'Copy link', keepOpen: true, action: function () {
                            const input = document.getElementById('shareUrl');
                            input.select();
                            try { document.execCommand('copy'); toast('Link copied'); } catch (e) { toast('Press Ctrl+C to copy'); }
                        }
                    },
                    { label: 'Done' }
                ]);
        }

        function confirmDelete(item) {
            const extra = item.is_folder ? ' Everything inside it will be deleted too.' : '';
            openModal('Delete "' + item.name + '"?', '<p>This cannot be undone.' + esc(extra) + '</p>', [
                { label: 'Cancel' },
                {
                    label: 'Delete', cls: 'danger', action: async function () {
                        const response = await api('/v1/files/' + item.id + '/Delete', { method: 'PUT' });
                        const data = await response.json();
                        toast(response.ok ? 'Deleted ' + data.removed + ' item(s)' : data.error);
                        loadFolder();
                        syncNow(false);
                    }
                }
            ]);
        }

        async function syncNow(manual) {
            if (!token) return;
            const dot = document.getElementById('syncDot');
            const label = document.getElementById('syncLabel');
            const detail = document.getElementById('syncDetail');
            dot.className = 'dot busy';
            label.textContent = 'Syncing...';
            try {
                const response = await api('/v1/' + user.id + '/sync', {
                    method: 'POST',
                    json: { cursor: cursor }
                });
                const data = await response.json();
                const firstSync = cursor === 0;
                cursor = data.cursor;
                dot.className = 'dot';
                label.textContent = 'Synced';
                const count = data.changes.length;
                detail.textContent = (count ? count + ' change(s) pulled' : 'Up to date') +
                    ' at ' + new Date().toLocaleTimeString();
                if (count && !firstSync && !manual) loadFolder();
                if (manual) loadFolder();
            } catch (e) {
                dot.className = 'dot err';
                label.textContent = 'Offline';
            }
        }

        showTab('login');
        document.getElementById('password').addEventListener('keydown', function (e) {
            if (e.key === 'Enter') submitAuth();
        });
        if (loadSession()) {
            enterApp();
        }
    </script>
</body>
</html>
"""


@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


previous = globals().get("_nimbus_server")
if previous is not None:
    previous.shutdown()

_nimbus_server = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_nimbus_server.serve_forever, daemon=True).start()

print("NimbusDrive is running. Create an account, then log in.")
output.serve_kernel_port_as_iframe(PORT, height=900)