'''
Stock Trading Platform

1. Requirement Analysis
1A. Functional Requirements
- To do Login/Register
- View price of current stock
- Buy the stock
- Watchlist with real-time update
- Provide dashboard
1B. Non-Functional Requirements
- Scale:10M+ users
- CAP Theorem: Consistency >>>> Available
- Low Latency

2. Identifying Entities
- USER
- STOCK
- Order
- TRADE
- PORTFOLIO
- WATCHLIST

3. API Design
3A. LOGIN : POST /v1/auth/login {user ID} → Login
3B. VIEW PRICE OF STOCK : GET /v1/stocks/{stock ID} → List<Stock>: Pagination
3C. BUY THE STOCK : POST /v1/orders = {orderID} → Buy the stock
3D. PORTFOLIO : GET /v1/api/portfolio/{user ID} → Portfolio
3E. WATCHLIST : GET /v1/api/watchlist

4. High level design 
user -> api gateway -> sever ( users , price tracker , watchlist , order , portfolio and payment ) -> database (userDB & paymentDB )

5. low level design 
uploaded image
'''
import json
import re
import time
import uuid
import random
import secrets
import sqlite3
import threading
import traceback
from collections import deque, defaultdict
from contextlib import contextmanager
from datetime import datetime

from flask import Flask, request, jsonify, g
from werkzeug.exceptions import HTTPException
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.serving import make_server


PORT = 5057
SESSION_TTL = 30 * 60
MAX_FAILED_LOGINS = 5
LOCK_SECONDS = 5 * 60
TICK_SECONDS = 1.0
CIRCUIT_LIMIT = 0.20
MAX_QTY = 10000
MAX_PAYMENT = 1_000_000


if "_STOP" in globals():
    _STOP.set()
if "_SERVER" in globals():
    try:
        _SERVER.shutdown()
        _SERVER.server_close()
    except Exception:
        pass
_STOP = threading.Event()


def now():
    return time.time()


def ts_str(t):
    return datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")


def r2(x):
    return round(float(x), 2)


def usd(x):
    return "${:,.2f}".format(float(x))


class ApiError(Exception):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.message = message
        self.code = code


DB = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
DB.row_factory = sqlite3.Row
DB_LOCK = threading.RLock()

DB.executescript("""
CREATE TABLE users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    username        TEXT UNIQUE,
    email           TEXT UNIQUE,
    full_name       TEXT,
    phone           TEXT DEFAULT '',
    pw_hash         TEXT,
    balance         REAL DEFAULT 0,
    realized_pnl    REAL DEFAULT 0,
    kyc_status      TEXT DEFAULT 'PENDING',
    kyc_doc         TEXT DEFAULT '',
    bank_name       TEXT DEFAULT '',
    bank_account    TEXT DEFAULT '',
    prefs           TEXT DEFAULT '{}',
    failed_attempts INTEGER DEFAULT 0,
    locked_until    REAL DEFAULT 0,
    created         REAL
);
CREATE TABLE orders (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER,
    symbol      TEXT,
    side        TEXT,
    otype       TEXT,
    qty         INTEGER,
    limit_price REAL,
    status      TEXT,
    exec_price  REAL,
    reserved    REAL DEFAULT 0,
    created     REAL,
    updated     REAL
);
CREATE TABLE positions (
    user_id   INTEGER,
    symbol    TEXT,
    qty       INTEGER DEFAULT 0,
    locked    INTEGER DEFAULT 0,
    avg_price REAL DEFAULT 0,
    PRIMARY KEY (user_id, symbol)
);
CREATE TABLE trades (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id     INTEGER,
    user_id      INTEGER,
    symbol       TEXT,
    side         TEXT,
    qty          INTEGER,
    price        REAL,
    pnl          REAL DEFAULT 0,
    exchange_ref TEXT,
    ts           REAL
);
CREATE TABLE watchlists (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    name    TEXT,
    UNIQUE (user_id, name)
);
CREATE TABLE watchlist_items (
    watchlist_id INTEGER,
    symbol       TEXT,
    PRIMARY KEY (watchlist_id, symbol)
);
CREATE TABLE payments (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id   INTEGER,
    ptype     TEXT,
    amount    REAL,
    method    TEXT,
    status    TEXT,
    ref       TEXT,
    parent_id INTEGER,
    ts        REAL
);
CREATE TABLE notifications (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    channel TEXT,
    message TEXT,
    ts      REAL
);
""")


def dbq(sql, args=(), one=False):
    with DB_LOCK:
        rows = [dict(r) for r in DB.execute(sql, args).fetchall()]
    if one:
        return rows[0] if rows else None
    return rows


@contextmanager
def tx():
    with DB_LOCK:
        DB.execute("BEGIN IMMEDIATE")
        try:
            yield DB
            DB.execute("COMMIT")
        except BaseException:
            DB.execute("ROLLBACK")
            raise


class Kafka:
    TOPICS = ["order-events", "trade-events", "price-updates", "user-activity"]

    def __init__(self):
        self.logs = {t: deque(maxlen=120) for t in self.TOPICS}
        self.offsets = defaultdict(int)
        self.subscribers = defaultdict(list)
        self.lock = threading.Lock()

    def subscribe(self, topic, fn):
        self.subscribers[topic].append(fn)

    def publish(self, topic, event):
        with self.lock:
            record = {
                "topic": topic,
                "offset": self.offsets[topic],
                "ts": ts_str(now()),
                "event": event,
            }
            self.offsets[topic] += 1
            self.logs[topic].append(record)
        for fn in self.subscribers[topic]:
            try:
                fn(record)
            except Exception as exc:
                print("[kafka] consumer error:", exc)


KAFKA = Kafka()
ANALYTICS = defaultdict(float)


def notification_consumer(record):
    event = record["event"]
    if not event.get("notify") or not event.get("user_id"):
        return
    with tx():
        for channel in event.get("channels", ["EMAIL"]):
            DB.execute(
                "INSERT INTO notifications(user_id, channel, message, ts) VALUES (?,?,?,?)",
                (event["user_id"], channel, event["notify"], now()),
            )


def analytics_consumer(record):
    event = record["event"]
    if record["topic"] == "user-activity":
        ANALYTICS["activity:" + event.get("action", "?")] += 1
    elif record["topic"] == "trade-events":
        ANALYTICS["trades"] += 1
        ANALYTICS["traded_value"] += event.get("price", 0) * event.get("qty", 0)
    elif record["topic"] == "order-events":
        ANALYTICS["orders"] += 1


for _topic in Kafka.TOPICS:
    KAFKA.subscribe(_topic, notification_consumer)
    KAFKA.subscribe(_topic, analytics_consumer)


def activity(user_id, action, **extra):
    event = {"user_id": user_id, "action": action}
    event.update(extra)
    KAFKA.publish("user-activity", event)


SEED_STOCKS = [
    ("AAPL", "Apple Inc.", "Technology", 189.50),
    ("MSFT", "Microsoft Corp.", "Technology", 412.30),
    ("GOOGL", "Alphabet Inc.", "Technology", 172.80),
    ("NVDA", "NVIDIA Corp.", "Technology", 905.10),
    ("META", "Meta Platforms", "Technology", 492.60),
    ("TSLA", "Tesla Inc.", "Consumer", 178.40),
    ("AMZN", "Amazon.com Inc.", "Consumer", 181.20),
    ("NFLX", "Netflix Inc.", "Consumer", 612.75),
    ("KO", "Coca-Cola Co.", "Consumer", 60.70),
    ("NKE", "Nike Inc.", "Consumer", 94.30),
    ("JPM", "JPMorgan Chase", "Finance", 196.40),
    ("BAC", "Bank of America", "Finance", 36.85),
    ("GS", "Goldman Sachs", "Finance", 452.10),
    ("V", "Visa Inc.", "Finance", 279.30),
    ("XOM", "Exxon Mobil", "Energy", 114.20),
    ("CVX", "Chevron Corp.", "Energy", 158.90),
    ("BP", "BP plc", "Energy", 36.10),
    ("PFE", "Pfizer Inc.", "Healthcare", 27.90),
    ("JNJ", "Johnson & Johnson", "Healthcare", 156.40),
    ("UNH", "UnitedHealth Group", "Healthcare", 492.80),
]
SECTORS = ["Technology", "Consumer", "Finance", "Energy", "Healthcare"]
INDEX_DEFS = {
    "MARKET-20": None,
    "TECH-IDX": "Technology",
    "CONSUMER-IDX": "Consumer",
    "FIN-IDX": "Finance",
    "ENERGY-IDX": "Energy",
    "HEALTH-IDX": "Healthcare",
}

MKT_LOCK = threading.RLock()
MARKET = {}


def seed_market():
    for symbol, name, sector, price in SEED_STOCKS:
        walk = [price]
        for _ in range(89):
            walk.append(walk[-1] * (1 + random.gauss(0, 0.002)))
        walk.reverse()
        t0 = now() - 89 * TICK_SECONDS
        history = deque(
            [[t0 + i * TICK_SECONDS, round(p, 2)] for i, p in enumerate(walk)],
            maxlen=240,
        )
        last = history[-1][1]
        prev_close = round(last * random.uniform(0.975, 1.025), 2)
        MARKET[symbol] = {
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "price": last,
            "base": price,
            "prev_close": prev_close,
            "open": history[0][1],
            "high": max(p[1] for p in history),
            "low": min(p[1] for p in history),
            "volume": random.randint(1_000_000, 6_000_000),
            "hist": history,
        }


seed_market()


def get_quote(symbol):
    with MKT_LOCK:
        m = MARKET[symbol]
        change = m["price"] - m["prev_close"]
        return {
            "symbol": m["symbol"],
            "name": m["name"],
            "sector": m["sector"],
            "price": r2(m["price"]),
            "prev_close": r2(m["prev_close"]),
            "open": r2(m["open"]),
            "high": r2(m["high"]),
            "low": r2(m["low"]),
            "volume": m["volume"],
            "change": r2(change),
            "change_pct": round(change / m["prev_close"] * 100, 2),
        }


def tick_market():
    with MKT_LOCK:
        for m in MARKET.values():
            new_price = m["price"] * (1 + random.gauss(0, 0.0012))
            lo = m["prev_close"] * (1 - CIRCUIT_LIMIT)
            hi = m["prev_close"] * (1 + CIRCUIT_LIMIT)
            new_price = round(min(max(new_price, lo), hi), 2)
            m["price"] = new_price
            m["high"] = max(m["high"], new_price)
            m["low"] = min(m["low"], new_price)
            m["volume"] += random.randint(100, 5000)
            m["hist"].append([now(), new_price])


def market_indices():
    result = []
    with MKT_LOCK:
        for name, sector in INDEX_DEFS.items():
            members = [m for m in MARKET.values() if sector is None or m["sector"] == sector]
            value = 1000 * sum(m["price"] / m["base"] for m in members) / len(members)
            prev = 1000 * sum(m["prev_close"] / m["base"] for m in members) / len(members)
            result.append({
                "name": name,
                "value": round(value, 2),
                "change_pct": round((value - prev) / prev * 100, 2),
            })
    return result


class ExchangeGateway:
    def market_data_feed(self):
        with MKT_LOCK:
            return {s: m["price"] for s, m in MARKET.items()}

    def execute(self, symbol, side, qty, price=None):
        if price is None:
            with MKT_LOCK:
                price = MARKET[symbol]["price"]
        return {
            "status": "FILLED",
            "price": r2(price),
            "exchange_ref": "EXC-" + uuid.uuid4().hex[:10].upper(),
        }


EXCHANGE = ExchangeGateway()


class PaymentGateway:
    def charge(self, amount, method):
        return {"status": "SUCCESS", "ref": "PG-" + uuid.uuid4().hex[:10].upper()}

    def payout(self, amount, bank_account):
        return {"status": "SUCCESS", "ref": "BANK-" + uuid.uuid4().hex[:10].upper()}

    def refund(self, ref):
        return {"status": "SUCCESS", "ref": "RF-" + uuid.uuid4().hex[:10].upper()}


class KycProvider:
    DOCS = ("NATIONAL_ID", "PASSPORT", "DRIVING_LICENSE")

    def verify(self, doc_type, number):
        if doc_type not in self.DOCS:
            return {"verified": False, "reason": "Unsupported document type"}
        if not re.fullmatch(r"[A-Za-z0-9\-]{6,20}", number or ""):
            return {"verified": False, "reason": "Document number must be 6-20 letters, digits or dashes"}
        return {"verified": True, "reason": "OK"}


PAYMENTS = PaymentGateway()
KYC = KycProvider()


USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DUMMY_HASH = generate_password_hash("timing-attack-protection")
SESSIONS = {}
SESSION_LOCK = threading.Lock()


def password_problems(pw):
    problems = []
    if len(pw) < 8:
        problems.append("at least 8 characters")
    if not re.search(r"[a-z]", pw):
        problems.append("a lowercase letter")
    if not re.search(r"[A-Z]", pw):
        problems.append("an uppercase letter")
    if not re.search(r"\d", pw):
        problems.append("a digit")
    if len(pw) > 64:
        problems.append("at most 64 characters")
    return problems


def get_user(uid):
    user = dbq("SELECT * FROM users WHERE id=?", (uid,), one=True)
    if not user:
        raise ApiError("User not found", 404)
    return user


def user_public(u):
    account = u["bank_account"] or ""
    doc = u["kyc_doc"] or ""
    return {
        "id": u["id"],
        "username": u["username"],
        "email": u["email"],
        "full_name": u["full_name"],
        "phone": u["phone"],
        "balance": r2(u["balance"]),
        "kyc_status": u["kyc_status"],
        "kyc_doc": ("*" * max(len(doc) - 3, 0) + doc[-3:]) if doc else "",
        "bank_name": u["bank_name"],
        "bank_account": ("*" * max(len(account) - 4, 0) + account[-4:]) if account else "",
        "prefs": json.loads(u["prefs"] or "{}"),
        "created": ts_str(u["created"]),
    }


def body():
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def int_arg(name, default):
    try:
        return int(request.args.get(name, default))
    except (TypeError, ValueError):
        return default


def require_kyc(u):
    if u["kyc_status"] != "VERIFIED":
        raise ApiError("Complete KYC verification (Profile page) before trading or moving money", 403)


def parse_amount(value):
    try:
        amount = round(float(value), 2)
    except (TypeError, ValueError):
        raise ApiError("Enter a valid amount")
    if amount <= 0:
        raise ApiError("Amount must be greater than zero")
    if amount > MAX_PAYMENT:
        raise ApiError("Maximum per transaction is " + usd(MAX_PAYMENT))
    return amount


def order_view(o):
    view = dict(o)
    view["created"] = ts_str(o["created"])
    view["updated"] = ts_str(o["updated"])
    view["type"] = view.pop("otype")
    return view


def _reserve(o, price_hint):
    uid = o["user_id"]
    if o["side"] == "BUY":
        unit = o["limit_price"] if o["otype"] == "LIMIT" else price_hint
        amount = round(unit * o["qty"], 2)
        balance = dbq("SELECT balance FROM users WHERE id=?", (uid,), one=True)["balance"]
        if balance + 1e-9 < amount:
            raise ApiError("Insufficient funds: need %s, available %s" % (usd(amount), usd(balance)))
        DB.execute("UPDATE users SET balance = balance - ? WHERE id=?", (amount, uid))
        o["reserved"] = amount
    else:
        pos = dbq("SELECT * FROM positions WHERE user_id=? AND symbol=?", (uid, o["symbol"]), one=True)
        available = (pos["qty"] - pos["locked"]) if pos else 0
        if available < o["qty"]:
            raise ApiError("Insufficient holdings: you can sell at most %d %s" % (available, o["symbol"]))
        DB.execute(
            "UPDATE positions SET locked = locked + ? WHERE user_id=? AND symbol=?",
            (o["qty"], uid, o["symbol"]),
        )
        o["reserved"] = 0


def _release(o):
    uid = o["user_id"]
    if o["side"] == "BUY":
        DB.execute("UPDATE users SET balance = balance + ? WHERE id=?", (o["reserved"], uid))
    else:
        DB.execute(
            "UPDATE positions SET locked = locked - ? WHERE user_id=? AND symbol=?",
            (o["qty"], uid, o["symbol"]),
        )


def _fill(o, price, exchange_ref):
    uid, sym, qty = o["user_id"], o["symbol"], o["qty"]
    cost = round(price * qty, 2)
    pnl = 0.0
    pos = dbq("SELECT * FROM positions WHERE user_id=? AND symbol=?", (uid, sym), one=True)

    if o["side"] == "BUY":
        refund = round(o["reserved"] - cost, 2)
        DB.execute("UPDATE users SET balance = balance + ? WHERE id=?", (refund, uid))
        if pos:
            new_qty = pos["qty"] + qty
            new_avg = (pos["avg_price"] * pos["qty"] + cost) / new_qty
            DB.execute(
                "UPDATE positions SET qty=?, avg_price=? WHERE user_id=? AND symbol=?",
                (new_qty, new_avg, uid, sym),
            )
        else:
            DB.execute(
                "INSERT INTO positions(user_id, symbol, qty, locked, avg_price) VALUES (?,?,?,?,?)",
                (uid, sym, qty, 0, price),
            )
    else:
        pnl = round((price - pos["avg_price"]) * qty, 2)
        remaining = pos["qty"] - qty
        if remaining <= 0:
            DB.execute("DELETE FROM positions WHERE user_id=? AND symbol=?", (uid, sym))
        else:
            DB.execute(
                "UPDATE positions SET qty=?, locked=locked-? WHERE user_id=? AND symbol=?",
                (remaining, qty, uid, sym),
            )
        DB.execute(
            "UPDATE users SET balance = balance + ?, realized_pnl = realized_pnl + ? WHERE id=?",
            (cost, pnl, uid),
        )

    t = now()
    cur = DB.execute(
        "INSERT INTO trades(order_id, user_id, symbol, side, qty, price, pnl, exchange_ref, ts) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (o["id"], uid, sym, o["side"], qty, price, pnl, exchange_ref, t),
    )
    DB.execute(
        "UPDATE orders SET status='EXECUTED', exec_price=?, reserved=0, updated=? WHERE id=?",
        (price, t, o["id"]),
    )
    return {
        "type": "TRADE_EXECUTED",
        "user_id": uid,
        "trade_id": cur.lastrowid,
        "order_id": o["id"],
        "symbol": sym,
        "side": o["side"],
        "qty": qty,
        "price": price,
        "pnl": pnl,
        "exchange_ref": exchange_ref,
        "notify": "%s %d %s executed at %s" % (o["side"], qty, sym, usd(price)),
        "channels": ["EMAIL", "SMS"],
    }


def place_order(uid, d):
    symbol = str(d.get("symbol", "")).strip().upper()
    side = str(d.get("side", "")).strip().upper()
    otype = str(d.get("type", "MARKET")).strip().upper()


    if symbol not in MARKET:
        raise ApiError("Unknown stock symbol", 404)
    if side not in ("BUY", "SELL"):
        raise ApiError("side must be BUY or SELL")
    if otype not in ("MARKET", "LIMIT"):
        raise ApiError("type must be MARKET or LIMIT")
    try:
        qty = int(d.get("qty"))
    except (TypeError, ValueError):
        raise ApiError("Quantity must be a whole number")
    if qty <= 0 or qty > MAX_QTY:
        raise ApiError("Quantity must be between 1 and %d" % MAX_QTY)

    price = get_quote(symbol)["price"]
    limit_price = None
    if otype == "LIMIT":
        try:
            limit_price = round(float(d.get("limit_price")), 2)
        except (TypeError, ValueError):
            raise ApiError("Enter a valid limit price")
        if limit_price <= 0:
            raise ApiError("Limit price must be greater than zero")
        if abs(limit_price - price) / price > CIRCUIT_LIMIT:
            raise ApiError("Limit price must be within %d%% of the last price (%s)" % (CIRCUIT_LIMIT * 100, usd(price)))

    placed_event = None
    trade_event = None
    with tx():
        user = get_user(uid)
        require_kyc(user)
        t = now()
        cur = DB.execute(
            "INSERT INTO orders(user_id, symbol, side, otype, qty, limit_price, status, created, updated) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (uid, symbol, side, otype, qty, limit_price, "OPEN", t, t),
        )
        order = {
            "id": cur.lastrowid, "user_id": uid, "symbol": symbol, "side": side,
            "otype": otype, "qty": qty, "limit_price": limit_price, "reserved": 0,
        }
        _reserve(order, price)
        DB.execute("UPDATE orders SET reserved=? WHERE id=?", (order["reserved"], order["id"]))
        placed_event = {
            "type": "ORDER_PLACED", "user_id": uid, "order_id": order["id"], "symbol": symbol,
            "side": side, "order_type": otype, "qty": qty, "limit_price": limit_price,
        }
        if otype == "MARKET":
            fill = EXCHANGE.execute(symbol, side, qty)
            trade_event = _fill(order, fill["price"], fill["exchange_ref"])
    KAFKA.publish("order-events", placed_event)
    if trade_event:
        KAFKA.publish("trade-events", trade_event)
    return order_view(dbq("SELECT * FROM orders WHERE id=?", (placed_event["order_id"],), one=True))


def cancel_order(uid, oid):
    with tx():
        o = dbq("SELECT * FROM orders WHERE id=? AND user_id=?", (oid, uid), one=True)
        if not o:
            raise ApiError("Order not found", 404)
        if o["status"] != "OPEN":
            raise ApiError("Only open orders can be cancelled")
        _release(o)
        DB.execute("UPDATE orders SET status='CANCELLED', reserved=0, updated=? WHERE id=?", (now(), oid))
    KAFKA.publish("order-events", {
        "type": "ORDER_CANCELLED", "user_id": uid, "order_id": oid, "symbol": o["symbol"],
        "notify": "Order #%d (%s %s) cancelled" % (oid, o["side"], o["symbol"]),
    })
    return order_view(dbq("SELECT * FROM orders WHERE id=?", (oid,), one=True))


def modify_order(uid, oid, d):
    with tx():
        o = dbq("SELECT * FROM orders WHERE id=? AND user_id=?", (oid, uid), one=True)
        if not o:
            raise ApiError("Order not found", 404)
        if o["status"] != "OPEN" or o["otype"] != "LIMIT":
            raise ApiError("Only open LIMIT orders can be modified")
        try:
            qty = int(d.get("qty", o["qty"]))
            limit_price = round(float(d.get("limit_price", o["limit_price"])), 2)
        except (TypeError, ValueError):
            raise ApiError("Invalid quantity or limit price")
        if qty <= 0 or qty > MAX_QTY or limit_price <= 0:
            raise ApiError("Quantity or limit price out of range")
        price = get_quote(o["symbol"])["price"]
        if abs(limit_price - price) / price > CIRCUIT_LIMIT:
            raise ApiError("Limit price must be within %d%% of the last price (%s)" % (CIRCUIT_LIMIT * 100, usd(price)))
        _release(o)
        new = dict(o, qty=qty, limit_price=limit_price, reserved=0)
        _reserve(new, price)
        DB.execute(
            "UPDATE orders SET qty=?, limit_price=?, reserved=?, updated=? WHERE id=?",
            (qty, limit_price, new["reserved"], now(), oid),
        )
    KAFKA.publish("order-events", {
        "type": "ORDER_MODIFIED", "user_id": uid, "order_id": oid, "symbol": o["symbol"],
        "qty": qty, "limit_price": limit_price,
    })
    return order_view(dbq("SELECT * FROM orders WHERE id=?", (oid,), one=True))


def match_limit_orders():
    trade_events = []
    with tx():
        for o in dbq("SELECT * FROM orders WHERE status='OPEN' AND otype='LIMIT' ORDER BY id"):
            px = MARKET[o["symbol"]]["price"]
            hit = (o["side"] == "BUY" and px <= o["limit_price"]) or \
                  (o["side"] == "SELL" and px >= o["limit_price"])
            if hit:
                fill = EXCHANGE.execute(o["symbol"], o["side"], o["qty"], price=px)
                trade_events.append(_fill(o, fill["price"], fill["exchange_ref"]))
    return trade_events


def engine_loop(stop_event):
    while not stop_event.is_set():
        try:
            tick_market()
            movers = sorted(MARKET.values(), key=lambda m: abs(m["price"] / m["prev_close"] - 1), reverse=True)[:3]
            KAFKA.publish("price-updates", {
                "type": "PRICE_TICK",
                "symbols": len(MARKET),
                "top_movers": {m["symbol"]: m["price"] for m in movers},
            })
            for event in match_limit_orders():
                KAFKA.publish("trade-events", event)
        except Exception:
            traceback.print_exc()
        stop_event.wait(TICK_SECONDS)


def build_portfolio(uid):
    user = get_user(uid)
    rows = dbq("SELECT * FROM positions WHERE user_id=? ORDER BY symbol", (uid,))
    blocked = dbq(
        "SELECT COALESCE(SUM(reserved),0) AS s FROM orders WHERE user_id=? AND status='OPEN' AND side='BUY'",
        (uid,), one=True,
    )["s"]
    holdings, sectors = [], defaultdict(float)
    invested = value = 0.0
    for p in rows:
        qt = get_quote(p["symbol"])
        inv = p["avg_price"] * p["qty"]
        val = qt["price"] * p["qty"]
        invested += inv
        value += val
        sectors[qt["sector"]] += val
        holdings.append({
            "symbol": p["symbol"], "name": qt["name"], "sector": qt["sector"],
            "qty": p["qty"], "locked": p["locked"], "available": p["qty"] - p["locked"],
            "avg_price": r2(p["avg_price"]), "ltp": qt["price"], "change_pct": qt["change_pct"],
            "invested": r2(inv), "value": r2(val), "pnl": r2(val - inv),
            "pnl_pct": round((val - inv) / inv * 100, 2) if inv else 0,
        })
    unrealized = value - invested
    return {
        "user_id": uid,
        "cash": r2(user["balance"]),
        "blocked": r2(blocked),
        "invested": r2(invested),
        "current_value": r2(value),
        "unrealized_pnl": r2(unrealized),
        "unrealized_pct": round(unrealized / invested * 100, 2) if invested else 0,
        "realized_pnl": r2(user["realized_pnl"]),
        "total_pnl": r2(unrealized + user["realized_pnl"]),
        "net_worth": r2(user["balance"] + blocked + value),
        "holdings": holdings,
        "allocation": [
            {"sector": s, "value": r2(v), "pct": round(v / value * 100, 1) if value else 0}
            for s, v in sorted(sectors.items(), key=lambda kv: -kv[1])
        ],
    }


def trade_view(t):
    view = dict(t)
    view["ts"] = ts_str(t["ts"])
    return view


app = Flask(__name__)
try:
    app.json.sort_keys = False
except Exception:
    pass

RATE = defaultdict(deque)
RATE_LOCK = threading.Lock()
GATEWAY_LOG = deque(maxlen=60)
SERVICE_MAP = [
    ("/v1/auth", "User Service"),
    ("/v1/users", "User Service"),
    ("/v1/stocks", "Price Tracker Service"),
    ("/v1/indices", "Price Tracker Service"),
    ("/v1/api/watchlist", "Watchlist Service"),
    ("/v1/watchlist", "Watchlist Service"),
    ("/v1/orders", "Order Service"),
    ("/v1/api/portfolio", "Portfolio Service"),
    ("/v1/portfolio", "Portfolio Service"),
    ("/v1/trades", "Portfolio Service"),
    ("/v1/dashboard", "Portfolio Service"),
    ("/v1/payments", "Payment Service"),
    ("/v1/system", "Platform"),
]


def rate_ok(key, limit, window=60):
    t = now()
    with RATE_LOCK:
        bucket = RATE[key]
        while bucket and bucket[0] < t - window:
            bucket.popleft()
        if len(bucket) >= limit:
            return False
        bucket.append(t)
        return True


@app.before_request
def api_gateway():
    g.t0 = time.time()
    path = request.path
    if path in ("/", "/health"):
        return None
    is_auth = path in ("/v1/auth/login", "/v1/auth/register")
    token = request.headers.get("Authorization", "").replace("Bearer ", "").strip()


    if is_auth or not token:
        key, limit = "ip:" + str(request.remote_addr), 20
    else:
        key, limit = "tok:" + token[-24:], 600
    if not rate_ok(key, limit):
        return jsonify(error="Rate limit exceeded. Slow down and retry shortly."), 429
    if is_auth:
        return None


    with SESSION_LOCK:
        session = SESSIONS.get(token)
        if not session or session["expires"] < now():
            SESSIONS.pop(token, None)
            return jsonify(error="Authentication required. Please login."), 401
        session["expires"] = now() + SESSION_TTL
    g.user_id = session["user_id"]
    g.token = token
    return None


@app.after_request
def gateway_log(resp):
    path = request.path
    if path != "/" and not path.startswith("/v1/stocks/live") and not path.startswith("/v1/system"):
        service = next((svc for prefix, svc in SERVICE_MAP if path.startswith(prefix)), "Gateway")
        GATEWAY_LOG.append({
            "ts": ts_str(now()),
            "method": request.method,
            "path": path,
            "service": service,
            "status": resp.status_code,
            "ms": round((time.time() - getattr(g, "t0", time.time())) * 1000, 1),
        })
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.errorhandler(Exception)
def handle_error(exc):
    if isinstance(exc, ApiError):
        return jsonify(error=exc.message), exc.code
    if isinstance(exc, HTTPException):
        return jsonify(error=exc.description), exc.code
    traceback.print_exc()
    return jsonify(error="Internal server error"), 500


@app.get("/")
def index():
    return INDEX_HTML


@app.get("/health")
def health():
    return jsonify(status="ok", users=dbq("SELECT COUNT(*) AS c FROM users", one=True)["c"])


@app.post("/v1/auth/register")
def register():
    d = body()
    full_name = str(d.get("full_name", "")).strip()
    username = str(d.get("username", "")).strip()
    email = str(d.get("email", "")).strip().lower()
    password = str(d.get("password", ""))

    if len(full_name) < 2 or len(full_name) > 60:
        raise ApiError("Enter your full name (2-60 characters)")
    if not USERNAME_RE.match(username):
        raise ApiError("User ID must be 3-20 characters: letters, numbers or underscore")
    if not EMAIL_RE.match(email):
        raise ApiError("Enter a valid email address")
    problems = password_problems(password)
    if problems:
        raise ApiError("Password needs " + ", ".join(problems))
    if username.lower() in password.lower():
        raise ApiError("Password must not contain your User ID")

    with tx():
        taken = dbq("SELECT 1 FROM users WHERE lower(username)=? OR email=?", (username.lower(), email), one=True)
        if taken:
            raise ApiError("User ID or email is already registered", 409)
        cur = DB.execute(
            "INSERT INTO users(username, email, full_name, pw_hash, created) VALUES (?,?,?,?,?)",
            (username, email, full_name, generate_password_hash(password), now()),
        )
        uid = cur.lastrowid
        wl = DB.execute("INSERT INTO watchlists(user_id, name) VALUES (?,?)", (uid, "My Watchlist")).lastrowid
        for sym in ("AAPL", "MSFT", "NVDA", "TSLA"):
            DB.execute("INSERT INTO watchlist_items(watchlist_id, symbol) VALUES (?,?)", (wl, sym))
    activity(uid, "REGISTER", notify="Welcome to TradeNova, %s! Complete KYC to start trading." % full_name)
    return jsonify(message="Registration successful. Please login.", user_id=username), 201


@app.post("/v1/auth/login")
def login():
    d = body()
    ident = str(d.get("user_id") or d.get("username") or "").strip().lower()
    password = str(d.get("password", ""))
    if not ident or not password:
        raise ApiError("User ID and password are required")

    user = dbq("SELECT * FROM users WHERE lower(username)=? OR email=?", (ident, ident), one=True)
    if not user:
        check_password_hash(DUMMY_HASH, password)
        raise ApiError("Invalid User ID or password", 401)

    if user["locked_until"] > now():
        minutes = int((user["locked_until"] - now()) // 60) + 1
        raise ApiError("Account locked after too many failed attempts. Try again in %d min." % minutes, 423)

    if not check_password_hash(user["pw_hash"], password):
        attempts = user["failed_attempts"] + 1
        locked_until = 0
        with tx():
            if attempts >= MAX_FAILED_LOGINS:
                locked_until, attempts = now() + LOCK_SECONDS, 0
            DB.execute("UPDATE users SET failed_attempts=?, locked_until=? WHERE id=?",
                       (attempts, locked_until, user["id"]))
        if locked_until:
            activity(user["id"], "ACCOUNT_LOCKED", notify="Your account was locked for 5 minutes after repeated failed logins.",
                     channels=["EMAIL", "SMS"])
            raise ApiError("Too many failed attempts. Account locked for %d minutes." % (LOCK_SECONDS // 60), 423)
        activity(user["id"], "LOGIN_FAILED")
        raise ApiError("Invalid User ID or password. %d attempt(s) left." % (MAX_FAILED_LOGINS - attempts), 401)

    with tx():
        DB.execute("UPDATE users SET failed_attempts=0, locked_until=0 WHERE id=?", (user["id"],))
    token = secrets.token_urlsafe(32)
    with SESSION_LOCK:

        for old in [t for t, s in SESSIONS.items() if s["user_id"] == user["id"]]:
            SESSIONS.pop(old, None)
        SESSIONS[token] = {"user_id": user["id"], "expires": now() + SESSION_TTL}
    activity(user["id"], "LOGIN")
    return jsonify(token=token, expires_in=SESSION_TTL, user=user_public(get_user(user["id"])))


@app.post("/v1/auth/logout")
def logout():
    with SESSION_LOCK:
        SESSIONS.pop(g.token, None)
    activity(g.user_id, "LOGOUT")
    return jsonify(message="Logged out")


@app.get("/v1/users/me")
def me():
    return jsonify(user=user_public(get_user(g.user_id)))


@app.put("/v1/users/me")
def update_profile():
    d = body()
    full_name = str(d.get("full_name", "")).strip()
    phone = str(d.get("phone", "")).strip()
    if len(full_name) < 2 or len(full_name) > 60:
        raise ApiError("Enter your full name (2-60 characters)")
    if phone and not re.fullmatch(r"\+?[0-9 \-]{7,15}", phone):
        raise ApiError("Enter a valid phone number")
    with tx():
        DB.execute("UPDATE users SET full_name=?, phone=? WHERE id=?", (full_name, phone, g.user_id))
    return jsonify(user=user_public(get_user(g.user_id)))


@app.post("/v1/users/kyc")
def kyc_verify():
    d = body()
    doc_type = str(d.get("doc_type", "")).upper()
    number = str(d.get("doc_number", "")).strip()
    result = KYC.verify(doc_type, number)
    if not result["verified"]:
        raise ApiError(result["reason"])
    with tx():
        DB.execute("UPDATE users SET kyc_status='VERIFIED', kyc_doc=? WHERE id=?", (number, g.user_id))
    activity(g.user_id, "KYC_VERIFIED", notify="Your KYC verification is complete. You can now trade.")
    return jsonify(user=user_public(get_user(g.user_id)))


@app.put("/v1/users/bank")
def bank_details():
    d = body()
    bank = str(d.get("bank_name", "")).strip()
    account = re.sub(r"\s", "", str(d.get("account_number", "")))
    if len(bank) < 2:
        raise ApiError("Enter your bank name")
    if not re.fullmatch(r"\d{8,18}", account):
        raise ApiError("Account number must be 8-18 digits")
    with tx():
        DB.execute("UPDATE users SET bank_name=?, bank_account=? WHERE id=?", (bank, account, g.user_id))
    return jsonify(user=user_public(get_user(g.user_id)))


@app.put("/v1/users/preferences")
def preferences():
    d = body()
    theme = d.get("theme", "dark")
    refresh = d.get("refresh", 2)
    if theme not in ("dark", "light"):
        raise ApiError("Invalid theme")
    if refresh not in (1, 2, 5, 10):
        raise ApiError("Invalid refresh interval")
    with tx():
        DB.execute("UPDATE users SET prefs=? WHERE id=?", (json.dumps({"theme": theme, "refresh": refresh}), g.user_id))
    return jsonify(user=user_public(get_user(g.user_id)))


@app.get("/v1/stocks")
def list_stocks():
    page = max(1, int_arg("page", 1))
    size = min(100, max(1, int_arg("page_size", 10)))
    text = request.args.get("q", "").strip().lower()
    sector = request.args.get("sector", "").strip()
    items = [get_quote(s) for s in sorted(MARKET)]
    if text:
        items = [i for i in items if text in i["symbol"].lower() or text in i["name"].lower()]
    if sector:
        items = [i for i in items if i["sector"] == sector]
    total = len(items)
    start = (page - 1) * size
    return jsonify(items=items[start:start + size], page=page, page_size=size, total=total,
                   pages=max(1, -(-total // size)), sectors=SECTORS)


@app.get("/v1/stocks/live")
def live_prices():
    prices = {}
    for s in MARKET:
        q = get_quote(s)
        prices[s] = {"price": q["price"], "change": q["change"], "change_pct": q["change_pct"]}
    return jsonify(prices=prices, ts=ts_str(now()))


@app.get("/v1/stocks/<symbol>")
def stock_detail(symbol):
    symbol = symbol.upper()
    if symbol not in MARKET:
        raise ApiError("Unknown stock symbol", 404)
    return jsonify(get_quote(symbol))


@app.get("/v1/stocks/<symbol>/history")
def stock_history(symbol):
    symbol = symbol.upper()
    if symbol not in MARKET:
        raise ApiError("Unknown stock symbol", 404)
    points = min(240, max(10, int_arg("points", 90)))
    with MKT_LOCK:
        data = list(MARKET[symbol]["hist"])[-points:]
    return jsonify(symbol=symbol, points=data)


@app.get("/v1/indices")
def indices():
    return jsonify(indices=market_indices())


def owned_watchlist(wid):
    w = dbq("SELECT * FROM watchlists WHERE id=? AND user_id=?", (wid, g.user_id), one=True)
    if not w:
        raise ApiError("Watchlist not found", 404)
    return w


@app.get("/v1/watchlists")
@app.get("/v1/api/watchlist")
def get_watchlists():
    lists = dbq("SELECT * FROM watchlists WHERE user_id=? ORDER BY id", (g.user_id,))
    for w in lists:
        items = dbq("SELECT symbol FROM watchlist_items WHERE watchlist_id=? ORDER BY rowid", (w["id"],))
        w["stocks"] = [get_quote(i["symbol"]) for i in items]
    return jsonify(watchlists=lists)


@app.post("/v1/watchlists")
def create_watchlist():
    name = str(body().get("name", "")).strip()
    if not 1 <= len(name) <= 30:
        raise ApiError("Watchlist name must be 1-30 characters")
    with tx():
        if dbq("SELECT COUNT(*) AS c FROM watchlists WHERE user_id=?", (g.user_id,), one=True)["c"] >= 10:
            raise ApiError("You can have at most 10 watchlists")
        if dbq("SELECT 1 FROM watchlists WHERE user_id=? AND lower(name)=?", (g.user_id, name.lower()), one=True):
            raise ApiError("A watchlist with this name already exists", 409)
        wid = DB.execute("INSERT INTO watchlists(user_id, name) VALUES (?,?)", (g.user_id, name)).lastrowid
    return jsonify(id=wid, name=name), 201


@app.put("/v1/watchlists/<int:wid>")
def rename_watchlist(wid):
    owned_watchlist(wid)
    name = str(body().get("name", "")).strip()
    if not 1 <= len(name) <= 30:
        raise ApiError("Watchlist name must be 1-30 characters")
    with tx():
        if dbq("SELECT 1 FROM watchlists WHERE user_id=? AND lower(name)=? AND id<>?", (g.user_id, name.lower(), wid), one=True):
            raise ApiError("A watchlist with this name already exists", 409)
        DB.execute("UPDATE watchlists SET name=? WHERE id=?", (name, wid))
    return jsonify(id=wid, name=name)


@app.delete("/v1/watchlists/<int:wid>")
def delete_watchlist(wid):
    owned_watchlist(wid)
    with tx():
        if dbq("SELECT COUNT(*) AS c FROM watchlists WHERE user_id=?", (g.user_id,), one=True)["c"] <= 1:
            raise ApiError("You must keep at least one watchlist")
        DB.execute("DELETE FROM watchlist_items WHERE watchlist_id=?", (wid,))
        DB.execute("DELETE FROM watchlists WHERE id=?", (wid,))
    return jsonify(message="Watchlist deleted")


@app.post("/v1/watchlists/<int:wid>/stocks")
def watchlist_add(wid):
    owned_watchlist(wid)
    symbol = str(body().get("symbol", "")).upper()
    if symbol not in MARKET:
        raise ApiError("Unknown stock symbol", 404)
    with tx():
        count = dbq("SELECT COUNT(*) AS c FROM watchlist_items WHERE watchlist_id=?", (wid,), one=True)["c"]
        if count >= 30:
            raise ApiError("A watchlist can hold at most 30 stocks")
        DB.execute("INSERT OR IGNORE INTO watchlist_items(watchlist_id, symbol) VALUES (?,?)", (wid, symbol))
    return jsonify(message="%s added" % symbol), 201


@app.delete("/v1/watchlists/<int:wid>/stocks/<symbol>")
def watchlist_remove(wid, symbol):
    owned_watchlist(wid)
    with tx():
        DB.execute("DELETE FROM watchlist_items WHERE watchlist_id=? AND symbol=?", (wid, symbol.upper()))
    return jsonify(message="%s removed" % symbol.upper())


@app.post("/v1/orders")
def api_place_order():
    order = place_order(g.user_id, body())
    activity(g.user_id, "ORDER_" + order["status"], order_id=order["id"])
    return jsonify(order=order), 201


@app.get("/v1/orders")
def api_list_orders():
    status = request.args.get("status", "").upper()
    sql = "SELECT * FROM orders WHERE user_id=?"
    args = [g.user_id]
    if status in ("OPEN", "EXECUTED", "CANCELLED"):
        sql += " AND status=?"
        args.append(status)
    sql += " ORDER BY id DESC LIMIT 100"
    return jsonify(orders=[order_view(o) for o in dbq(sql, tuple(args))])


@app.get("/v1/orders/<int:oid>")
def api_get_order(oid):
    o = dbq("SELECT * FROM orders WHERE id=? AND user_id=?", (oid, g.user_id), one=True)
    if not o:
        raise ApiError("Order not found", 404)
    return jsonify(order=order_view(o))


@app.put("/v1/orders/<int:oid>")
def api_modify_order(oid):
    return jsonify(order=modify_order(g.user_id, oid, body()))


@app.delete("/v1/orders/<int:oid>")
def api_cancel_order(oid):
    return jsonify(order=cancel_order(g.user_id, oid))


@app.get("/v1/portfolio/<uid>")
@app.get("/v1/api/portfolio/<uid>")
def api_portfolio(uid):
    if uid != "me" and str(g.user_id) != uid:
        raise ApiError("You can only view your own portfolio", 403)
    return jsonify(build_portfolio(g.user_id))


@app.get("/v1/trades")
def api_trades():
    rows = dbq("SELECT * FROM trades WHERE user_id=? ORDER BY id DESC LIMIT 100", (g.user_id,))
    return jsonify(trades=[trade_view(t) for t in rows])


@app.get("/v1/dashboard")
def api_dashboard():
    quotes = sorted((get_quote(s) for s in MARKET), key=lambda q: q["change_pct"], reverse=True)
    portfolio = build_portfolio(g.user_id)
    portfolio.pop("holdings")
    recent = dbq("SELECT * FROM trades WHERE user_id=? ORDER BY id DESC LIMIT 5", (g.user_id,))
    return jsonify(
        portfolio=portfolio,
        indices=market_indices(),
        gainers=quotes[:5],
        losers=list(reversed(quotes[-5:])),
        recent_trades=[trade_view(t) for t in recent],
        open_orders=dbq("SELECT COUNT(*) AS c FROM orders WHERE user_id=? AND status='OPEN'", (g.user_id,), one=True)["c"],
        holdings_count=dbq("SELECT COUNT(*) AS c FROM positions WHERE user_id=?", (g.user_id,), one=True)["c"],
    )


@app.post("/v1/payments/add")
def pay_add():
    d = body()
    amount = parse_amount(d.get("amount"))
    method = str(d.get("method", "CARD")).upper()
    if method not in ("CARD", "BANK", "WALLET"):
        raise ApiError("Unsupported payment method")
    with tx():
        user = get_user(g.user_id)
        require_kyc(user)
        if method == "BANK" and not user["bank_account"]:
            raise ApiError("Add your bank details in the Profile page first")
        charge = PAYMENTS.charge(amount, method)
        if charge["status"] != "SUCCESS":
            raise ApiError("Payment declined by gateway", 402)
        pid = DB.execute(
            "INSERT INTO payments(user_id, ptype, amount, method, status, ref, ts) VALUES (?,?,?,?,?,?,?)",
            (g.user_id, "ADD", amount, method, "SUCCESS", charge["ref"], now()),
        ).lastrowid
        DB.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amount, g.user_id))
    activity(g.user_id, "FUNDS_ADDED", amount=amount, notify="%s added to your trading wallet." % usd(amount))
    return jsonify(payment_id=pid, balance=r2(get_user(g.user_id)["balance"])), 201


@app.post("/v1/payments/withdraw")
def pay_withdraw():
    amount = parse_amount(body().get("amount"))
    with tx():
        user = get_user(g.user_id)
        require_kyc(user)
        if not user["bank_account"]:
            raise ApiError("Add your bank details in the Profile page first")
        if user["balance"] + 1e-9 < amount:
            raise ApiError("Insufficient available balance (%s)" % usd(user["balance"]))
        payout = PAYMENTS.payout(amount, user["bank_account"])
        DB.execute("UPDATE users SET balance = balance - ? WHERE id=?", (amount, g.user_id))
        pid = DB.execute(
            "INSERT INTO payments(user_id, ptype, amount, method, status, ref, ts) VALUES (?,?,?,?,?,?,?)",
            (g.user_id, "WITHDRAW", amount, "BANK", "SUCCESS", payout["ref"], now()),
        ).lastrowid
    activity(g.user_id, "FUNDS_WITHDRAWN", amount=amount, notify="%s withdrawn to your bank account." % usd(amount))
    return jsonify(payment_id=pid, balance=r2(get_user(g.user_id)["balance"])), 201


@app.post("/v1/payments/<int:pid>/refund")
def pay_refund(pid):
    with tx():
        p = dbq("SELECT * FROM payments WHERE id=? AND user_id=?", (pid, g.user_id), one=True)
        if not p:
            raise ApiError("Payment not found", 404)
        if p["ptype"] != "ADD" or p["status"] != "SUCCESS":
            raise ApiError("Only successful deposits can be refunded")
        user = get_user(g.user_id)
        if user["balance"] + 1e-9 < p["amount"]:
            raise ApiError("Refund needs the deposited funds to be unspent in your wallet")
        refund = PAYMENTS.refund(p["ref"])
        DB.execute("UPDATE users SET balance = balance - ? WHERE id=?", (p["amount"], g.user_id))
        DB.execute("UPDATE payments SET status='REFUNDED' WHERE id=?", (pid,))
        DB.execute(
            "INSERT INTO payments(user_id, ptype, amount, method, status, ref, parent_id, ts) VALUES (?,?,?,?,?,?,?,?)",
            (g.user_id, "REFUND", p["amount"], p["method"], "SUCCESS", refund["ref"], pid, now()),
        )
    activity(g.user_id, "REFUND", amount=p["amount"], notify="Refund of %s processed to your original payment method." % usd(p["amount"]))
    return jsonify(balance=r2(get_user(g.user_id)["balance"]))


@app.get("/v1/payments")
def pay_list():
    rows = dbq("SELECT * FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 100", (g.user_id,))
    for r in rows:
        r["ts"] = ts_str(r["ts"])
    return jsonify(payments=rows)


@app.get("/v1/system/overview")
def system_overview():
    topics = {}
    for name in Kafka.TOPICS:
        topics[name] = {
            "offset": KAFKA.offsets[name],
            "events": list(KAFKA.logs[name])[-6:][::-1],
        }
    notes = dbq("SELECT * FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 12", (g.user_id,))
    for n in notes:
        n["ts"] = ts_str(n["ts"])
    counts = {
        t: dbq("SELECT COUNT(*) AS c FROM %s" % t, one=True)["c"]
        for t in ("users", "orders", "positions", "trades", "watchlists", "payments")
    }
    with SESSION_LOCK:
        active = len(SESSIONS)
    return jsonify(
        topics=topics,
        gateway_log=list(GATEWAY_LOG)[-14:][::-1],
        notifications=notes,
        db_rows=counts,
        analytics={k: round(v, 2) for k, v in ANALYTICS.items()},
        active_sessions=active,
    )


INDEX_HTML = r'''<!DOCTYPE html>
<html lang="en" data-theme="dark">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>TradeNova - Stock Trading Platform</title>
    <style>
        :root {
            --bg: #0b1220;
            --panel: #111a2e;
            --panel-2: #172238;
            --border: #243352;
            --text: #e6ecf8;
            --muted: #8fa0bf;
            --accent: #4f8cff;

            --green: #22c55e;
            --red: #ef4444;
            --amber: #f59e0b;

        }

        [data-theme="light"] {
            --bg: #f2f5fb;
            --panel: #ffffff;
            --panel-2: #f6f8fd;
            --border: #dbe3f2;
            --text: #14213d;
            --muted: #5d6b88;

        }

        * {
            box-sizing: border-box;
        }

        [hidden] {
            display: none !important;
        }

        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            font-size: 14px;
            line-height: 1.45;
        }

        h1, h2, h3, h4 {
            margin: 0;
        }

        button, input, select {
            font-family: inherit;
            font-size: 14px;
        }


        label {
            display: block;
            margin: 12px 0 5px;
            color: var(--muted);
            font-size: 12px;
            font-weight: 600;
            letter-spacing: .03em;
            text-transform: uppercase;
        }

        input, select {
            width: 100%;
            padding: 10px 12px;
            background: var(--panel-2);
            color: var(--text);
            border: 1px solid var(--border);
            border-radius: 4px;
            outline: none;
        }

        input:focus, select:focus {
            border-color: var(--accent);
            outline: 2px solid var(--accent);
            outline-offset: 1px;
        }

        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            padding: 9px 16px;
            border: 1px solid transparent;
            border-radius: 4px;
            background: var(--panel-2);
            color: var(--text);
            font-weight: 600;
            cursor: pointer;

        }

        .btn:hover {
            filter: brightness(1.12);
        }

        .btn-primary {
            background: var(--accent);
            color: #fff;
        }

        .btn-green {
            background: var(--green);
            color: #04210f;
        }

        .btn-red {
            background: var(--red);
            color: #fff;
        }

        .btn-ghost {
            background: transparent;
            border-color: var(--border);
        }

        .btn-sm {
            padding: 5px 10px;
            font-size: 12px;
        }

        .btn-block {
            width: 100%;
            margin-top: 16px;
            padding: 12px;
        }

        .form-msg {
            min-height: 20px;
            margin-top: 10px;
            font-size: 13px;
        }

        .form-msg.err {
            color: var(--red);
        }

        .form-msg.ok {
            color: var(--green);
        }


        #auth-screen {
            display: grid;
            grid-template-columns: 1.1fr 1fr;
            min-height: 100vh;
        }

        .auth-hero {
            display: flex;
            flex-direction: column;
            justify-content: center;
            padding: 48px;
            background: var(--panel);
            border-right: 1px solid var(--border);
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 24px;
            font-weight: 800;
            letter-spacing: -.02em;
        }

        .auth-hero h2 {
            margin: 28px 0 12px;
            font-size: 26px;
            line-height: 1.15;
        }

        .auth-hero p {
            max-width: 460px;
            color: var(--muted);
        }

        .auth-panel {
            display: grid;
            place-items: center;
            padding: 24px;
        }

        .auth-card {
            width: 100%;
            max-width: 420px;
            padding: 28px;
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 8px;
        }

        .tabs {
            display: flex;
            gap: 6px;
            margin-bottom: 8px;
            padding: 4px;
            background: var(--panel-2);
            border-radius: 6px;
        }

        .tab {
            flex: 1;
            padding: 9px;
            border: 0;
            border-radius: 4px;
            background: transparent;
            color: var(--muted);
            font-weight: 600;
            cursor: pointer;
        }

        .tab.active {
            background: var(--accent);
            color: #fff;
        }

        .pw-rules {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2px 10px;
            margin: 8px 0 0;
            padding: 0;
            color: var(--muted);
            font-size: 12px;
            list-style: none;
        }

        .pw-rules li.ok {
            color: var(--green);
        }

        .policy-note {
            margin-top: 14px;
            padding: 10px 12px;
            background: var(--panel-2);
            border-radius: 4px;
            color: var(--muted);
            font-size: 12px;
        }


        #app-screen {
            display: grid;
            grid-template-columns: 220px 1fr;
            grid-template-rows: auto auto 1fr;
            grid-template-areas:
                "top top"
                "tick tick"
                "nav main";
            min-height: 100vh;
        }

        .topbar {
            grid-area: top;
            display: flex;
            align-items: center;
            gap: 16px;
            padding: 12px 20px;
            background: var(--panel);
            border-bottom: 1px solid var(--border);
        }

        .topbar .spacer {
            flex: 1;
        }

        .chip {
            padding: 6px 12px;
            background: var(--panel-2);
            border: 1px solid var(--border);
            border-radius: 3px;
            font-size: 13px;
            white-space: nowrap;
        }

        .chip b {
            color: var(--green);
        }

        .live-dot {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            color: var(--green);
            font-size: 12px;
            font-weight: 700;
            letter-spacing: .06em;
        }

        .live-dot::before {
            content: "";
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--green);
        }

        .ticker {
            grid-area: tick;
            display: flex;
            gap: 28px;
            padding: 8px 20px;
            overflow-x: auto;
            background: var(--panel-2);
            border-bottom: 1px solid var(--border);
            font-size: 12px;
            white-space: nowrap;
        }

        .ticker span b {
            margin-right: 6px;
            color: var(--muted);
        }

        .nav {
            grid-area: nav;
            padding: 14px 10px;
            background: var(--panel);
            border-right: 1px solid var(--border);
        }

        .nav button {
            display: flex;
            align-items: center;
            gap: 10px;
            width: 100%;
            margin-bottom: 4px;
            padding: 10px 12px;
            border: 0;
            border-radius: 4px;
            background: transparent;
            color: var(--muted);
            font-weight: 600;
            text-align: left;
            cursor: pointer;
        }

        .nav button:hover {
            background: var(--panel-2);
            color: var(--text);
        }

        .nav button.active {
            background: rgba(79, 140, 255, .16);
            color: var(--accent);
        }

        main {
            grid-area: main;
            min-width: 0;
            padding: 22px;
        }

        .page-head {
            display: flex;
            align-items: center;
            gap: 14px;
            margin-bottom: 16px;
        }

        .page-head h2 {
            font-size: 22px;
        }

        .page-head .spacer {
            flex: 1;
        }

        .loading {
            padding: 40px;
            color: var(--muted);
            text-align: center;
        }


        .card {
            margin-bottom: 16px;
            padding: 16px;
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 6px;
        }

        .card h3 {
            margin-bottom: 12px;
            font-size: 15px;
        }

        .grid {
            display: grid;
            gap: 14px;
            margin-bottom: 16px;
        }

        .grid-4 {
            grid-template-columns: repeat(4, 1fr);
        }

        .grid-3 {
            grid-template-columns: repeat(3, 1fr);
        }

        .grid-2 {
            grid-template-columns: repeat(2, 1fr);
        }

        .grid .card {
            margin-bottom: 0;
        }

        .stat .label {
            color: var(--muted);
            font-size: 12px;
            font-weight: 600;
            letter-spacing: .04em;
            text-transform: uppercase;
        }

        .stat .value {
            margin-top: 6px;
            font-size: 24px;
            font-weight: 700;
        }

        .stat .sub {
            margin-top: 2px;
            color: var(--muted);
            font-size: 12px;
        }

        .toolbar {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 14px;
        }

        .toolbar input {
            flex: 1;
            min-width: 200px;
        }

        .toolbar select {
            width: auto;
        }


        .table-wrap {
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th {
            padding: 9px 10px;
            border-bottom: 1px solid var(--border);
            color: var(--muted);
            font-size: 11px;
            letter-spacing: .05em;
            text-align: left;
            text-transform: uppercase;
            white-space: nowrap;
        }

        td {
            padding: 10px;
            border-bottom: 1px solid var(--border);
            white-space: nowrap;
        }

        tr:last-child td {
            border-bottom: 0;
        }

        .num {
            text-align: right;
        }

        th.num {
            text-align: right;
        }

        .sym {
            color: var(--accent);
            font-weight: 700;
            cursor: pointer;
        }

        .sym small {
            display: block;
            color: var(--muted);
            font-weight: 400;
        }

        .up {
            color: var(--green);
        }

        .down {
            color: var(--red);
        }

        .flat {
            color: var(--muted);
        }

        .badge {
            display: inline-block;
            padding: 2px 9px;
            border-radius: 3px;
            background: var(--panel-2);
            font-size: 11px;
            font-weight: 700;
        }

        .badge.green { background: rgba(34, 197, 94, .18); color: var(--green); }
        .badge.red { background: rgba(239, 68, 68, .18); color: var(--red); }
        .badge.amber { background: rgba(245, 158, 11, .2); color: var(--amber); }
        .badge.blue { background: rgba(79, 140, 255, .2); color: var(--accent); }

        .empty {
            padding: 26px;
            color: var(--muted);
            text-align: center;
        }

        .px {
            padding: 2px 4px;
            border-radius: 2px;
            font-variant-numeric: tabular-nums;
            font-weight: 600;
        }

        @keyframes flashUp {
            from { background: rgba(34, 197, 94, .5); }
            to { background: transparent; }
        }

        @keyframes flashDown {
            from { background: rgba(239, 68, 68, .5); }
            to { background: transparent; }
        }

        .flash-up { animation: flashUp .8s ease-out; }
        .flash-down { animation: flashDown .8s ease-out; }

        .pager {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 10px;
            margin-top: 12px;
            color: var(--muted);
        }

        .bar-row {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 8px;
        }

        .bar-row .name {
            width: 90px;
            color: var(--muted);
        }

        .bar {
            flex: 1;
            height: 10px;
            overflow: hidden;
            background: var(--panel-2);
            border-radius: 2px;
        }

        .bar i {
            display: block;
            height: 100%;
            background: var(--accent);
        }

        .chips {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 10px;
        }

        .wl-tabs {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 8px;
            margin-bottom: 14px;
        }

        .wl-tabs .tab {
            flex: none;
            padding: 7px 14px;
            background: var(--panel-2);
        }


        #modal-backdrop {
            position: fixed;
            inset: 0;
            z-index: 50;
            display: grid;
            place-items: center;
            padding: 16px;
            overflow-y: auto;
            background: rgba(3, 8, 20, .66);
        }

        #modal {
            width: 100%;
            max-width: 860px;
            padding: 20px;
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 8px;
        }

        #modal.small {
            max-width: 420px;
        }

        .modal-head {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 14px;
        }

        .modal-head h3 {
            font-size: 20px;
        }

        .modal-head .price {
            font-size: 26px;
            font-weight: 800;
        }

        .modal-body {
            display: grid;
            grid-template-columns: 1.5fr 1fr;
            gap: 20px;
        }

        .chart {
            width: 100%;
            height: 220px;
            background: var(--panel-2);
            border-radius: 6px;
        }

        .chart-axis {
            display: flex;
            justify-content: space-between;
            margin-top: 4px;
            color: var(--muted);
            font-size: 11px;
        }

        .stat-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-top: 14px;
        }

        .stat-grid div {
            padding: 8px 10px;
            background: var(--panel-2);
            border-radius: 4px;
        }

        .stat-grid small {
            display: block;
            color: var(--muted);
            font-size: 11px;
        }

        .seg {
            display: flex;
            gap: 6px;
            padding: 4px;
            background: var(--panel-2);
            border-radius: 6px;
        }

        .seg-btn {
            flex: 1;
            padding: 9px;
            border: 0;
            border-radius: 4px;
            background: transparent;
            color: var(--muted);
            font-weight: 700;
            cursor: pointer;
        }

        .seg-btn.buy-on {
            background: var(--green);
            color: #04210f;
        }

        .seg-btn.sell-on {
            background: var(--red);
            color: #fff;
        }

        .est {
            margin-top: 12px;
            padding: 10px 12px;
            background: var(--panel-2);
            border-radius: 4px;
            color: var(--muted);
            font-size: 13px;
        }

        .est b {
            color: var(--text);
        }

        .kyc-banner {
            margin-bottom: 16px;
            padding: 12px 16px;
            background: rgba(245, 158, 11, .14);
            border: 1px solid rgba(245, 158, 11, .5);
            border-radius: 6px;
            color: var(--amber);
        }

        .kyc-banner a {
            color: inherit;
            font-weight: 700;
            text-decoration: underline;
            cursor: pointer;
        }

        .mono {
            font-family: ui-monospace, Menlo, Consolas, monospace;
            font-size: 12px;
        }

        .event {
            padding: 6px 0;
            border-bottom: 1px dashed var(--border);
            color: var(--muted);
            font-size: 12px;
            word-break: break-all;
        }

        .event:last-child {
            border-bottom: 0;
        }

        .event b {
            color: var(--accent);
        }

        .flow {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 8px;
            color: var(--muted);
        }

        .flow span.node {
            padding: 6px 12px;
            background: var(--panel-2);
            border: 1px solid var(--border);
            border-radius: 4px;
            color: var(--text);
            font-weight: 600;
        }


        #toasts {
            position: fixed;
            right: 16px;
            bottom: 16px;
            z-index: 100;
            display: grid;
            gap: 8px;
        }

        .toast {
            max-width: 360px;
            padding: 12px 16px;
            background: var(--panel);
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 4px;
        }

        .toast.ok { border-left-color: var(--green); }
        .toast.err { border-left-color: var(--red); }


        @media (max-width: 980px) {
            .grid-4 { grid-template-columns: repeat(2, 1fr); }
            .grid-3 { grid-template-columns: 1fr; }
            .grid-2 { grid-template-columns: 1fr; }
            .modal-body { grid-template-columns: 1fr; }
            #auth-screen { grid-template-columns: 1fr; }
            .auth-hero { display: none; }
        }

        @media (max-width: 760px) {
            #app-screen {
                grid-template-columns: 1fr;
                grid-template-areas:
                    "top"
                    "tick"
                    "nav"
                    "main";
            }

            .nav {
                display: flex;
                gap: 4px;
                overflow-x: auto;
                padding: 8px;
                border-right: 0;
                border-bottom: 1px solid var(--border);
            }

            .nav button {
                width: auto;
                margin: 0;
                white-space: nowrap;
            }

            .grid-4 { grid-template-columns: 1fr; }
            .chip.hide-sm { display: none; }
        }
    </style>
</head>

<body>
    <div id="toasts"></div>


    <section id="auth-screen" hidden>
        <div class="auth-hero">
            <div class="brand">TradeNova</div>
            <h2>Simulated stock exchange</h2>
            <p>
                20 stocks, prices updated every second. Market and limit orders settle cash,
                holdings and trade records in a single transaction, so a failed order changes nothing.
            </p>
            <p>
                To place your first order: register, verify KYC on the Profile page with any
                6-20 character document number, then add funds from the Wallet page.
            </p>
        </div>

        <div class="auth-panel">
            <div class="auth-card">
                <div class="tabs">
                    <button class="tab active" id="tab-login" onclick="authTab('login')">Login</button>
                    <button class="tab" id="tab-register" onclick="authTab('register')">Register</button>
                </div>

                <form id="login-form" onsubmit="return doLogin(event)">
                    <label for="l-id">User ID or email</label>
                    <input id="l-id" autocomplete="username" placeholder="e.g. trader_01" required>
                    <label for="l-pw">Password</label>
                    <input id="l-pw" type="password" autocomplete="current-password" placeholder="Your password" required>
                    <button class="btn btn-primary btn-block" type="submit">Login</button>
                    <div id="login-msg" class="form-msg"></div>
                    <div class="policy-note">
                        Login policy: 5 wrong attempts lock the account for 5 minutes, sessions expire
                        after 30 idle minutes, and a new login signs out your older session.
                    </div>
                </form>

                <form id="register-form" onsubmit="return doRegister(event)" hidden>
                    <label for="r-name">Full name</label>
                    <input id="r-name" autocomplete="name" placeholder="Your full name" required>
                    <label for="r-id">User ID</label>
                    <input id="r-id" autocomplete="username" placeholder="3-20 letters, numbers, _" required>
                    <label for="r-email">Email</label>
                    <input id="r-email" type="email" autocomplete="email" placeholder="you@example.com" required>
                    <label for="r-pw">Password</label>
                    <input id="r-pw" type="password" autocomplete="new-password" oninput="checkPwRules()" placeholder="Create a strong password" required>
                    <ul class="pw-rules" id="pw-rules">
                        <li data-rule="len">8+ characters</li>
                        <li data-rule="low">lowercase letter</li>
                        <li data-rule="up">uppercase letter</li>
                        <li data-rule="dig">a digit</li>
                    </ul>
                    <label for="r-pw2">Confirm password</label>
                    <input id="r-pw2" type="password" autocomplete="new-password" placeholder="Repeat password" required>
                    <button class="btn btn-primary btn-block" type="submit">Create account</button>
                    <div id="register-msg" class="form-msg"></div>
                </form>
            </div>
        </div>
    </section>


    <div id="app-screen" hidden>
        <header class="topbar">
            <div class="brand">
                TradeNova
            </div>
            <div class="spacer"></div>
            <div class="chip">Cash <b id="hdr-balance">$0.00</b></div>
            <div class="chip hide-sm" id="hdr-user"></div>
            <button class="btn btn-ghost btn-sm" onclick="doLogout()">Logout</button>
        </header>

        <div class="ticker" id="ticker"></div>

        <nav class="nav" id="nav">
            <button data-page="dashboard" onclick="showPage('dashboard')">Dashboard</button>
            <button data-page="market" onclick="showPage('market')">Market</button>
            <button data-page="watchlist" onclick="showPage('watchlist')">Watchlist</button>
            <button data-page="orders" onclick="showPage('orders')">Orders</button>
            <button data-page="portfolio" onclick="showPage('portfolio')">Portfolio</button>
            <button data-page="wallet" onclick="showPage('wallet')">Wallet</button>
            <button data-page="profile" onclick="showPage('profile')">Profile</button>
            <button data-page="system" onclick="showPage('system')">System</button>
        </nav>

        <main id="view"></main>
    </div>


    <div id="modal-backdrop" hidden onclick="if (event.target === this) closeModal()">
        <div id="modal"></div>
    </div>

    <script>

        const S = {
            token: null,
            user: null,
            page: 'dashboard',
            live: {},
            stocks: [],
            refresh: 2000,
            timer: null,
            tick: 0,
            market: { page: 1, q: '', sector: '', timer: null },
            wlActive: null,
            orderFilter: '',
            modalSym: null,
            side: 'BUY',
        };

        try { S.token = sessionStorage.getItem('tn_token'); } catch (e) {  }

        const $ = id => document.getElementById(id);
        const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c =>
            ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
        const money = n => '$' + Number(n || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const num = n => Number(n || 0).toLocaleString('en-US');
        const sgn = n => (n > 0 ? '+' : '') + Number(n || 0).toFixed(2);
        const spct = n => sgn(n) + '%';
        const smoney = n => (n > 0 ? '+' : n < 0 ? '-' : '') + money(Math.abs(n));
        const cls = n => (n > 0 ? 'up' : n < 0 ? 'down' : 'flat');

        function toast(msg, type) {
            const el = document.createElement('div');
            el.className = 'toast ' + (type || '');
            el.textContent = msg;
            $('toasts').appendChild(el);
            setTimeout(() => el.remove(), 4200);
        }

        async function api(method, path, payload) {
            const opts = { method: method, headers: { 'Content-Type': 'application/json' } };
            if (S.token) opts.headers['Authorization'] = 'Bearer ' + S.token;
            if (payload !== undefined) opts.body = JSON.stringify(payload);
            const res = await fetch(path.replace(/^\//, ''), opts);
            let data = {};
            try { data = await res.json(); } catch (e) {  }
            if (res.status === 401 && S.token && path.indexOf('/auth/') === -1) {
                endSession('Session expired. Please login again.');
                throw new Error('Session expired');
            }
            if (!res.ok) throw new Error(data.error || ('Request failed (' + res.status + ')'));
            return data;
        }


        const pxSpan = (sym, price) =>
            `<span class="px" data-sym="${sym}" data-last="${price}">${money(price)}</span>`;
        const chgSpan = (sym, pct) =>
            `<span class="chg ${cls(pct)}" data-sym="${sym}">${spct(pct)}</span>`;
        const statusBadge = s => {
            const c = { EXECUTED: 'green', OPEN: 'blue', CANCELLED: 'amber', SUCCESS: 'green',
                        REFUNDED: 'amber', VERIFIED: 'green', PENDING: 'amber' }[s] || '';
            return `<span class="badge ${c}">${esc(s)}</span>`;
        };
        const sideBadge = s => `<span class="badge ${s === 'BUY' ? 'green' : 'red'}">${s}</span>`;


        function authTab(which) {
            $('login-form').hidden = which !== 'login';
            $('register-form').hidden = which !== 'register';
            $('tab-login').classList.toggle('active', which === 'login');
            $('tab-register').classList.toggle('active', which === 'register');
        }

        function checkPwRules() {
            const pw = $('r-pw').value;
            const rules = { len: pw.length >= 8, low: /[a-z]/.test(pw), up: /[A-Z]/.test(pw), dig: /\d/.test(pw) };
            document.querySelectorAll('#pw-rules li').forEach(li => {
                li.classList.toggle('ok', rules[li.dataset.rule]);
            });
        }

        async function doLogin(ev) {
            ev.preventDefault();
            const msg = $('login-msg');
            msg.className = 'form-msg';
            msg.textContent = 'Signing in...';
            try {
                const r = await api('POST', '/v1/auth/login', {
                    user_id: $('l-id').value.trim(),
                    password: $('l-pw').value,
                });
                $('l-pw').value = '';
                msg.textContent = '';
                await startSession(r.token, r.user);
            } catch (e) {
                msg.className = 'form-msg err';
                msg.textContent = e.message;
            }
            return false;
        }

        async function doRegister(ev) {
            ev.preventDefault();
            const msg = $('register-msg');
            msg.className = 'form-msg err';
            if ($('r-pw').value !== $('r-pw2').value) {
                msg.textContent = 'Passwords do not match';
                return false;
            }
            try {
                const r = await api('POST', '/v1/auth/register', {
                    full_name: $('r-name').value,
                    username: $('r-id').value,
                    email: $('r-email').value,
                    password: $('r-pw').value,
                });
                msg.textContent = '';
                $('l-id').value = r.user_id;
                $('register-form').reset();
                checkPwRules();
                authTab('login');
                const lm = $('login-msg');
                lm.className = 'form-msg ok';
                lm.textContent = 'Account created. Please login.';
            } catch (e) {
                msg.textContent = e.message;
            }
            return false;
        }

        async function startSession(token, user) {
            S.token = token;
            try { sessionStorage.setItem('tn_token', token); } catch (e) {  }
            applyUser(user);
            $('auth-screen').hidden = true;
            $('app-screen').hidden = false;
            const list = await api('GET', '/v1/stocks?page_size=100');
            S.stocks = list.items;
            await refreshTicker();
            startPolling();
            showPage('dashboard');
        }

        function endSession(message) {
            stopPolling();
            S.token = null;
            S.user = null;
            try { sessionStorage.removeItem('tn_token'); } catch (e) {  }
            closeModal();
            $('app-screen').hidden = true;
            $('auth-screen').hidden = false;
            if (message) {
                const lm = $('login-msg');
                lm.className = 'form-msg err';
                lm.textContent = message;
            }
        }

        async function doLogout() {
            try { await api('POST', '/v1/auth/logout', {}); } catch (e) {  }
            endSession('');
            toast('You have been logged out', 'ok');
        }

        function applyUser(user) {
            S.user = user;
            const prefs = user.prefs || {};
            document.documentElement.dataset.theme = prefs.theme || 'dark';
            S.refresh = (prefs.refresh || 2) * 1000;
            $('hdr-balance').textContent = money(user.balance);
            $('hdr-user').textContent = user.full_name + ' (' + user.username + ')';
        }

        async function refreshUser() {
            const r = await api('GET', '/v1/users/me');
            applyUser(r.user);
        }


        const LOADERS = {
            dashboard: loadDashboard, market: loadMarket, watchlist: loadWatchlist, orders: loadOrders,
            portfolio: loadPortfolio, wallet: loadWallet, profile: loadProfile, system: loadSystem,
        };
        const AUTO_REFRESH = ['dashboard', 'orders', 'portfolio', 'system'];

        function startPolling() {
            stopPolling();
            S.timer = setInterval(pollLive, S.refresh);
        }

        function stopPolling() {
            if (S.timer) clearInterval(S.timer);
            S.timer = null;
        }

        async function pollLive() {
            if (!S.token) return;
            try {
                const d = await api('GET', '/v1/stocks/live');
                S.live = d.prices;
                applyLive();
                if (S.modalSym) refreshModalChart();
                S.tick++;
                if (S.tick % 3 === 0) {
                    await refreshUser();
                    await refreshTicker();
                    if (AUTO_REFRESH.indexOf(S.page) !== -1) await LOADERS[S.page]();
                }
            } catch (e) {  }
        }

        function applyLive() {
            document.querySelectorAll('.px').forEach(el => {
                const q = S.live[el.dataset.sym];
                if (!q) return;
                const text = money(q.price);
                if (el.textContent !== text) {
                    const prev = parseFloat(el.dataset.last || q.price);
                    el.textContent = text;
                    el.classList.remove('flash-up', 'flash-down');
                    void el.offsetWidth;
                    el.classList.add(q.price >= prev ? 'flash-up' : 'flash-down');
                    el.dataset.last = q.price;
                }
            });
            document.querySelectorAll('.chg').forEach(el => {
                const q = S.live[el.dataset.sym];
                if (!q) return;
                el.textContent = spct(q.change_pct);
                el.className = 'chg ' + cls(q.change_pct);
            });
            updateEst();
        }

        async function refreshTicker() {
            const d = await api('GET', '/v1/indices');
            $('ticker').innerHTML = d.indices.map(i =>
                `<span><b>${i.name}</b>${Number(i.value).toLocaleString('en-US')}
                 <span class="${cls(i.change_pct)}">${spct(i.change_pct)}</span></span>`).join('');
        }


        async function showPage(name) {
            S.page = name;
            document.querySelectorAll('#nav button').forEach(b =>
                b.classList.toggle('active', b.dataset.page === name));
            $('view').innerHTML = '<div class="loading">Loading...</div>';
            try {
                await LOADERS[name]();
            } catch (e) {
                if (e.message !== 'Session expired') {
                    $('view').innerHTML = `<div class="empty">${esc(e.message)}</div>`;
                }
            }
        }

        function kycBanner() {
            if (!S.user || S.user.kyc_status === 'VERIFIED') return '';
            return `<div class="kyc-banner">
                        KYC verification is pending. You need it to add funds and trade.
                        <a onclick="showPage('profile')">Verify now</a>
                    </div>`;
        }


        function moversTable(rows) {
            return `<table>
                <tbody>
                    ${rows.map(q => `
                        <tr>
                            <td class="sym" onclick="openStock('${q.symbol}')">${q.symbol}<small>${esc(q.name)}</small></td>
                            <td class="num">${pxSpan(q.symbol, q.price)}</td>
                            <td class="num">${chgSpan(q.symbol, q.change_pct)}</td>
                        </tr>`).join('')}
                </tbody>
            </table>`;
        }

        async function loadDashboard() {
            const d = await api('GET', '/v1/dashboard');
            if (S.page !== 'dashboard') return;
            const p = d.portfolio;
            $('view').innerHTML = `
                <div class="page-head">
                    <h2>Dashboard</h2>
                </div>
                ${kycBanner()}
                <div class="grid grid-4">
                    <div class="card stat">
                        <div class="label">Net worth</div>
                        <div class="value">${money(p.net_worth)}</div>
                        <div class="sub">Cash + blocked + holdings</div>
                    </div>
                    <div class="card stat">
                        <div class="label">Available cash</div>
                        <div class="value">${money(p.cash)}</div>
                        <div class="sub">Blocked in open orders: ${money(p.blocked)}</div>
                    </div>
                    <div class="card stat">
                        <div class="label">Holdings value</div>
                        <div class="value">${money(p.current_value)}</div>
                        <div class="sub">Invested ${money(p.invested)} &middot; ${d.holdings_count} stock(s)</div>
                    </div>
                    <div class="card stat">
                        <div class="label">Total P&amp;L</div>
                        <div class="value ${cls(p.total_pnl)}">${smoney(p.total_pnl)}</div>
                        <div class="sub">Unrealized ${smoney(p.unrealized_pnl)} &middot; Realized ${smoney(p.realized_pnl)}</div>
                    </div>
                </div>

                <div class="grid grid-2">
                    <div class="card">
                        <h3>Top gainers</h3>
                        ${moversTable(d.gainers)}
                    </div>
                    <div class="card">
                        <h3>Top losers</h3>
                        ${moversTable(d.losers)}
                    </div>
                </div>

                <div class="grid grid-2">
                    <div class="card">
                        <h3>Recent trades</h3>
                        ${d.recent_trades.length ? `
                            <div class="table-wrap"><table>
                                <thead><tr><th>Time</th><th>Stock</th><th>Side</th><th class="num">Qty</th><th class="num">Price</th></tr></thead>
                                <tbody>
                                    ${d.recent_trades.map(t => `
                                        <tr>
                                            <td>${t.ts}</td>
                                            <td>${t.symbol}</td>
                                            <td>${sideBadge(t.side)}</td>
                                            <td class="num">${t.qty}</td>
                                            <td class="num">${money(t.price)}</td>
                                        </tr>`).join('')}
                                </tbody>
                            </table></div>`
                            : '<div class="empty">No trades yet. Buy a stock from the Market page.</div>'}
                    </div>
                    <div class="card">
                        <h3>Open orders</h3>
                        <p style="color: var(--muted)">
                            ${d.open_orders} limit order(s) waiting for their price. Funds for open buy orders are blocked until they fill or are cancelled.
                        </p>
                        <div class="chips">
                            <button class="btn btn-primary" onclick="openStock('${(S.stocks[0] || {}).symbol || 'AAPL'}')">Place an order</button>
                            <button class="btn" onclick="showPage('wallet')">Add funds</button>
                        </div>
                    </div>
                </div>`;
        }


        async function loadMarket() {
            if (!$('market-table')) {
                const sectors = ['Technology', 'Consumer', 'Finance', 'Energy', 'Healthcare'];
                $('view').innerHTML = `
                    <div class="page-head">
                        <h2>Market</h2>
                    </div>
                    ${kycBanner()}
                    <div class="toolbar">
                        <input id="mk-q" placeholder="Search by symbol or company..." value="${esc(S.market.q)}" oninput="marketSearch(this.value)">
                        <select id="mk-sector" onchange="marketSector(this.value)">
                            <option value="">All sectors</option>
                            ${sectors.map(s => `<option ${S.market.sector === s ? 'selected' : ''}>${s}</option>`).join('')}
                        </select>
                    </div>
                    <div class="card" id="market-table"></div>`;
            }
            await loadMarketTable();
        }

        async function loadMarketTable() {
            const m = S.market;
            const d = await api('GET', `/v1/stocks?page=${m.page}&page_size=10&q=${encodeURIComponent(m.q)}&sector=${encodeURIComponent(m.sector)}`);
            if (S.page !== 'market' || !$('market-table')) return;
            $('market-table').innerHTML = d.items.length ? `
                <div class="table-wrap"><table>
                    <thead>
                        <tr>
                            <th>Stock</th><th>Sector</th>
                            <th class="num">Price</th><th class="num">Change</th>
                            <th class="num">High</th><th class="num">Low</th><th class="num">Volume</th>
                            <th class="num">Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${d.items.map(q => `
                            <tr>
                                <td class="sym" onclick="openStock('${q.symbol}')">${q.symbol}<small>${esc(q.name)}</small></td>
                                <td>${q.sector}</td>
                                <td class="num">${pxSpan(q.symbol, q.price)}</td>
                                <td class="num">${chgSpan(q.symbol, q.change_pct)}</td>
                                <td class="num">${money(q.high)}</td>
                                <td class="num">${money(q.low)}</td>
                                <td class="num">${num(q.volume)}</td>
                                <td class="num">
                                    <button class="btn btn-sm" title="Add to watchlist" onclick="quickWatch('${q.symbol}')">&#9734;</button>
                                    <button class="btn btn-sm btn-green" onclick="openStock('${q.symbol}', 'BUY')">Buy</button>
                                    <button class="btn btn-sm btn-red" onclick="openStock('${q.symbol}', 'SELL')">Sell</button>
                                </td>
                            </tr>`).join('')}
                    </tbody>
                </table></div>
                <div class="pager">
                    <span>${d.total} stock(s) &middot; page ${d.page} of ${d.pages}</span>
                    <button class="btn btn-sm btn-ghost" ${d.page <= 1 ? 'disabled' : ''} onclick="marketPage(${d.page - 1})">Prev</button>
                    <button class="btn btn-sm btn-ghost" ${d.page >= d.pages ? 'disabled' : ''} onclick="marketPage(${d.page + 1})">Next</button>
                </div>`
                : '<div class="empty">No stocks match your search.</div>';
        }

        function marketPage(p) { S.market.page = p; loadMarketTable(); }
        function marketSector(v) { S.market.sector = v; S.market.page = 1; loadMarketTable(); }
        function marketSearch(v) {
            clearTimeout(S.market.timer);
            S.market.timer = setTimeout(() => { S.market.q = v.trim(); S.market.page = 1; loadMarketTable(); }, 250);
        }

        async function quickWatch(sym) {
            try {
                const d = await api('GET', '/v1/watchlists');
                const target = d.watchlists.find(w => w.id === S.wlActive) || d.watchlists[0];
                await api('POST', `/v1/watchlists/${target.id}/stocks`, { symbol: sym });
                toast(`${sym} added to "${target.name}"`, 'ok');
            } catch (e) { toast(e.message, 'err'); }
        }


        async function loadWatchlist() {
            const d = await api('GET', '/v1/api/watchlist');
            if (S.page !== 'watchlist') return;
            const lists = d.watchlists;
            S.wlNames = {};
            lists.forEach(w => { S.wlNames[w.id] = w.name; });
            if (!lists.find(w => w.id === S.wlActive)) S.wlActive = lists[0].id;
            const active = lists.find(w => w.id === S.wlActive);
            const inList = active.stocks.map(s => s.symbol);
            const addable = S.stocks.filter(s => inList.indexOf(s.symbol) === -1);

            $('view').innerHTML = `
                <div class="page-head">
                    <h2>Watchlist</h2>
                    <div class="spacer"></div>
                    <button class="btn btn-primary btn-sm" onclick="newWatchlist()">+ New list</button>
                </div>
                <div class="wl-tabs">
                    ${lists.map(w => `<button class="tab ${w.id === S.wlActive ? 'active' : ''}" onclick="pickWatchlist(${w.id})">${esc(w.name)} (${w.stocks.length})</button>`).join('')}
                    <button class="btn btn-ghost btn-sm" onclick="renameWatchlist(${active.id})">Rename</button>
                    <button class="btn btn-ghost btn-sm" onclick="deleteWatchlist(${active.id})">Delete</button>
                </div>
                <div class="toolbar">
                    <select id="wl-add" style="flex: 1; min-width: 220px; width: auto">
                        ${addable.map(s => `<option value="${s.symbol}">${s.symbol} - ${esc(s.name)}</option>`).join('')}
                    </select>
                    <button class="btn btn-primary" onclick="wlAdd(${active.id})" ${addable.length ? '' : 'disabled'}>Add stock</button>
                </div>
                <div class="card">
                    ${active.stocks.length ? `
                        <div class="table-wrap"><table>
                            <thead>
                                <tr>
                                    <th>Stock</th><th class="num">Price</th><th class="num">Change</th>
                                    <th class="num">High</th><th class="num">Low</th><th class="num">Action</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${active.stocks.map(q => `
                                    <tr>
                                        <td class="sym" onclick="openStock('${q.symbol}')">${q.symbol}<small>${esc(q.name)}</small></td>
                                        <td class="num">${pxSpan(q.symbol, q.price)}</td>
                                        <td class="num">${chgSpan(q.symbol, q.change_pct)}</td>
                                        <td class="num">${money(q.high)}</td>
                                        <td class="num">${money(q.low)}</td>
                                        <td class="num">
                                            <button class="btn btn-sm btn-green" onclick="openStock('${q.symbol}', 'BUY')">Buy</button>
                                            <button class="btn btn-sm btn-ghost" onclick="wlRemove(${active.id}, '${q.symbol}')">Remove</button>
                                        </td>
                                    </tr>`).join('')}
                            </tbody>
                        </table></div>`
                        : '<div class="empty">This watchlist is empty. Add a stock above.</div>'}
                </div>`;
        }

        function pickWatchlist(id) { S.wlActive = id; showPage('watchlist'); }

        async function wlAction(fn, okMsg) {
            try {
                await fn();
                if (okMsg) toast(okMsg, 'ok');
                await loadWatchlist();
            } catch (e) { toast(e.message, 'err'); }
        }

        function wlAdd(id) {
            const sym = $('wl-add').value;
            wlAction(() => api('POST', `/v1/watchlists/${id}/stocks`, { symbol: sym }), sym + ' added');
        }

        function wlRemove(id, sym) {
            wlAction(() => api('DELETE', `/v1/watchlists/${id}/stocks/${sym}`), sym + ' removed');
        }

        function newWatchlist() {
            openModal(`
                <div class="modal-head"><h3>New watchlist</h3><button class="btn btn-ghost btn-sm" onclick="closeModal()">&#10005;</button></div>
                <label for="wl-name">Name</label>
                <input id="wl-name" maxlength="30" placeholder="e.g. Long term">
                <div id="m-msg" class="form-msg err"></div>
                <button class="btn btn-primary btn-block" onclick="saveNewWatchlist()">Create</button>`, true);
        }

        async function saveNewWatchlist() {
            try {
                const r = await api('POST', '/v1/watchlists', { name: $('wl-name').value });
                S.wlActive = r.id;
                closeModal();
                toast('Watchlist created', 'ok');
                loadWatchlist();
            } catch (e) { $('m-msg').textContent = e.message; }
        }

        function renameWatchlist(id) {
            const current = S.wlNames[id] || '';
            openModal(`
                <div class="modal-head"><h3>Rename watchlist</h3><button class="btn btn-ghost btn-sm" onclick="closeModal()">&#10005;</button></div>
                <label for="wl-name">Name</label>
                <input id="wl-name" maxlength="30" value="${esc(current)}">
                <div id="m-msg" class="form-msg err"></div>
                <button class="btn btn-primary btn-block" onclick="saveRename(${id})">Save</button>`, true);
        }

        async function saveRename(id) {
            try {
                await api('PUT', '/v1/watchlists/' + id, { name: $('wl-name').value });
                closeModal();
                toast('Watchlist renamed', 'ok');
                loadWatchlist();
            } catch (e) { $('m-msg').textContent = e.message; }
        }

        function deleteWatchlist(id) {
            if (!confirm('Delete this watchlist?')) return;
            S.wlActive = null;
            wlAction(() => api('DELETE', '/v1/watchlists/' + id), 'Watchlist deleted');
        }


        async function loadOrders() {
            const d = await api('GET', '/v1/orders' + (S.orderFilter ? '?status=' + S.orderFilter : ''));
            if (S.page !== 'orders') return;
            const filters = [['', 'All'], ['OPEN', 'Open'], ['EXECUTED', 'Executed'], ['CANCELLED', 'Cancelled']];
            $('view').innerHTML = `
                <div class="page-head">
                    <h2>Orders</h2>
                    <div class="spacer"></div>
                    <button class="btn btn-primary" onclick="openStock('${(S.stocks[0] || {}).symbol || 'AAPL'}')">+ New order</button>
                </div>
                ${kycBanner()}
                <div class="wl-tabs">
                    ${filters.map(f => `<button class="tab ${S.orderFilter === f[0] ? 'active' : ''}" onclick="setOrderFilter('${f[0]}')">${f[1]}</button>`).join('')}
                </div>
                <div class="card">
                    ${d.orders.length ? `
                        <div class="table-wrap"><table>
                            <thead>
                                <tr>
                                    <th>#</th><th>Time</th><th>Stock</th><th>Side</th><th>Type</th>
                                    <th class="num">Qty</th><th class="num">Limit</th><th class="num">Exec price</th>
                                    <th>Status</th><th class="num">Action</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${d.orders.map(o => `
                                    <tr>
                                        <td>${o.id}</td>
                                        <td>${o.created}</td>
                                        <td><b>${o.symbol}</b></td>
                                        <td>${sideBadge(o.side)}</td>
                                        <td>${o.type}</td>
                                        <td class="num">${o.qty}</td>
                                        <td class="num">${o.limit_price ? money(o.limit_price) : '-'}</td>
                                        <td class="num">${o.exec_price ? money(o.exec_price) : '-'}</td>
                                        <td>${statusBadge(o.status)}</td>
                                        <td class="num">
                                            ${o.status === 'OPEN' ? `
                                                <button class="btn btn-sm" onclick="openModify(${o.id}, ${o.qty}, ${o.limit_price || 0})">Modify</button>
                                                <button class="btn btn-sm btn-red" onclick="cancelOrder(${o.id})">Cancel</button>` : ''}
                                        </td>
                                    </tr>`).join('')}
                            </tbody>
                        </table></div>`
                        : '<div class="empty">No orders match this filter. Place one from the Market page.</div>'}
                </div>`;
        }

        function setOrderFilter(f) { S.orderFilter = f; loadOrders(); }

        async function cancelOrder(id) {
            try {
                await api('DELETE', '/v1/orders/' + id);
                toast('Order #' + id + ' cancelled, funds released', 'ok');
                await refreshUser();
                loadOrders();
            } catch (e) { toast(e.message, 'err'); }
        }

        function openModify(id, qty, limit) {
            openModal(`
                <div class="modal-head"><h3>Modify order #${id}</h3><button class="btn btn-ghost btn-sm" onclick="closeModal()">&#10005;</button></div>
                <label for="mod-qty">Quantity</label>
                <input id="mod-qty" type="number" min="1" step="1" value="${qty}">
                <label for="mod-limit">Limit price</label>
                <input id="mod-limit" type="number" min="0.01" step="0.01" value="${limit}">
                <div id="m-msg" class="form-msg err"></div>
                <button class="btn btn-primary btn-block" onclick="saveModify(${id})">Update order</button>`, true);
        }

        async function saveModify(id) {
            try {
                await api('PUT', '/v1/orders/' + id, {
                    qty: parseInt($('mod-qty').value, 10),
                    limit_price: parseFloat($('mod-limit').value),
                });
                closeModal();
                toast('Order #' + id + ' updated', 'ok');
                await refreshUser();
                loadOrders();
            } catch (e) { $('m-msg').textContent = e.message; }
        }


        function openModal(html, small) {
            $('modal').className = small ? 'small' : '';
            $('modal').innerHTML = html;
            $('modal-backdrop').hidden = false;
            if (small) S.modalSym = null;
        }

        function closeModal() {
            $('modal-backdrop').hidden = true;
            $('modal').innerHTML = '';
            S.modalSym = null;
        }

        function chartSvg(points) {
            if (!points.length) return '';
            const w = 640, h = 220;
            const ys = points.map(p => p[1]);
            const min = Math.min.apply(null, ys), max = Math.max.apply(null, ys);
            const pad = (max - min) * 0.12 || 1;
            const lo = min - pad, hi = max + pad;
            const x = i => (i / (points.length - 1)) * w;
            const y = v => h - ((v - lo) / (hi - lo)) * h;
            const line = points.map((p, i) => x(i).toFixed(1) + ',' + y(p[1]).toFixed(1)).join(' ');
            const color = ys[ys.length - 1] >= ys[0] ? 'var(--green)' : 'var(--red)';
            return `
                <svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
                    <polygon points="0,${h} ${line} ${w},${h}" style="fill: ${color}; opacity: .14"></polygon>
                    <polyline points="${line}" style="fill: none; stroke: ${color}; stroke-width: 2" vector-effect="non-scaling-stroke"></polyline>
                </svg>
                <div class="chart-axis"><span>Low ${money(min)}</span><span>Live chart (last ${points.length} ticks)</span><span>High ${money(max)}</span></div>`;
        }

        async function refreshModalChart() {
            if (!S.modalSym || !$('m-chart')) return;
            try {
                const h = await api('GET', `/v1/stocks/${S.modalSym}/history?points=90`);
                if ($('m-chart')) $('m-chart').innerHTML = chartSvg(h.points);
            } catch (e) {  }
        }

        async function openStock(sym, side) {
            S.side = side || 'BUY';
            try {
                const [q, h] = await Promise.all([
                    api('GET', '/v1/stocks/' + sym),
                    api('GET', `/v1/stocks/${sym}/history?points=90`),
                ]);
                const options = S.stocks.map(s =>
                    `<option value="${s.symbol}" ${s.symbol === sym ? 'selected' : ''}>${s.symbol} - ${esc(s.name)}</option>`).join('');
                openModal(`
                    <div class="modal-head">
                        <div>
                            <select id="m-sym" style="width: auto; margin-bottom: 8px" onchange="openStock(this.value, S.side)">${options}</select>
                            <h3>${q.name}</h3>
                            <div style="color: var(--muted)">${q.sector}</div>
                        </div>
                        <div style="text-align: right">
                            <div class="price">${pxSpan(q.symbol, q.price)}</div>
                            <div>${chgSpan(q.symbol, q.change_pct)}</div>
                        </div>
                        <button class="btn btn-ghost btn-sm" onclick="closeModal()">&#10005;</button>
                    </div>
                    <div class="modal-body">
                        <div>
                            <div id="m-chart">${chartSvg(h.points)}</div>
                            <div class="stat-grid">
                                <div><small>Open</small>${money(q.open)}</div>
                                <div><small>Prev close</small>${money(q.prev_close)}</div>
                                <div><small>Volume</small>${num(q.volume)}</div>
                                <div><small>Day high</small>${money(q.high)}</div>
                                <div><small>Day low</small>${money(q.low)}</div>
                                <div><small>Sector</small>${q.sector}</div>
                            </div>
                            <button class="btn btn-ghost btn-sm" style="margin-top: 12px" onclick="quickWatch('${q.symbol}')">&#9734; Add to watchlist</button>
                        </div>
                        <div>
                            <div class="seg">
                                <button id="side-buy" class="seg-btn" onclick="setSide('BUY')">Buy</button>
                                <button id="side-sell" class="seg-btn" onclick="setSide('SELL')">Sell</button>
                            </div>
                            <label for="o-type">Order type</label>
                            <select id="o-type" onchange="setType()">
                                <option value="MARKET">Market (execute now)</option>
                                <option value="LIMIT">Limit (execute at your price)</option>
                            </select>
                            <label for="o-qty">Quantity</label>
                            <input id="o-qty" type="number" min="1" step="1" value="1" oninput="updateEst()">
                            <div id="o-limit-wrap" hidden>
                                <label for="o-limit">Limit price</label>
                                <input id="o-limit" type="number" min="0.01" step="0.01" value="${q.price}" oninput="updateEst()">
                            </div>
                            <div class="est">Estimated <b id="o-est">-</b><br>Available cash <b>${money(S.user.balance)}</b></div>
                            <div id="o-msg" class="form-msg err"></div>
                            <button id="o-submit" class="btn btn-green btn-block" style="margin-top: 4px" onclick="submitOrder()">Buy</button>
                        </div>
                    </div>`);
                S.modalSym = sym;
                setSide(S.side);
            } catch (e) { toast(e.message, 'err'); }
        }

        function setSide(side) {
            S.side = side;
            $('side-buy').className = 'seg-btn' + (side === 'BUY' ? ' buy-on' : '');
            $('side-sell').className = 'seg-btn' + (side === 'SELL' ? ' sell-on' : '');
            const btn = $('o-submit');
            btn.className = 'btn btn-block ' + (side === 'BUY' ? 'btn-green' : 'btn-red');
            btn.textContent = (side === 'BUY' ? 'Buy ' : 'Sell ') + S.modalSym;
            updateEst();
        }

        function setType() {
            $('o-limit-wrap').hidden = $('o-type').value !== 'LIMIT';
            updateEst();
        }

        function updateEst() {
            if (!S.modalSym || !$('o-est')) return;
            const qty = parseInt($('o-qty').value, 10) || 0;
            const live = (S.live[S.modalSym] || {}).price || 0;
            const price = $('o-type').value === 'LIMIT' ? (parseFloat($('o-limit').value) || 0) : live;
            $('o-est').textContent = money(qty * price);
        }

        async function submitOrder() {
            const payload = {
                symbol: S.modalSym,
                side: S.side,
                type: $('o-type').value,
                qty: parseInt($('o-qty').value, 10),
            };
            if (payload.type === 'LIMIT') payload.limit_price = parseFloat($('o-limit').value);
            try {
                const r = await api('POST', '/v1/orders', payload);
                const o = r.order;
                closeModal();
                if (o.status === 'EXECUTED') {
                    toast(`${o.side} ${o.qty} ${o.symbol} executed at ${money(o.exec_price)}`, 'ok');
                } else {
                    toast(`Limit order #${o.id} placed. It executes when the price is reached.`, 'ok');
                }
                await refreshUser();
                if (AUTO_REFRESH.indexOf(S.page) !== -1) LOADERS[S.page]();
            } catch (e) {
                $('o-msg').textContent = e.message;
            }
        }


        async function loadPortfolio() {
            const [p, t] = await Promise.all([
                api('GET', '/v1/api/portfolio/' + S.user.id),
                api('GET', '/v1/trades'),
            ]);
            if (S.page !== 'portfolio') return;
            $('view').innerHTML = `
                <div class="page-head">
                    <h2>Portfolio</h2>
                </div>
                <div class="grid grid-4">
                    <div class="card stat"><div class="label">Invested</div><div class="value">${money(p.invested)}</div></div>
                    <div class="card stat"><div class="label">Current value</div><div class="value">${money(p.current_value)}</div></div>
                    <div class="card stat">
                        <div class="label">Unrealized P&amp;L</div>
                        <div class="value ${cls(p.unrealized_pnl)}">${smoney(p.unrealized_pnl)}</div>
                        <div class="sub">${spct(p.unrealized_pct)}</div>
                    </div>
                    <div class="card stat">
                        <div class="label">Realized P&amp;L</div>
                        <div class="value ${cls(p.realized_pnl)}">${smoney(p.realized_pnl)}</div>
                        <div class="sub">Total ${smoney(p.total_pnl)}</div>
                    </div>
                </div>

                <div class="card">
                    <h3>Holdings</h3>
                    ${p.holdings.length ? `
                        <div class="table-wrap"><table>
                            <thead>
                                <tr>
                                    <th>Stock</th><th class="num">Qty</th><th class="num">Avg price</th>
                                    <th class="num">LTP</th><th class="num">Day</th><th class="num">Value</th>
                                    <th class="num">P&amp;L</th><th class="num">Action</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${p.holdings.map(h => `
                                    <tr>
                                        <td class="sym" onclick="openStock('${h.symbol}')">${h.symbol}<small>${esc(h.name)}</small></td>
                                        <td class="num">${h.qty}${h.locked ? `<small style="color: var(--amber)"> (${h.locked} locked)</small>` : ''}</td>
                                        <td class="num">${money(h.avg_price)}</td>
                                        <td class="num">${pxSpan(h.symbol, h.ltp)}</td>
                                        <td class="num">${chgSpan(h.symbol, h.change_pct)}</td>
                                        <td class="num">${money(h.value)}</td>
                                        <td class="num ${cls(h.pnl)}">${smoney(h.pnl)} (${spct(h.pnl_pct)})</td>
                                        <td class="num">
                                            <button class="btn btn-sm btn-green" onclick="openStock('${h.symbol}', 'BUY')">Buy</button>
                                            <button class="btn btn-sm btn-red" onclick="openStock('${h.symbol}', 'SELL')">Sell</button>
                                        </td>
                                    </tr>`).join('')}
                            </tbody>
                        </table></div>`
                        : '<div class="empty">You do not hold any stocks yet.</div>'}
                </div>

                <div class="grid grid-2">
                    <div class="card">
                        <h3>Sector allocation</h3>
                        ${p.allocation.length ? p.allocation.map(a => `
                            <div class="bar-row">
                                <div class="name">${a.sector}</div>
                                <div class="bar"><i style="width: ${a.pct}%"></i></div>
                                <div>${a.pct}%</div>
                            </div>`).join('') : '<div class="empty">Allocation appears after your first purchase.</div>'}
                    </div>
                    <div class="card">
                        <h3>Trade history</h3>
                        ${t.trades.length ? `
                            <div class="table-wrap"><table>
                                <thead><tr><th>Time</th><th>Stock</th><th>Side</th><th class="num">Qty</th><th class="num">Price</th><th class="num">P&amp;L</th></tr></thead>
                                <tbody>
                                    ${t.trades.slice(0, 12).map(x => `
                                        <tr>
                                            <td>${x.ts}</td><td>${x.symbol}</td><td>${sideBadge(x.side)}</td>
                                            <td class="num">${x.qty}</td><td class="num">${money(x.price)}</td>
                                            <td class="num ${cls(x.pnl)}">${x.side === 'SELL' ? smoney(x.pnl) : '-'}</td>
                                        </tr>`).join('')}
                                </tbody>
                            </table></div>`
                            : '<div class="empty">No trades yet.</div>'}
                    </div>
                </div>`;
        }


        async function loadWallet() {
            const d = await api('GET', '/v1/payments');
            if (S.page !== 'wallet') return;
            const u = S.user;
            $('view').innerHTML = `
                <div class="page-head"><h2>Wallet</h2></div>
                ${kycBanner()}
                <div class="grid grid-3">
                    <div class="card stat">
                        <div class="label">Available balance</div>
                        <div class="value">${money(u.balance)}</div>
                        <div class="sub">Bank: ${u.bank_name ? esc(u.bank_name) + ' ' + esc(u.bank_account) : 'not linked'}</div>
                    </div>
                    <div class="card">
                        <h3>Add funds</h3>
                        <label for="p-add">Amount (USD)</label>
                        <input id="p-add" type="number" min="1" step="0.01" placeholder="1000">
                        <div class="chips">
                            ${[500, 1000, 5000, 10000].map(a => `<button class="btn btn-sm btn-ghost" onclick="$('p-add').value = ${a}">+${num(a)}</button>`).join('')}
                        </div>
                        <label for="p-method">Method</label>
                        <select id="p-method">
                            <option value="CARD">Debit / credit card</option>
                            <option value="BANK">Bank transfer</option>
                            <option value="WALLET">Digital wallet</option>
                        </select>
                        <button class="btn btn-green btn-block" onclick="addFunds()">Add funds</button>
                    </div>
                    <div class="card">
                        <h3>Withdraw funds</h3>
                        <label for="p-wd">Amount (USD)</label>
                        <input id="p-wd" type="number" min="1" step="0.01" placeholder="500">
                        <div class="est">Payouts go to your linked bank account.</div>
                        <button class="btn btn-primary btn-block" onclick="withdrawFunds()">Withdraw</button>
                    </div>
                </div>

                <div class="card">
                    <h3>Payment history</h3>
                    ${d.payments.length ? `
                        <div class="table-wrap"><table>
                            <thead>
                                <tr><th>#</th><th>Time</th><th>Type</th><th>Method</th><th class="num">Amount</th><th>Status</th><th>Reference</th><th class="num">Action</th></tr>
                            </thead>
                            <tbody>
                                ${d.payments.map(p => `
                                    <tr>
                                        <td>${p.id}</td>
                                        <td>${p.ts}</td>
                                        <td>${p.ptype}</td>
                                        <td>${p.method}</td>
                                        <td class="num ${p.ptype === 'WITHDRAW' ? 'down' : 'up'}">${p.ptype === 'WITHDRAW' ? '-' : '+'}${money(p.amount)}</td>
                                        <td>${statusBadge(p.status)}</td>
                                        <td class="mono">${p.ref}</td>
                                        <td class="num">
                                            ${p.ptype === 'ADD' && p.status === 'SUCCESS' ? `<button class="btn btn-sm" onclick="refundPayment(${p.id})">Refund</button>` : ''}
                                        </td>
                                    </tr>`).join('')}
                            </tbody>
                        </table></div>`
                        : '<div class="empty">No deposits or withdrawals yet. Add funds to start trading.</div>'}
                </div>`;
        }

        async function walletCall(fn, okMsg) {
            try {
                await fn();
                toast(okMsg, 'ok');
                await refreshUser();
                loadWallet();
            } catch (e) { toast(e.message, 'err'); }
        }

        function addFunds() {
            walletCall(() => api('POST', '/v1/payments/add', {
                amount: parseFloat($('p-add').value),
                method: $('p-method').value,
            }), 'Funds added successfully');
        }

        function withdrawFunds() {
            walletCall(() => api('POST', '/v1/payments/withdraw', { amount: parseFloat($('p-wd').value) }),
                'Withdrawal sent to your bank');
        }

        function refundPayment(id) {
            if (!confirm('Refund this deposit?')) return;
            walletCall(() => api('POST', `/v1/payments/${id}/refund`, {}), 'Refund processed');
        }


        async function loadProfile() {
            await refreshUser();
            if (S.page !== 'profile') return;
            const u = S.user;
            const prefs = u.prefs || {};
            $('view').innerHTML = `
                <div class="page-head"><h2>Profile</h2></div>
                <div class="grid grid-2">
                    <div class="card">
                        <h3>Personal details</h3>
                        <label>User ID</label>
                        <input value="${esc(u.username)}" disabled>
                        <label>Email</label>
                        <input value="${esc(u.email)}" disabled>
                        <label for="pf-name">Full name</label>
                        <input id="pf-name" value="${esc(u.full_name)}">
                        <label for="pf-phone">Phone</label>
                        <input id="pf-phone" value="${esc(u.phone)}" placeholder="+977 98XXXXXXXX">
                        <button class="btn btn-primary btn-block" onclick="saveProfile()">Save profile</button>
                    </div>

                    <div class="card">
                        <h3>KYC verification ${statusBadge(u.kyc_status)}</h3>
                        ${u.kyc_status === 'VERIFIED'
                            ? `<p style="color: var(--muted)">Your identity document <b>${esc(u.kyc_doc)}</b> is verified. Trading and payments are enabled.</p>`
                            : `<label for="kyc-type">Document type</label>
                               <select id="kyc-type">
                                   <option value="NATIONAL_ID">National ID</option>
                                   <option value="PASSPORT">Passport</option>
                                   <option value="DRIVING_LICENSE">Driving license</option>
                               </select>
                               <label for="kyc-num">Document number</label>
                               <input id="kyc-num" placeholder="6-20 letters or digits">
                               <button class="btn btn-green btn-block" onclick="submitKyc()">Verify identity</button>`}
                    </div>

                    <div class="card">
                        <h3>Bank details</h3>
                        <label for="bk-name">Bank name</label>
                        <input id="bk-name" value="${esc(u.bank_name)}" placeholder="Your bank">
                        <label for="bk-acc">Account number</label>
                        <input id="bk-acc" placeholder="${u.bank_account ? esc(u.bank_account) : '8-18 digits'}">
                        <button class="btn btn-primary btn-block" onclick="saveBank()">Save bank details</button>
                    </div>

                    <div class="card">
                        <h3>Preferences</h3>
                        <label for="pr-theme">Theme</label>
                        <select id="pr-theme">
                            <option value="dark" ${prefs.theme !== 'light' ? 'selected' : ''}>Dark</option>
                            <option value="light" ${prefs.theme === 'light' ? 'selected' : ''}>Light</option>
                        </select>
                        <label for="pr-refresh">Live price refresh</label>
                        <select id="pr-refresh">
                            ${[1, 2, 5, 10].map(s => `<option value="${s}" ${(prefs.refresh || 2) === s ? 'selected' : ''}>Every ${s} second(s)</option>`).join('')}
                        </select>
                        <button class="btn btn-primary btn-block" onclick="savePrefs()">Save preferences</button>
                    </div>
                </div>`;
        }

        async function profileCall(fn, okMsg) {
            try {
                const r = await fn();
                if (r && r.user) applyUser(r.user);
                toast(okMsg, 'ok');
                loadProfile();
            } catch (e) { toast(e.message, 'err'); }
        }

        function saveProfile() {
            profileCall(() => api('PUT', '/v1/users/me', { full_name: $('pf-name').value, phone: $('pf-phone').value }), 'Profile saved');
        }

        function submitKyc() {
            profileCall(() => api('POST', '/v1/users/kyc', { doc_type: $('kyc-type').value, doc_number: $('kyc-num').value }), 'KYC verified. You can trade now!');
        }

        function saveBank() {
            profileCall(() => api('PUT', '/v1/users/bank', { bank_name: $('bk-name').value, account_number: $('bk-acc').value }), 'Bank details saved');
        }

        async function savePrefs() {
            await profileCall(() => api('PUT', '/v1/users/preferences', {
                theme: $('pr-theme').value,
                refresh: parseInt($('pr-refresh').value, 10),
            }), 'Preferences saved');
            startPolling();
        }


        const API_DOCS = [
            ['POST', '/v1/auth/login', 'Login with User ID and password, returns a bearer token'],
            ['POST', '/v1/auth/register', 'Create an account'],
            ['GET', '/v1/stocks?page=1&page_size=10&q=', 'List stocks with pagination and search'],
            ['GET', '/v1/stocks/{stockID}', 'Current price and day statistics'],
            ['GET', '/v1/stocks/{stockID}/history', 'Historical price points'],
            ['POST', '/v1/orders', 'Place BUY / SELL, MARKET / LIMIT order'],
            ['PUT / DELETE', '/v1/orders/{orderID}', 'Modify or cancel an open order'],
            ['GET', '/v1/api/portfolio/{userID}', 'Holdings, positions and P&L'],
            ['GET', '/v1/api/watchlist', 'Watchlists with live quotes'],
            ['POST', '/v1/payments/add', 'Add funds through the payment gateway'],
        ];

        async function loadSystem() {
            const d = await api('GET', '/v1/system/overview');
            if (S.page !== 'system') return;
            const topicName = { 'order-events': 'Order events', 'trade-events': 'Trade events', 'price-updates': 'Price updates', 'user-activity': 'User activity events' };
            $('view').innerHTML = `
                <div class="page-head">
                    <h2>System</h2>
                </div>

                <div class="card">
                    <h3>Request path (high level design)</h3>
                    <div class="flow">
                        <span class="node">User (Web / Mobile / API)</span> &rarr;
                        <span class="node">API Gateway</span> &rarr;
                        <span class="node">User &middot; Price Tracker &middot; Watchlist &middot; Order &middot; Portfolio &middot; Payment</span> &rarr;
                        <span class="node">Kafka</span> &harr;
                        <span class="node">Exchange Gateway</span>,
                        <span class="node">Primary DB</span>,
                        <span class="node">External services</span>
                    </div>
                    <div class="chips">
                        <span class="badge blue">Active sessions: ${d.active_sessions}</span>
                        ${Object.keys(d.db_rows).map(k => `<span class="badge">${k}: ${d.db_rows[k]}</span>`).join('')}
                    </div>
                </div>

                <div class="grid grid-2">
                    ${Object.keys(d.topics).map(name => `
                        <div class="card">
                            <h3>Kafka &middot; ${topicName[name]} <span class="badge">${name}</span> <span class="badge blue">offset ${d.topics[name].offset}</span></h3>
                            ${d.topics[name].events.length ? d.topics[name].events.map(e => `
                                <div class="event"><b>#${e.offset}</b> ${e.ts}<br>${esc(JSON.stringify(e.event))}</div>`).join('')
                                : '<div class="empty">No events on this topic yet.</div>'}
                        </div>`).join('')}
                </div>

                <div class="grid grid-2">
                    <div class="card">
                        <h3>Notifications (Email / SMS service)</h3>
                        ${d.notifications.length ? d.notifications.map(n => `
                            <div class="event"><span class="badge ${n.channel === 'SMS' ? 'amber' : 'blue'}">${n.channel}</span> ${n.ts}<br>${esc(n.message)}</div>`).join('')
                            : '<div class="empty">No notifications yet. Deposits, trades and KYC events appear here.</div>'}
                    </div>
                    <div class="card">
                        <h3>API gateway log (auth, rate limiting, routing)</h3>
                        <div class="table-wrap"><table>
                            <thead><tr><th>Method</th><th>Path</th><th>Service</th><th class="num">Status</th><th class="num">ms</th></tr></thead>
                            <tbody>
                                ${d.gateway_log.map(l => `
                                    <tr>
                                        <td>${l.method}</td>
                                        <td class="mono">${esc(l.path)}</td>
                                        <td>${l.service}</td>
                                        <td class="num ${l.status < 400 ? 'up' : 'down'}">${l.status}</td>
                                        <td class="num">${l.ms}</td>
                                    </tr>`).join('')}
                            </tbody>
                        </table></div>
                    </div>
                </div>

                <div class="card">
                    <h3>API reference</h3>
                    <div class="table-wrap"><table>
                        <tbody>
                            ${API_DOCS.map(a => `<tr><td><span class="badge blue">${a[0]}</span></td><td class="mono">${esc(a[1])}</td><td>${esc(a[2])}</td></tr>`).join('')}
                        </tbody>
                    </table></div>
                </div>`;
        }


        (async function boot() {
            if (S.token) {
                try {
                    const r = await api('GET', '/v1/users/me');
                    await startSession(S.token, r.user);
                    return;
                } catch (e) {  }
            }
            $('auth-screen').hidden = false;
        })();
    </script>
</body>

</html>
'''


_SERVER = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_SERVER.serve_forever, daemon=True).start()
threading.Thread(target=engine_loop, args=(_STOP,), daemon=True).start()
print("TradeNova is running on port %d  (register an account, complete KYC, add funds, trade)" % PORT)

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=950)
except ImportError:
    print("Not running in Colab - open http://127.0.0.1:%d in your browser" % PORT)