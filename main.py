import os, re, time, threading
from collections import defaultdict, deque
import msgpack, requests, websocket
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="public", static_url_path="")
TOTAL_TIMEOUT = 8.0
CACHE_TTL = 60
RATE_LIMIT, RATE_WINDOW = 10, 60  # requests per IP per window
NAME_RE = re.compile(r"^[A-Za-z0-9_ .\-]{1,32}$")
REGION_RE = re.compile(r"^[a-z0-9]+$")
HOST_RE = re.compile(r"^[A-Za-z0-9.\-:]+$")
OP_PING, OP_ROOM_DATA = 9, 13  # 10 (JOIN_ROOM) is also answered with [10] per the game flow

_cache, _hits, _lock = {}, defaultdict(deque), threading.Lock()


class LookupError_(Exception):
    def __init__(self, status, msg):
        super().__init__(msg)
        self.status, self.msg = status, msg


def lookup_player(username):
    deadline = time.monotonic() + TOTAL_TIMEOUT

    def left():
        r = deadline - time.monotonic()
        if r <= 0:
            raise LookupError_(504, "Lookup timed out. Try again.")
        return r

    try:
        # 1. guest token + region
        r = requests.post("https://loadbalancer.swordmasters.io/api/server/findServer",
                          json={}, timeout=left())
        r.raise_for_status()
        j = r.json()
        token = j.get("token") or j.get("accessToken")
        region = str(j.get("foundServer", ""))
        if not token or not REGION_RE.match(region):
            raise LookupError_(502, "Unexpected response from game load balancer.")

        # 2. seat reservation
        r = requests.post(f"https://{region}.swordmasters.io/matchmake/joinOrCreate/world_1",
                          json={"accessToken": token}, timeout=left())
        r.raise_for_status()
        m = r.json()
        room = m.get("room", {})
        session = m.get("sessionId")
        addr = m.get("publicAddress") or room.get("publicAddress")
        pid = m.get("processId") or room.get("processId")
        rid = m.get("roomId") or room.get("roomId")
        if not all([session, addr, pid, rid]) or not HOST_RE.match(str(addr)):
            raise LookupError_(502, "Unexpected matchmaking response.")

        # 3. websocket
        ws = websocket.create_connection(
            f"wss://{addr}/{pid}/{rid}?sessionId={session}",
            header=["Origin: https://swordmasters.io"], origin=None, timeout=left())
    except LookupError_:
        raise
    except (requests.Timeout, websocket.WebSocketTimeoutException):
        raise LookupError_(504, "Game servers took too long to respond.")
    except Exception as e:
        raise LookupError_(502, f"Could not reach game servers ({type(e).__name__}).")

    try:
        # 4. handshake -> keepalive -> request
        first = ws.recv()
        if isinstance(first, bytes) and first[:1] == bytes([10]):
            ws.send_binary(bytes([10]))
        ws.send_binary(bytes([OP_ROOM_DATA]) + msgpack.packb("Client:SkinStatue:GetPlayerInfo")
                       + msgpack.packb({"username": username}))

        # 5. read loop
        while True:
            ws.settimeout(left())
            data = ws.recv()
            if not isinstance(data, (bytes, bytearray)) or not data:
                continue
            op = data[0]
            if op in (10, OP_PING):
                ws.send_binary(bytes([op]))
            elif op == OP_ROOM_DATA:
                try:
                    u = msgpack.Unpacker(raw=False)
                    u.feed(bytes(data[1:]))
                    mtype, payload = next(u), next(u)
                except Exception:
                    continue
                if mtype == "Server:SkinStatue:GetPlayerInfo":
                    inv = payload.get("inv") if isinstance(payload, dict) else None
                    if not inv:
                        raise LookupError_(404, f"Player '{username}' was not found.")
                    return inv
    except LookupError_:
        raise
    except websocket.WebSocketTimeoutException:
        raise LookupError_(504, "Timed out waiting for player data.")
    except Exception as e:
        raise LookupError_(502, f"Connection to game server failed ({type(e).__name__}).")
    finally:
        try: ws.close()
        except Exception: pass


def rate_limited(ip):
    now = time.monotonic()
    with _lock:
        q = _hits[ip]
        while q and now - q[0] > RATE_WINDOW:
            q.popleft()
        if len(q) >= RATE_LIMIT:
            return True
        q.append(now)
        return False


@app.get("/")
def index():
    return send_from_directory("public", "index.html")


@app.get("/api/lookup")
def api_lookup():
    username = (request.args.get("username") or "").strip()
    if not username:
        return jsonify(success=False, error="Missing 'username' parameter."), 400
    if not NAME_RE.match(username):
        return jsonify(success=False, error="Invalid username format."), 400
    key = username.lower()
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return jsonify(success=True, data=hit[1], cached=True)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
    if rate_limited(ip):
        return jsonify(success=False, error="Too many lookups. Wait a minute."), 429
    try:
        inv = lookup_player(username)
    except LookupError_ as e:
        return jsonify(success=False, error=e.msg), e.status
    _cache[key] = (time.time(), inv)
    return jsonify(success=True, data=inv)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
