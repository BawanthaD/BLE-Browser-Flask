import asyncio, threading, time, json, re
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, field

from flask import Flask, jsonify, request, render_template
from flask_sock import Sock  # pip install flask-sock
from bleak import BleakScanner, BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic
import os, json, re


# ---------- Async bridge (same concept as your Tk app) ----------
class AsyncioBridge:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self):
        try:
            if self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)
        finally:
            if self.thread.is_alive():
                self.thread.join(timeout=1.0)

# ---------- BLE Manager ----------
@dataclass
class CharInfo:
    service_uuid: str
    handle: int
    props: List[str]
    description: str

@dataclass
class BLEManager:
    bridge: AsyncioBridge
    client: Optional[BleakClient] = None
    connected_address: Optional[str] = None
    char_index: Dict[str, CharInfo] = field(default_factory=dict)
    notify_active_uuid: Optional[str] = None

    # subscribers (WebSocket) for logs and notifications
    sockets: List = field(default_factory=list)

    def _broadcast(self, payload: dict):
        txt = json.dumps(payload)
        dead = []
        for ws in self.sockets:
            try:
                ws.send(txt)
            except Exception:
                dead.append(ws)
        # cleanup dead sockets
        for d in dead:
            try: self.sockets.remove(d)
            except ValueError:
                pass

    # ----------------- Public API -----------------
    def scan(self, timeout=5.0):
        fut = self.bridge.run(self._scan_async(timeout))
        return fut.result()

    async def _scan_async(self, timeout):
        devs = await BleakScanner.discover(timeout=timeout)
        out = []
        for d in devs:
            name = (d.name or "").strip() or "(Unknown)"
            addr = getattr(d, "address", getattr(d, "mac_address", ""))
            rssi = getattr(d, "rssi", None)
            out.append({"name": name, "address": addr, "rssi": rssi})
        return out

    def connect(self, address: str):
        fut = self.bridge.run(self._connect_and_list(address))
        return fut.result()

    async def _connect_and_list(self, address: str):
        # reset state
        self.char_index.clear()
        try:
            if self.client and getattr(self.client, "is_connected", False):
                try:
                    await self.client.disconnect()
                except Exception:
                    pass
            self.client = BleakClient(address)
            await self.client.connect(timeout=10.0)
            ok = bool(getattr(self.client, "is_connected", False))
            self.connected_address = address if ok else None

            if not ok:
                return {"connected": False, "error": "Failed to connect"}

            # disconnect callback
            try:
                self.client.set_disconnected_callback(self._on_disconnected)
            except Exception:
                pass

            # services/characteristics
            services = getattr(self.client, "services", None)
            if not services or len(list(services)) == 0:
                get_services = getattr(self.client, "get_services", None)
                if callable(get_services):
                    services = await get_services()
            if not services:
                return {"connected": True, "characteristics": []}

            chars = []
            for svc in services:
                for ch in svc.characteristics:
                    uuid = str(ch.uuid)
                    props = list(getattr(ch, "properties", []))
                    desc = getattr(ch, "description", "")
                    self.char_index[uuid] = CharInfo(str(svc.uuid), ch.handle, props, desc)
                    chars.append({
                        "uuid": uuid,
                        "service_uuid": str(svc.uuid),
                        "props": props,
                        "description": desc
                    })
            self._broadcast({"type": "status", "message": f"Connected to {address}"})
            return {"connected": True, "characteristics": chars}

        except Exception as exc:
            self.connected_address = None
            return {"connected": False, "error": str(exc)}

    def disconnect(self):
        fut = self.bridge.run(self._disconnect_async())
        fut.result()
        return {"connected": False}

    async def _disconnect_async(self):
        try:
            if self.notify_active_uuid:
                try:
                    info = self.char_index.get(self.notify_active_uuid)
                    if info:
                        await self.client.stop_notify(info.handle)
                except Exception:
                    pass
                self.notify_active_uuid = None
            if self.client:
                await self.client.disconnect()
        except Exception:
            pass
        finally:
            self.connected_address = None
            self._broadcast({"type": "status", "message": "Disconnected"})

    def list_chars(self):
        out = []
        for uuid, info in self.char_index.items():
            out.append({
                "uuid": uuid,
                "service_uuid": info.service_uuid,
                "props": info.props,
                "description": info.description,
            })
        return out

    def read(self, uuid: str):
        fut = self.bridge.run(self._read_async(uuid))
        return fut.result()

    async def _read_async(self, uuid: str):
        if not (self.client and getattr(self.client, "is_connected", False)):
            return {"ok": False, "error": "Not connected"}
        try:
            data = await self.client.read_gatt_char(uuid)
            hex_str = " ".join(f"{b:02X}" for b in data)
            self._broadcast({"type": "log", "line": f"[READ {uuid}] {hex_str} (len={len(data)})"})
            return {"ok": True, "data": list(bytearray(data))}
        except Exception as exc:
            self._broadcast({"type": "log", "line": f"[READ {uuid}] Failed: {exc}"})
            return {"ok": False, "error": str(exc)}

    def write(self, uuid: str, bytes_list: List[int]):
        fut = self.bridge.run(self._write_async(uuid, bytes(bytes_list or [])))
        return fut.result()

    async def _write_async(self, uuid: str, payload: bytes):
        if not (self.client and getattr(self.client, "is_connected", False)):
            return {"ok": False, "error": "Not connected"}
        try:
            info = self.char_index.get(uuid)
            # pick response flag based on props
            response = True
            if info and ("write-without-response" in info.props and "write" not in info.props):
                response = False
            await self.client.write_gatt_char(uuid, payload, response=response)
            shown = " ".join(f"{b:02X}" for b in payload)
            self._broadcast({"type": "log", "line": f"[WRITE {uuid}] {shown}"})
            return {"ok": True}
        except Exception as exc:
            self._broadcast({"type": "log", "line": f"[WRITE {uuid}] Failed: {exc}"})
            return {"ok": False, "error": str(exc)}

    def toggle_notify(self, uuid: str):
        if self.notify_active_uuid == uuid:
            fut = self.bridge.run(self._stop_notify_async(uuid))
            fut.result()
            return {"ok": True, "subscribed": False}
        else:
            fut = self.bridge.run(self._start_notify_async(uuid))
            return fut.result()

    async def _start_notify_async(self, uuid: str):
        if not (self.client and getattr(self.client, "is_connected", False)):
            return {"ok": False, "error": "Not connected"}
        try:
            info = self.char_index.get(uuid)
            if not info:
                return {"ok": False, "error": "Characteristic not found"}
            await self.client.start_notify(info.handle, self._notification_handler)
            self.notify_active_uuid = uuid
            self._broadcast({"type": "log", "line": f"[NOTIFY {uuid}] Subscribed (handle {info.handle})"})
            return {"ok": True, "subscribed": True}
        except Exception as exc:
            self._broadcast({"type": "log", "line": f"[NOTIFY {uuid}] Failed to subscribe: {exc}"})
            return {"ok": False, "error": str(exc)}

    async def _stop_notify_async(self, uuid: str):
        try:
            info = self.char_index.get(uuid)
            if info and self.client:
                await self.client.stop_notify(info.handle)
            self._broadcast({"type": "log", "line": f"[NOTIFY {uuid}] Unsubscribed"})
        except Exception as exc:
            self._broadcast({"type": "log", "line": f"[NOTIFY {uuid}] Failed to unsubscribe: {exc}"})
        finally:
            if self.notify_active_uuid == uuid:
                self.notify_active_uuid = None

    # ---------- Callbacks ----------
    def _notification_handler(self, sender, data: bytearray):
        if isinstance(sender, BleakGATTCharacteristic):
            uuid = str(sender.uuid)
        else:
            # sender is handle
            uuid = next((u for u, info in self.char_index.items() if info.handle == sender), "Unknown")

        b = bytes(data)
        hex_str = " ".join(f"{x:02X}" for x in b)

        # existing broadcasts
        self._broadcast({
            "type": "notify",
            "uuid": uuid,
            "data": list(b),
            "hex": hex_str,
            "length": len(b)
        })
        self._broadcast({"type": "log", "line": f"[NOTIF {uuid}] {hex_str} (len={len(b)})"})

        # >>> add this block exactly here <<<
        computed = self._compute_values(uuid, b)
        if computed:
            self._broadcast({
                "type": "computed",
                "uuid": uuid,
                "values": computed
            })


    def _on_disconnected(self, _client):
        self.connected_address = None
        self.notify_active_uuid = None
        self._broadcast({"type": "status", "message": "Disconnected"})

        # ---------- Config (per characteristic UUID) ----------
    def _config_path(self, uuid: str) -> str:
        safe = re.sub(r'[^a-zA-Z0-9_\-]+', '_', uuid or 'unknown')
        return os.path.join(CONFIG_DIR, f"ble_{safe}.json")

    def load_formulas(self, uuid: str):
        try:
            with open(self._config_path(uuid), "r", encoding="utf-8") as f:
                data = json.load(f)
            # Expect {"uuid": "...", "formulas": ["name = expr", ...]}
            if isinstance(data, dict) and isinstance(data.get("formulas"), list):
                return data["formulas"]
        except Exception:
            pass
        return []

    def save_formulas(self, uuid: str, formulas):
        data = {"uuid": uuid, "formulas": list(formulas or [])}
        with open(self._config_path(uuid), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return {"ok": True}

    def _compute_values(self, uuid: str, payload: bytes):
        formulas = self.load_formulas(uuid)
        if not formulas:
            return None  # nothing to compute

        b = bytes(payload)
        env = {"__builtins__": {}}

        # Provide helpers and data
        env_locals = {
            "b": list(b),                 # list of ints
            "len": len,                   # len(b)
            "u16le": lambda i: (b[i] | (b[i+1] << 8)) if i+1 < len(b) else None,
            "u16be": lambda i: ((b[i] << 8) | b[i+1]) if i+1 < len(b) else None,
            "u32le": lambda i: (b[i] | (b[i+1]<<8) | (b[i+2]<<16) | (b[i+3]<<24)) if i+3 < len(b) else None,
            "u32be": lambda i: ((b[i]<<24) | (b[i+1]<<16) | (b[i+2]<<8) | b[i+3]) if i+3 < len(b) else None,
        }
        # byte aliases: b0,b1,... for convenience
        for i, val in enumerate(b):
            env_locals[f"b{i}"] = val

        results = {}
        for line in formulas:
            if not isinstance(line, str): 
                continue
            ln = line.strip()
            if not ln or ln.startswith("#"):
                continue
            if "=" not in ln:
                # allow bare expressions with auto name exprN
                name = f"expr{len(results)+1}"
                expr = ln
            else:
                name, expr = ln.split("=", 1)
                name = name.strip()
                expr = expr.strip()
                if not name:
                    name = f"expr{len(results)+1}"
            try:
                # Safe eval with restricted env + our locals
                val = eval(expr, env, env_locals)
            except Exception as e:
                val = f"ERR: {e.__class__.__name__}"
            results[name] = val
        return results


# ---------- Flask app ----------
app = Flask(__name__)
sock = Sock(app)

bridge = AsyncioBridge()
ble = BLEManager(bridge=bridge)
CONFIG_DIR = os.path.join(os.getcwd(), "configs")
os.makedirs(CONFIG_DIR, exist_ok=True)


@app.route("/")
def index():
    return render_template("index.html")

# ---- REST API ----
@app.route("/api/scan", methods=["POST"])
def api_scan():
    timeout = float(request.json.get("timeout", 5.0)) if request.is_json else 5.0
    devices = ble.scan(timeout=timeout)
    return jsonify({"devices": devices})

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.get_json(force=True)
    address = data.get("address","")
    res = ble.connect(address)
    return jsonify(res)

@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    res = ble.disconnect()
    return jsonify(res)

@app.route("/api/chars", methods=["GET"])
def api_chars():
    return jsonify({"characteristics": ble.list_chars(), "connected": bool(ble.connected_address), "address": ble.connected_address})

@app.route("/api/read", methods=["POST"])
def api_read():
    data = request.get_json(force=True)
    uuid = data.get("uuid","")
    return jsonify(ble.read(uuid))

@app.route("/api/write", methods=["POST"])
def api_write():
    data = request.get_json(force=True)
    uuid = data.get("uuid","")
    bytes_list = data.get("bytes", [])
    return jsonify(ble.write(uuid, bytes_list))

@app.route("/api/notify", methods=["POST"])
def api_notify_toggle():
    data = request.get_json(force=True)
    uuid = data.get("uuid","")
    return jsonify(ble.toggle_notify(uuid))

@app.route("/api/config/get", methods=["POST"])
def api_config_get():
    data = request.get_json(force=True)
    uuid = data.get("uuid", "")
    formulas = ble.load_formulas(uuid)
    return jsonify({"uuid": uuid, "formulas": formulas})

@app.route("/api/config/save", methods=["POST"])
def api_config_save():
    data = request.get_json(force=True)
    uuid = data.get("uuid", "")
    formulas = data.get("formulas", [])
    res = ble.save_formulas(uuid, formulas)
    return jsonify(res)


# ---- WebSocket for logs & notifications ----
@sock.route("/ws")
def ws(ws):
    # register
    ble.sockets.append(ws)
    try:
        # send initial status
        ws.send(json.dumps({"type":"hello","message":"connected"}))
        # keep open; we don't expect messages from client in this simple design
        while True:
            # block waiting for client pings or close; ignore content
            data = ws.receive()
            if data is None:
                break
    except Exception:
        pass
    finally:
        try:
            ble.sockets.remove(ws)
        except Exception:
            pass

# ---- Graceful stop (optional) ----
import atexit
@atexit.register
def _shutdown():
    try:
        ble.disconnect()
    except Exception:
        pass
    try:
        bridge.stop()
    except Exception:
        pass

if __name__ == "__main__":
    # Run:  flask --app app.py run  (or)  python app.py
    # Flask dev server + flask-sock uses simple-websocket (threading), OK on Windows
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
