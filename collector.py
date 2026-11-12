#!/usr/bin/env python3
"""Ship radar: παίρνει ζωντανά δεδομένα AIS από το aisstream.io και τα σερβίρει σε χάρτη."""
import asyncio, json, os, threading, time
from functools import partial
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

import websockets

API_KEY = os.environ.get("AIS_API_KEY", "")
BBOX = [[[37.45, 23.10], [38.05, 23.85]]]
PORT = 8080
MAX_AGE = 30 * 60
BASE = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(BASE, "web")
OUT = os.path.join(WEB, "ships.json")

ships = {}
lock = threading.Lock()


def update(mmsi, **kw):
    with lock:
        s = ships.setdefault(mmsi, {"mmsi": mmsi})
        s.update({k: v for k, v in kw.items() if v not in (None, "")})
        s["ts"] = time.time()


def handle(msg):
    meta = msg.get("MetaData", {})
    mmsi = meta.get("MMSI")
    if not mmsi:
        return
    name = (meta.get("ShipName") or "").strip()
    mtype = msg.get("MessageType")
    body = msg.get("Message", {}).get(mtype, {})
    if mtype in ("PositionReport", "StandardClassBPositionReport"):
        hdg = body.get("TrueHeading")
        update(mmsi, name=name,
               lat=body.get("Latitude", meta.get("latitude")),
               lon=body.get("Longitude", meta.get("longitude")),
               sog=body.get("Sog"), cog=body.get("Cog"),
               hdg=hdg if hdg is not None and hdg < 360 else None)
    elif mtype == "ShipStaticData":
        d = body.get("Dimension") or {}
        length = (d.get("A") or 0) + (d.get("B") or 0)
        beam = (d.get("C") or 0) + (d.get("D") or 0)
        update(mmsi, name=(body.get("Name") or name).strip(),
               type=body.get("Type"),
               dest=(body.get("Destination") or "").strip(),
               length=length or None, beam=beam or None)


def writer():
    while True:
        time.sleep(10)
        now = time.time()
        with lock:
            for k in [k for k, s in ships.items() if now - s["ts"] > MAX_AGE]:
                del ships[k]
            data = [s for s in ships.values() if "lat" in s and "lon" in s]
        tmp = OUT + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"updated": now, "ships": data}, f, ensure_ascii=False)
        os.replace(tmp, OUT)


def server():
    handler = partial(SimpleHTTPRequestHandler, directory=WEB)
    handler.log_message = lambda *a: None
    ThreadingHTTPServer(("0.0.0.0", PORT), handler).serve_forever()


async def stream():
    delay = 2
    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
                await ws.send(json.dumps({
                    "APIKey": API_KEY,
                    "BoundingBoxes": BBOX,
                    "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport",
                                           "ShipStaticData"],
                }))
                print("Συνδέθηκε στο aisstream.io", flush=True)
                delay = 2
                async for raw in ws:
                    try:
                        handle(json.loads(raw))
                    except Exception:
                        pass
        except Exception as e:
            print(f"Αποσύνδεση ({e}), νέα προσπάθεια σε {delay}s", flush=True)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("Λείπει το AIS_API_KEY")
    threading.Thread(target=writer, daemon=True).start()
    threading.Thread(target=server, daemon=True).start()
    print(f"Χάρτης στη θύρα {PORT}", flush=True)
    asyncio.run(stream())
