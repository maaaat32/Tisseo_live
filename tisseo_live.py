#!/usr/bin/env python3
"""
Tisséo Live — prochains passages en temps réel à Toulouse (bus, tram, téléphérique).

Aucune dépendance : Python 3.9+ uniquement.

    python3 tisseo_live.py
    -> ouvre http://localhost:8080 dans ton navigateur

Données (open data Tisséo, licence ODbL) :
  - GTFS statique (arrêts, lignes) : data.toulouse-metropole.fr, dataset "tisseo-gtfs"
  - GTFS-RT (temps réel)           : https://api.tisseo.fr/opendata/gtfsrt/GtfsRt.pb

Limites connues :
  - Le flux temps réel Tisséo (Beta) couvre bus, tram et téléphérique (expérimental).
    Le métro A/B n'y figure pas : pour lui, la page affiche les horaires théoriques si tu
    fournis le GTFS complet (voir option --with-schedule).
"""
import argparse
import io
import json
import os
import struct
import sys
import threading
import time
import urllib.parse
import urllib.request
import zipfile
import csv
from collections import defaultdict
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

GTFS_URL = ("https://data.toulouse-metropole.fr/explore/dataset/tisseo-gtfs/"
            "files/fc1dda89077cf37e4f7521760e0ef4e9/download/")
RT_URL = "https://api.tisseo.fr/opendata/gtfsrt/GtfsRt.pb"
CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "tisseo_live")
RT_TTL = 5          # secondes (le flux est rafraîchi toutes les ~5 s)
GTFS_TTL = 24 * 3600
UA = {"User-Agent": "tisseo-live/1.0 (projet perso)"}


# --------------------------------------------------------------------------
# Décodeur protobuf minimal pour GTFS-RT (évite d'installer protobuf)
# --------------------------------------------------------------------------
def _varint(buf, i):
    shift = result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def _fields(buf):
    """Itère (numéro_de_champ, wire_type, valeur) sur un message protobuf."""
    i, n = 0, len(buf)
    while i < n:
        key, i = _varint(buf, i)
        fn, wt = key >> 3, key & 7
        if wt == 0:
            v, i = _varint(buf, i)
        elif wt == 2:
            ln, i = _varint(buf, i)
            v = buf[i:i + ln]
            i += ln
        elif wt == 1:
            v = struct.unpack_from("<Q", buf, i)[0]
            i += 8
        elif wt == 5:
            v = struct.unpack_from("<I", buf, i)[0]
            i += 4
        else:
            raise ValueError("wire type non géré: %d" % wt)
        yield fn, wt, v


def _signed32(v):
    v &= 0xFFFFFFFF
    return v - (1 << 32) if v & 0x80000000 else v


def parse_gtfs_rt(data):
    """Retourne (timestamp_flux, liste de TripUpdate simplifiés)."""
    feed_ts = 0
    updates = []
    for fn, wt, v in _fields(data):
        if fn == 1 and wt == 2:  # header
            for hf, hw, hv in _fields(v):
                if hf == 3 and hw == 0:
                    feed_ts = hv
        elif fn == 2 and wt == 2:  # entity
            for ef, ew, ev in _fields(v):
                if ef == 3 and ew == 2:  # trip_update
                    updates.append(_parse_trip_update(ev))
    return feed_ts, updates


def _parse_trip_update(buf):
    tu = {"trip_id": "", "route_id": "", "direction": None, "stops": []}
    for f, w, v in _fields(buf):
        if f == 1 and w == 2:  # TripDescriptor
            for tf, tw, tv in _fields(v):
                if tf == 1 and tw == 2:
                    tu["trip_id"] = tv.decode("utf-8", "replace")
                elif tf == 5 and tw == 2:
                    tu["route_id"] = tv.decode("utf-8", "replace")
                elif tf == 6 and tw == 0:
                    tu["direction"] = tv
        elif f == 2 and w == 2:  # StopTimeUpdate
            st = {"stop_id": "", "time": None, "delay": None, "skipped": False}
            for sf, sw, sv in _fields(v):
                if sf == 4 and sw == 2:
                    st["stop_id"] = sv.decode("utf-8", "replace")
                elif sf in (2, 3) and sw == 2:  # arrival / departure
                    for ef, ew, evv in _fields(sv):
                        if ef == 2 and ew == 0 and (sf == 2 or st["time"] is None):
                            st["time"] = evv
                        elif ef == 1 and ew == 0:
                            st["delay"] = _signed32(evv)
                elif sf == 5 and sw == 0 and sv == 1:  # SKIPPED
                    st["skipped"] = True
            tu["stops"].append(st)
    return tu


# --------------------------------------------------------------------------
# GTFS statique
# --------------------------------------------------------------------------
ROUTE_TYPES = {0: "tram", 1: "metro", 2: "train", 3: "bus", 6: "telepherique", 7: "funiculaire"}


def _read(zf, name):
    with zf.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))


class Gtfs:
    def __init__(self, zip_path, with_schedule=False):
        self.stops = {}            # stop_id -> dict
        self.by_name = defaultdict(list)  # nom -> [stop_id]
        self.routes = {}           # route_id -> dict
        self.trips = {}            # trip_id -> (route_id, headsign, service_id)
        self.schedule = defaultdict(list)  # stop_id -> [(secs, trip_id)]  (optionnel)
        self.calendar = {}
        self.calendar_dates = defaultdict(dict)
        with zipfile.ZipFile(zip_path) as zf:
            for r in _read(zf, "stops.txt"):
                if r.get("location_type") in ("1", "2", "3", "4"):
                    continue  # on garde les points d'arrêt (quais)
                s = {
                    "id": r["stop_id"],
                    "name": r["stop_name"].strip(),
                    "lat": float(r["stop_lat"] or 0),
                    "lon": float(r["stop_lon"] or 0),
                }
                self.stops[s["id"]] = s
                self.by_name[s["name"]].append(s["id"])
            for r in _read(zf, "routes.txt"):
                self.routes[r["route_id"]] = {
                    "short": r.get("route_short_name") or r.get("route_long_name", ""),
                    "long": r.get("route_long_name", ""),
                    "type": ROUTE_TYPES.get(int(r.get("route_type") or 3), "bus"),
                    "color": "#" + (r.get("route_color") or "666666"),
                    "text_color": "#" + (r.get("route_text_color") or "ffffff"),
                }
            for r in _read(zf, "trips.txt"):
                self.trips[r["trip_id"]] = (r["route_id"], r.get("trip_headsign", ""), r.get("service_id", ""))
            if with_schedule:
                self._load_schedule(zf)

    def _load_schedule(self, zf):
        names = zf.namelist()
        if "calendar.txt" in names:
            for r in _read(zf, "calendar.txt"):
                self.calendar[r["service_id"]] = r
        if "calendar_dates.txt" in names:
            for r in _read(zf, "calendar_dates.txt"):
                self.calendar_dates[r["service_id"]][r["date"]] = r["exception_type"]
        for r in _read(zf, "stop_times.txt"):
            t = r["departure_time"] or r["arrival_time"]
            if not t:
                continue
            h, m, s = (int(x) for x in t.split(":"))
            self.schedule[r["stop_id"]].append((h * 3600 + m * 60 + s, r["trip_id"]))
        for lst in self.schedule.values():
            lst.sort()

    def service_active(self, service_id, day):
        ymd = day.strftime("%Y%m%d")
        ex = self.calendar_dates.get(service_id, {}).get(ymd)
        if ex == "1":
            return True
        if ex == "2":
            return False
        c = self.calendar.get(service_id)
        if not c or not (c["start_date"] <= ymd <= c["end_date"]):
            return False
        return c[["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][day.weekday()]] == "1"

    def scheduled_next(self, stop_ids, now, limit=12):
        """Passages théoriques (pour le métro, absent du flux temps réel)."""
        out = []
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for day_offset in (-1, 0, 1):
            day = midnight + timedelta(days=day_offset)
            for sid in stop_ids:
                for secs, trip_id in self.schedule.get(sid, ()):
                    ts = day.timestamp() + secs
                    if ts < now.timestamp() - 30 or ts > now.timestamp() + 3 * 3600:
                        continue
                    route_id, head, service = self.trips.get(trip_id, ("", "", ""))
                    if self.service_active(service, day):
                        out.append((ts, sid, route_id, head, trip_id))
        out.sort()
        return out[:limit]

    def search(self, q, limit=15):
        q = norm(q)
        if not q:
            return []
        res = []
        for name, ids in self.by_name.items():
            n = norm(name)
            if q in n:
                res.append((0 if n.startswith(q) else 1, len(n), name, ids))
        res.sort()
        out = []
        for _, _, name, ids in res[:limit]:
            lat = sum(self.stops[i]["lat"] for i in ids) / len(ids)
            lon = sum(self.stops[i]["lon"] for i in ids) / len(ids)
            out.append({"name": name, "ids": ids, "lat": lat, "lon": lon})
        return out

    def near(self, lat, lon, limit=8):
        best = []
        for name, ids in self.by_name.items():
            la = sum(self.stops[i]["lat"] for i in ids) / len(ids)
            lo = sum(self.stops[i]["lon"] for i in ids) / len(ids)
            d = ((la - lat) * 111000) ** 2 + ((lo - lon) * 80000) ** 2
            best.append((d, name, ids, la, lo))
        best.sort()
        return [{"name": n, "ids": ids, "lat": la, "lon": lo, "dist": int(d ** 0.5)}
                for d, n, ids, la, lo in best[:limit]]


def norm(s):
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", s.lower()) if unicodedata.category(c) != "Mn")


# --------------------------------------------------------------------------
# Récupération des données
# --------------------------------------------------------------------------
def fetch(url, timeout=20):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def ensure_gtfs(url, with_schedule):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, "tisseo_gtfs.zip")
    if url.startswith("file://") or os.path.exists(url):
        return url.replace("file://", "")
    if not os.path.exists(path) or time.time() - os.path.getmtime(path) > GTFS_TTL:
        print("Téléchargement du GTFS Tisséo…", flush=True)
        data = fetch(url, timeout=120)
        with open(path, "wb") as f:
            f.write(data)
    return path


class RtCache:
    def __init__(self, url):
        self.url = url
        self.lock = threading.Lock()
        self.ts = 0.0
        self.feed_ts = 0
        self.updates = []
        self.error = None

    def get(self):
        with self.lock:
            if time.time() - self.ts > RT_TTL:
                try:
                    self.feed_ts, self.updates = parse_gtfs_rt(fetch(self.url, timeout=10))
                    self.error = None
                except Exception as e:  # réseau, format…
                    self.error = "%s: %s" % (type(e).__name__, e)
                self.ts = time.time()
            return self.feed_ts, self.updates, self.error


def next_passages(gtfs, rt, stop_ids, limit=12):
    now = time.time()
    feed_ts, updates, error = rt.get()
    wanted = set(stop_ids)
    rows = []
    for tu in updates:
        for st in tu["stops"]:
            if st["stop_id"] in wanted and not st["skipped"] and st["time"]:
                if st["time"] < now - 30:
                    continue
                route_id, head, _ = gtfs.trips.get(tu["trip_id"], (tu["route_id"], "", ""))
                route = gtfs.routes.get(route_id or tu["route_id"], {})
                rows.append({
                    "time": st["time"],
                    "in_sec": int(st["time"] - now),
                    "line": route.get("short", "?"),
                    "type": route.get("type", "bus"),
                    "color": route.get("color", "#666666"),
                    "text_color": route.get("text_color", "#ffffff"),
                    "headsign": head or route.get("long", ""),
                    "stop_id": st["stop_id"],
                    "live": True,
                })
    rows.sort(key=lambda r: r["time"])
    out = rows[:limit]
    # Complète avec les horaires théoriques (métro notamment) si chargés
    if gtfs.schedule:
        live_trips = {(r["line"], r["headsign"]) for r in out}
        for ts, sid, route_id, head, _ in gtfs.scheduled_next(stop_ids, datetime.now().astimezone()):
            route = gtfs.routes.get(route_id, {})
            if route.get("type") == "metro" or (route.get("short"), head) not in live_trips:
                if route.get("type") != "metro" and len(out) >= limit:
                    continue
                out.append({
                    "time": int(ts), "in_sec": int(ts - now),
                    "line": route.get("short", "?"), "type": route.get("type", "bus"),
                    "color": route.get("color", "#666666"), "text_color": route.get("text_color", "#ffffff"),
                    "headsign": head, "stop_id": sid, "live": False,
                })
        out.sort(key=lambda r: r["time"])
        out = out[:limit + 6]
    return {"passages": out, "feed_age": int(now - feed_ts) if feed_ts else None, "error": error}


# --------------------------------------------------------------------------
# Serveur web
# --------------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))


def make_handler(gtfs, rt):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, obj, code=200):
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(u.query)
            try:
                static = {"/": ("index.html", "text/html; charset=utf-8"),
                          "/manifest.json": ("manifest.json", "application/manifest+json"),
                          "/sw.js": ("sw.js", "text/javascript"),
                          "/icon.svg": ("icon.svg", "image/svg+xml")}
                if u.path in static:
                    fname, ctype = static[u.path]
                    with open(os.path.join(HERE, fname), "rb") as f:
                        body = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", ctype)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif u.path == "/api/search":
                    self._json(gtfs.search(q.get("q", [""])[0]))
                elif u.path == "/api/near":
                    self._json(gtfs.near(float(q["lat"][0]), float(q["lon"][0])))
                elif u.path == "/api/next":
                    ids = [i for i in q.get("ids", [""])[0].split(",") if i]
                    self._json(next_passages(gtfs, rt, ids))
                elif u.path == "/api/status":
                    feed_ts, updates, err = rt.get()
                    self._json({"stops": len(gtfs.stops), "routes": len(gtfs.routes),
                                "trip_updates": len(updates), "feed_ts": feed_ts, "error": err})
                else:
                    self._json({"error": "not found"}, 404)
            except Exception as e:
                self._json({"error": "%s: %s" % (type(e).__name__, e)}, 500)
    return H


def main():
    ap = argparse.ArgumentParser(description="Prochains passages Tisséo (Toulouse)")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    ap.add_argument("--host", default="0.0.0.0" if os.environ.get("PORT") else "127.0.0.1", help="0.0.0.0 pour y accéder depuis ton téléphone (même Wi-Fi)")
    ap.add_argument("--gtfs", default=GTFS_URL, help="URL ou chemin du GTFS statique (zip)")
    ap.add_argument("--rt", default=RT_URL, help="URL du flux GTFS-RT (.pb)")
    ap.add_argument("--with-schedule", action="store_true",
                    help="charge stop_times.txt (plus lent, ~1 min) pour afficher les horaires théoriques, dont le métro")
    a = ap.parse_args()

    path = ensure_gtfs(a.gtfs, a.with_schedule)
    print("Chargement des arrêts et des lignes…", flush=True)
    gtfs = Gtfs(path, a.with_schedule)
    print("%d arrêts, %d lignes" % (len(gtfs.stops), len(gtfs.routes)), flush=True)
    rt = RtCache(a.rt)
    srv = ThreadingHTTPServer((a.host, a.port), make_handler(gtfs, rt))
    print("→ http://%s:%d" % ("localhost" if a.host == "127.0.0.1" else a.host, a.port), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
