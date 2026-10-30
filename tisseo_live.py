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
    alerts = []
    for fn, wt, v in _fields(data):
        if fn == 1 and wt == 2:  # header
            for hf, hw, hv in _fields(v):
                if hf == 3 and hw == 0:
                    feed_ts = hv
        elif fn == 2 and wt == 2:  # entity
            for ef, ew, ev in _fields(v):
                if ef == 3 and ew == 2:  # trip_update
                    updates.append(_parse_trip_update(ev))
                elif ef == 5 and ew == 2:  # alert
                    alerts.append(_parse_alert(ev))
    return feed_ts, updates, alerts


def _tstr(buf):
    """TranslatedString -> texte (français de préférence)."""
    best = None
    for f, w, v in _fields(buf):
        if f == 1 and w == 2:
            text = lang = ""
            for tf, tw, tv in _fields(v):
                if tf == 1 and tw == 2:
                    text = tv.decode("utf-8", "replace")
                elif tf == 2 and tw == 2:
                    lang = tv.decode("utf-8", "replace")
            if lang.lower().startswith("fr"):
                return text
            if best is None:
                best = text
    return best or ""


def _parse_alert(buf):
    a = {"periods": [], "routes": [], "stops": [], "effect": 8, "header": "", "desc": "", "url": ""}
    for f, w, v in _fields(buf):
        if f == 1 and w == 2:  # active_period
            st = en = 0
            for tf, tw, tv in _fields(v):
                if tf == 1 and tw == 0:
                    st = tv
                elif tf == 2 and tw == 0:
                    en = tv
            a["periods"].append((st, en))
        elif f == 5 and w == 2:  # informed_entity
            for ef, ew, ev in _fields(v):
                if ef == 2 and ew == 2:
                    a["routes"].append(ev.decode("utf-8", "replace"))
                elif ef == 5 and ew == 2:
                    a["stops"].append(ev.decode("utf-8", "replace"))
        elif f == 7 and w == 0:
            a["effect"] = v
        elif f == 8 and w == 2:
            a["url"] = _tstr(v)
        elif f == 10 and w == 2:
            a["header"] = _tstr(v)
        elif f == 11 and w == 2:
            a["desc"] = _tstr(v)
    return a


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
    def __init__(self, zip_path, metro_schedule=True):
        self.metro_stops = set()   # arrêts desservis par le métro
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
            if metro_schedule:
                self._load_metro_schedule(zf)

    def _load_metro_schedule(self, zf):
        """Horaires théoriques du métro uniquement (le flux temps réel ne le contient pas).
        On ne garde que les trajets du métro : quelques milliers de lignes, très léger."""
        metro_trips = {t for t, (rid, _, _) in self.trips.items()
                       if self.routes.get(rid, {}).get("type") == "metro"}
        if not metro_trips:
            return
        names = zf.namelist()
        if "calendar.txt" in names:
            for r in _read(zf, "calendar.txt"):
                self.calendar[r["service_id"]] = r
        if "calendar_dates.txt" in names:
            for r in _read(zf, "calendar_dates.txt"):
                self.calendar_dates[r["service_id"]][r["date"]] = r["exception_type"]
        for r in _read(zf, "stop_times.txt"):
            if r["trip_id"] not in metro_trips:
                continue
            t = r["departure_time"] or r["arrival_time"]
            if not t:
                continue
            h, m, s = (int(x) for x in t.split(":"))
            self.schedule[r["stop_id"]].append((h * 3600 + m * 60 + s, r["trip_id"]))
        for lst in self.schedule.values():
            lst.sort()
        self.metro_stops = set(self.schedule)

    def service_active(self, service_id, day):
        """day : datetime.date du jour de service (heure de Paris)."""
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

    def scheduled_next(self, stop_ids, now_ts, limit=12, horizon=3 * 3600):
        """Prochains passages théoriques du métro aux arrêts donnés."""
        out = []
        today = paris_date(now_ts)
        for day_offset in (-1, 0, 1):
            day = today + timedelta(days=day_offset)
            base = service_day_start(day)
            for sid in stop_ids:
                for secs, trip_id in self.schedule.get(sid, ()):
                    ts = base + secs
                    if ts < now_ts - 30 or ts > now_ts + horizon:
                        continue
                    route_id, head, service = self.trips.get(trip_id, ("", "", ""))
                    if self.service_active(service, day):
                        out.append((ts, sid, route_id, head, trip_id))
        out.sort()
        return out[:limit]

    def _ids_for(self, ids, mode):
        if mode == "metro":
            return [i for i in ids if i in self.metro_stops]
        return ids

    def search(self, q, limit=15, mode=None):
        q = norm(q)
        if not q:
            return []
        res = []
        for name, ids in self.by_name.items():
            n = norm(name)
            if q in n:
                ids = self._ids_for(ids, mode)
                if ids:
                    res.append((0 if n.startswith(q) else 1, len(n), name, ids))
        res.sort()
        out = []
        for _, _, name, ids in res[:limit]:
            lat = sum(self.stops[i]["lat"] for i in ids) / len(ids)
            lon = sum(self.stops[i]["lon"] for i in ids) / len(ids)
            out.append({"name": name, "ids": ids, "lat": lat, "lon": lon})
        return out

    def near(self, lat, lon, limit=8, mode=None):
        best = []
        for name, ids in self.by_name.items():
            ids = self._ids_for(ids, mode)
            if not ids:
                continue
            la = sum(self.stops[i]["lat"] for i in ids) / len(ids)
            lo = sum(self.stops[i]["lon"] for i in ids) / len(ids)
            d = ((la - lat) * 111000) ** 2 + ((lo - lon) * 80000) ** 2
            best.append((d, name, ids, la, lo))
        best.sort()
        return [{"name": n, "ids": ids, "lat": la, "lon": lo, "dist": int(d ** 0.5)}
                for d, n, ids, la, lo in best[:limit]]


# --- Heure de Paris sans dépendance (pas besoin de tzdata dans Docker) ---
def _last_sunday(year, month):
    d = datetime(year, month, 31 if month in (3, 10) else 30)
    while d.weekday() != 6:
        d -= timedelta(days=1)
    return d


def paris_offset(utc_ts):
    """Décalage de Paris par rapport à UTC en secondes (été 7200, hiver 3600)."""
    y = datetime.utcfromtimestamp(utc_ts).year
    start = (_last_sunday(y, 3).replace(hour=1) - datetime(1970, 1, 1)).total_seconds()
    end = (_last_sunday(y, 10).replace(hour=1) - datetime(1970, 1, 1)).total_seconds()
    return 7200 if start <= utc_ts < end else 3600


def paris_date(utc_ts):
    return datetime.utcfromtimestamp(utc_ts + paris_offset(utc_ts)).date()


def service_day_start(day):
    """Timestamp UTC du début d'un jour de service GTFS (midi local moins 12 h)."""
    noon_utc_naive = (datetime(day.year, day.month, day.day, 12) - datetime(1970, 1, 1)).total_seconds()
    return noon_utc_naive - paris_offset(noon_utc_naive) - 12 * 3600


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
        self.alerts = []
        self.error = None

    def get(self):
        with self.lock:
            if time.time() - self.ts > RT_TTL:
                try:
                    self.feed_ts, self.updates, self.alerts = parse_gtfs_rt(fetch(self.url, timeout=10))
                    self.error = None
                except Exception as e:  # réseau, format…
                    self.error = "%s: %s" % (type(e).__name__, e)
                self.ts = time.time()
            return self.feed_ts, self.updates, self.error


EFFECTS = {1: ("Service interrompu", 3), 2: ("Service réduit", 2), 3: ("Retards importants", 2),
           4: ("Déviation", 2), 5: ("Service renforcé", 0), 6: ("Service modifié", 1),
           7: ("Information", 0), 8: ("Information", 0), 9: ("Arrêt déplacé", 2),
           10: ("Information", 0), 11: ("Accessibilité", 1)}


def active_alerts(gtfs, rt, now=None):
    """Alertes en cours, mises en forme pour l'affichage, les plus graves d'abord."""
    now = now or time.time()
    out = []
    for a in rt.alerts:
        if a["periods"] and not any(st <= now and (en == 0 or now <= en) for st, en in a["periods"]):
            continue
        if not (a["header"] or a["desc"]):
            continue
        label, sev = EFFECTS.get(a["effect"], ("Information", 0))
        lines, seen = [], set()
        for rid in a["routes"]:
            r = gtfs.routes.get(rid)
            if r and r["short"] not in seen:
                seen.add(r["short"])
                lines.append({"short": r["short"], "color": r["color"], "text_color": r["text_color"], "type": r["type"]})
        out.append({"header": a["header"], "desc": a["desc"], "url": a["url"], "label": label, "severity": sev,
                    "lines": lines, "routes": a["routes"], "stops": a["stops"]})
    out.sort(key=lambda x: -x["severity"])
    return out


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
                    "route_id": route_id or tu["route_id"],
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
    # Métro : absent du flux temps réel, on ajoute les horaires théoriques
    if gtfs.schedule:
        for ts, sid, route_id, head, _ in gtfs.scheduled_next(stop_ids, now, limit=limit):
            route = gtfs.routes.get(route_id, {})
            out.append({
                "route_id": route_id,
                "time": int(ts), "in_sec": int(ts - now),
                "line": route.get("short", "?"), "type": "metro",
                "color": route.get("color", "#666666"), "text_color": route.get("text_color", "#ffffff"),
                "headsign": head or route.get("long", ""), "stop_id": sid, "live": False,
            })
        out.sort(key=lambda r: r["time"])
    routes = {r["route_id"] for r in out}
    mine = [a for a in active_alerts(gtfs, rt, now)
            if wanted & set(a["stops"]) or routes & set(a["routes"])]
    return {"passages": out, "alerts": mine[:3], "feed_age": int(now - feed_ts) if feed_ts else None, "error": error}


# --------------------------------------------------------------------------
# Serveur web
# --------------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))


class State:
    gtfs = None
    error = None


def make_handler(state, rt):
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
                elif u.path.startswith("/api/") and state.gtfs is None:
                    msg = state.error or "Chargement des données Tisséo en cours, réessaie dans une minute."
                    self._json({"error": msg, "loading": state.error is None, "passages": []}, 503)
                elif u.path == "/api/search":
                    gtfs = state.gtfs
                    self._json(gtfs.search(q.get("q", [""])[0], mode=q.get("mode", [None])[0]))
                elif u.path == "/api/near":
                    gtfs = state.gtfs
                    self._json(gtfs.near(float(q["lat"][0]), float(q["lon"][0]), mode=q.get("mode", [None])[0]))
                elif u.path == "/api/next":
                    ids = [i for i in q.get("ids", [""])[0].split(",") if i]
                    self._json(next_passages(state.gtfs, rt, ids))
                elif u.path == "/api/alerts":
                    rt.get()
                    self._json(active_alerts(state.gtfs, rt))
                elif u.path == "/api/status":
                    gtfs = state.gtfs
                    feed_ts, updates, err = rt.get()
                    self._json({"stops": len(gtfs.stops), "routes": len(gtfs.routes),
                                "metro_stops": len(gtfs.metro_stops),
                                "trip_updates": len(updates), "alerts": len(rt.alerts), "feed_ts": feed_ts, "error": err})
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
    ap.add_argument("--no-metro", action="store_true",
                    help="ne charge pas les horaires théoriques du métro (démarrage plus rapide)")
    a = ap.parse_args()

    state = State()
    rt = RtCache(a.rt)

    def load():
        try:
            path = ensure_gtfs(a.gtfs, False)
            print("Chargement des arrêts, lignes et horaires du métro…", flush=True)
            g = Gtfs(path, not a.no_metro)
            print("%d arrêts (dont %d métro), %d lignes" % (len(g.stops), len(g.metro_stops), len(g.routes)), flush=True)
            state.gtfs = g
        except Exception as e:
            state.error = "Impossible de charger les données Tisséo : %s: %s" % (type(e).__name__, e)
            print(state.error, flush=True)

    # Le serveur démarre tout de suite ; les données se chargent en arrière-plan
    threading.Thread(target=load, daemon=True).start()
    srv = ThreadingHTTPServer((a.host, a.port), make_handler(state, rt))
    print("→ http://%s:%d" % ("localhost" if a.host == "127.0.0.1" else a.host, a.port), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
