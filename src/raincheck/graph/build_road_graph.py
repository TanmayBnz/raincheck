"""Phase 6b / D6 -- critical roads and sensor coverage.

Which roads does each city's main-road network depend on most, and which of
those have no traffic sensor?

Why OpenStreetMap and not links.csv: links.csv holds only the road pieces that
carry a UTD19 detector. Joined end to end they fall apart (Torino: 159
fragments at a 30 m snap), so any centrality computed on them describes the
sensor layout, not the road network. The main-road network around the sensors
is fetched from OpenStreetMap once and cached.

Criticality is length-weighted edge betweenness: of all shortest routes between
junctions, the share that runs along each road. A road many routes depend on is
one whose slowdown spreads furthest.

Graph library: networkx. GraphX has no Python API and the networks here are a
few thousand edges, which networkx handles in seconds.

Outputs:
  lake/osm/{city}.json                    -- cached Overpass response
  export/dashboard/graph_{city}.json      -- road geometry + scores for the dashboard
  reports/phase6b_road_graph.md / .json

Run:  python -m raincheck.graph.build_road_graph
"""

from __future__ import annotations

import json
import math
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import duckdb
import networkx as nx
import numpy as np

from raincheck import config

OSM_CACHE = config.LAKE_ROOT / "osm"
EXPORT = config.PROJECT_ROOT / "export" / "dashboard"
REPORT = config.REPORTS_DIR / "phase6b_road_graph.md"
REPORT_JSON = config.REPORTS_DIR / "phase6b_road_graph.json"
ANALYSIS = config.LAKE_ROOT / "analysis" / "measurements_rain"

CITIES = ["manchester", "torino", "essen"]
MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
ROAD_CLASSES = "motorway|trunk|primary|secondary|tertiary"  # plus their _link ramps
MARGIN_KM = 3.0          # around the sensor extent
SENSOR_SNAP_M = 40.0     # a sensor further than this from every road is unmatched
TOP_SHARE = 0.10         # "critical" = top 10% of road length by betweenness
SEED = 20261017


def metres(lat1, lon1, lat2, lon2):
    """Equirectangular distance; exact enough at city scale."""
    k = 111_320.0
    dy = (lat2 - lat1) * k
    dx = (lon2 - lon1) * k * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot(dx, dy)


def sensor_extent(city):
    q = f"""select min(lat), max(lat), min(lon), max(lon)
            from read_parquet('{config.spark_path(ANALYSIS)}/city={city}/**/*.parquet')"""
    s, n, w, e = duckdb.sql(q).fetchone()
    dlat = MARGIN_KM / 111.32
    dlon = MARGIN_KM / (111.32 * math.cos(math.radians((s + n) / 2)))
    return s - dlat, w - dlon, n + dlat, e + dlon


def fetch_osm(city):
    """Main roads in the box around the sensors. Cached; mirrors tried in turn."""
    OSM_CACHE.mkdir(parents=True, exist_ok=True)
    path = OSM_CACHE / f"{city}.json"
    if path.exists():
        return json.loads(path.read_text())
    s, w, n, e = sensor_extent(city)
    query = (f'[out:json][timeout:120];way["highway"~"^({ROAD_CLASSES})(_link)?$"]'
             f"({s:.5f},{w:.5f},{n:.5f},{e:.5f});(._;>;);out body;")
    body = urllib.parse.urlencode({"data": query}).encode()
    # Public Overpass servers are often at capacity and say so with 502/504;
    # they ask for exponential backoff, so cycle the mirrors with growing waits.
    for attempt in range(5):
        for url in MIRRORS:
            try:
                req = urllib.request.Request(url, body, {"User-Agent": "raincheck-study/1.0"})
                data = json.loads(urllib.request.urlopen(req, timeout=180).read())
                path.write_text(json.dumps(data))
                print(f"  {city}: {len(data['elements']):,} OSM elements from {url.split('/')[2]}")
                return data
            except Exception as exc:
                code = getattr(exc, "code", exc.__class__.__name__)
                print(f"  {city}: {url.split('/')[2]} -> {code}")
        wait = 10 * 2 ** attempt
        print(f"  {city}: all mirrors busy, retrying in {wait}s")
        time.sleep(wait)
    raise SystemExit(f"FAIL: no Overpass mirror answered for {city}")


def build_graph(osm):
    """Undirected main-road graph, degree-2 chains contracted into single edges.

    ponytail: undirected -- one-way restrictions are ignored, which overstates
    some detour options. Use a DiGraph with the oneway tag if betweenness on
    one-way systems ever matters.
    """
    nodes = {el["id"]: (el["lat"], el["lon"]) for el in osm["elements"] if el["type"] == "node"}
    raw = nx.Graph()
    for el in osm["elements"]:
        if el["type"] != "way":
            continue
        tags = el.get("tags", {})
        meta = {"cls": tags.get("highway", "").replace("_link", ""),
                "name": tags.get("name") or tags.get("ref") or ""}
        ids = [i for i in el["nodes"] if i in nodes]
        for a, b in zip(ids, ids[1:]):
            raw.add_edge(a, b, length=metres(*nodes[a], *nodes[b]), **meta)

    # Contract chains so an edge is junction-to-junction: fewer, longer edges
    # render as clean polylines and make betweenness about real road links.
    junction = {n for n in raw if raw.degree(n) != 2}
    if not junction:  # a pure loop; pick any node
        junction = {next(iter(raw))}
    G, seen = nx.MultiGraph(), set()
    for start in junction:
        for nxt in raw.neighbors(start):
            if (start, nxt) in seen:
                continue
            path, prev, cur = [start], start, nxt
            while True:
                seen.add((prev, cur)); seen.add((cur, prev))
                path.append(cur)
                if cur in junction:
                    break
                prev, cur = cur, next(n for n in raw.neighbors(cur) if n != prev)
            d = raw.edges[path[0], path[1]]
            G.add_edge(path[0], path[-1],
                       length=sum(raw.edges[a, b]["length"] for a, b in zip(path, path[1:])),
                       coords=[[round(nodes[i][1], 5), round(nodes[i][0], 5)] for i in path],
                       cls=d["cls"], name=d["name"])
    return G


def edge_betweenness(G):
    """Length-weighted edge betweenness on a simple-graph view, normalised to 0..1."""
    simple = nx.Graph()
    for u, v, d in G.edges(data=True):
        if u != v and (not simple.has_edge(u, v) or d["length"] < simple.edges[u, v]["length"]):
            simple.add_edge(u, v, length=d["length"])
    k = min(len(simple), 1500)  # sampled sources; exact when the graph is smaller
    eb = nx.edge_betweenness_centrality(simple, k=k, weight="length", seed=SEED)
    top = max(eb.values()) or 1.0
    return {frozenset(e): v / top for e, v in eb.items()}


def detectors(city):
    q = f"""select detid, any_value(lat) lat, any_value(lon) lon, any_value(fclass) fclass,
                   avg(typical_speed) normal_speed
            from read_parquet('{config.spark_path(ANALYSIS)}/city={city}/**/*.parquet')
            group by detid"""
    return duckdb.sql(q).fetchall()


def snap(edges, sensors):
    """Nearest edge (by distance to its vertices) for every sensor, within SENSOR_SNAP_M."""
    pts, owner = [], []
    for i, e in enumerate(edges):
        for lon, lat in e["c"]:
            pts.append((lat, lon)); owner.append(i)
        # Densify long straight edges so a sensor mid-edge is not missed.
        for (lo1, la1), (lo2, la2) in zip(e["c"], e["c"][1:]):
            steps = int(metres(la1, lo1, la2, lo2) // 20)
            for t in range(1, steps):
                f = t / steps
                pts.append((la1 + f * (la2 - la1), lo1 + f * (lo2 - lo1))); owner.append(i)
    P = np.array(pts)
    owner = np.array(owner)
    coslat = math.cos(math.radians(P[:, 0].mean()))
    out = []
    for det, lat, lon, fclass, normal in sensors:
        d = np.hypot((P[:, 0] - lat) * 111_320, (P[:, 1] - lon) * 111_320 * coslat)
        j = int(d.argmin())
        hit = int(owner[j]) if d[j] <= SENSOR_SNAP_M else None
        out.append({"id": det, "lat": round(lat, 5), "lon": round(lon, 5),
                    "edge": hit, "dist_m": round(float(d[j]), 1),
                    "normal_kmh": round(normal, 1) if normal else None})
    return out


def analyse(city):
    G = build_graph(fetch_osm(city))
    eb = edge_betweenness(G)
    edges = [{"c": d["coords"], "b": round(eb.get(frozenset((u, v)), 0.0), 4),
              "cls": d["cls"], "name": d["name"], "len": round(d["length"])}
             for u, v, d in G.edges(data=True) if u != v]
    sensors = snap(edges, detectors(city))
    covered = {s["edge"] for s in sensors if s["edge"] is not None}
    for i, e in enumerate(edges):
        e["s"] = i in covered

    # Critical = the highest-betweenness edges making up TOP_SHARE of road length.
    order = sorted(range(len(edges)), key=lambda i: -edges[i]["b"])
    total = sum(e["len"] for e in edges)
    crit, acc = [], 0
    for i in order:
        if acc >= TOP_SHARE * total:
            break
        crit.append(i); acc += edges[i]["len"]
    crit_set = set(crit)
    for i, e in enumerate(edges):
        e["crit"] = i in crit_set
    crit_len = sum(edges[i]["len"] for i in crit)
    crit_cov = sum(edges[i]["len"] for i in crit if edges[i]["s"])

    # Unmonitored critical roads, grouped by street name for a readable list.
    gaps = {}
    for i in crit:
        e = edges[i]
        if not e["s"]:
            key = e["name"] or f"unnamed {e['cls']}"
            g = gaps.setdefault(key, {"name": key, "cls": e["cls"], "len": 0, "b": 0.0})
            g["len"] += e["len"]; g["b"] = max(g["b"], e["b"])
    gaps = sorted(gaps.values(), key=lambda g: -g["b"] * g["len"])

    EXPORT.mkdir(parents=True, exist_ok=True)
    (EXPORT / f"graph_{city}.json").write_text(
        json.dumps({"city": city, "edges": edges, "sensors": sensors}, separators=(",", ":")))

    return {
        "junctions": G.number_of_nodes(), "edges": len(edges),
        "road_km": round(total / 1000, 1),
        "sensors": len(sensors),
        "sensors_matched": sum(1 for s in sensors if s["edge"] is not None),
        "critical_km": round(crit_len / 1000, 1),
        "critical_covered_pct": round(100 * crit_cov / crit_len, 1) if crit_len else 0.0,
        "all_covered_pct": round(100 * sum(e["len"] for e in edges if e["s"]) / total, 1),
        "top_gaps": gaps[:8],
    }


def render(res, generated):
    L = ["# Phase-6b Critical Roads and Sensor Coverage", "",
         f"_Generated {generated}. OpenStreetMap main roads ({ROAD_CLASSES} and "
         f"ramps) within {MARGIN_KM:.0f} km of each city's sensors. Criticality is "
         "length-weighted edge betweenness, scaled so the most critical road in "
         f"each city is 1. \"Critical\" is the top {TOP_SHARE:.0%} of road length "
         f"by that score. A sensor counts as on a road within {SENSOR_SNAP_M:.0f} m._",
         "",
         "| city | road km | junctions | road links | sensors (matched) | critical km "
         "| critical km with a sensor | all km with a sensor |",
         "|---|---|---|---|---|---|---|---|"]
    for c, r in res.items():
        L.append(f"| {c} | {r['road_km']} | {r['junctions']:,} | {r['edges']:,} | "
                 f"{r['sensors']} ({r['sensors_matched']}) | {r['critical_km']} | "
                 f"{r['critical_covered_pct']}% | {r['all_covered_pct']}% |")
    L.append("")
    for c, r in res.items():
        L += [f"## {c}: critical roads with no sensor", "",
              "| road | class | unmonitored critical length (m) | peak criticality |",
              "|---|---|---|---|"]
        L += [f"| {g['name']} | {g['cls']} | {g['len']:,} | {g['b']:.2f} |" for g in r["top_gaps"]]
        L.append("")
    L += ["## Caveats", "",
          "- Today's OpenStreetMap, against 2016–2017 traffic. Main roads change",
          "  little over that span, but not never.",
          "- One-way restrictions are ignored, so some detours look shorter than",
          "  they are.",
          "- Betweenness measures how many shortest routes use a road, not how",
          "  much traffic it carries. It ranks structural importance; it is not a",
          "  traffic count.",
          "- The box is the sensor extent plus a margin, so roads near its edge",
          "  have fewer routes passing through them than in the full city.", ""]
    return "\n".join(L)


def main() -> int:
    res = {}
    for city in CITIES:
        print(f"[{city}]")
        res[city] = analyse(city)
        r = res[city]
        print(f"  {r['road_km']} km, {r['edges']:,} links, sensors matched "
              f"{r['sensors_matched']}/{r['sensors']}, critical covered "
              f"{r['critical_covered_pct']}%")
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    failures = [f"{c}: under half the sensors matched a road"
                for c, r in res.items() if r["sensors_matched"] < r["sensors"] / 2]
    REPORT.write_text(render(res, generated), encoding="utf-8")
    REPORT_JSON.write_text(json.dumps({"generated": generated, "results": res,
                                       "failures": failures}, indent=2), encoding="utf-8")
    print(f"wrote {REPORT}")
    for f in failures:
        print(f"FAIL: {f}")
    if not failures:
        print("PASS: road graph built")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
