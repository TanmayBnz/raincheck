"""Assemble the monitor as a standalone static site in site/ for Vercel.

The artifact version of dashboard/index.html is a page fragment (the artifact
host wraps it in a document); a static host needs the full document, so this
adds the doctype, head and the few base rules the artifact host supplies.

Run:  python dashboard/build_site.py   (after raincheck.dashboard.export_dashboard)
"""
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC, DATA, OUT = ROOT / "dashboard" / "index.html", ROOT / "export" / "dashboard", ROOT / "site"

BASE = """<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="description" content="Rain-aware traffic monitor for Torino, Manchester and Essen.">
<style>:root{padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}
body{margin:0}img{max-width:100%}[hidden]{display:none!important}</style>
"""

page = SRC.read_text(encoding="utf-8").replace('<meta charset="utf-8">\n', "", 1)
head, body = page.split('<div class="wrap">', 1)
OUT.mkdir(exist_ok=True)
(OUT / "index.html").write_text(
    f'<!doctype html>\n<html lang="en">\n<head>\n{BASE}{head}</head>\n<body>\n<div class="wrap">{body}</body>\n</html>\n',
    encoding="utf-8")
files = sorted(DATA.glob("*.json"))
if not files:
    raise SystemExit("FAIL: no data in export/dashboard; run raincheck.dashboard.export_dashboard first")
for f in files:
    shutil.copy2(f, OUT / f.name)
print(f"wrote {OUT}: index.html + {len(files)} data files")
