"""Architecture diagram as hand-laid SVG, light and dark (docs/img/architecture-*.svg)."""
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "docs" / "img"
OUT.mkdir(parents=True, exist_ok=True)
T = {"light": dict(bg="#ffffff", ink="#1f2328", ink2="#59636e", box="#f6f8fa", line="#8c959f",
                   a="#2a78d6", b="#eb6834", c="#1baf7a", d="#8250df"),
     "dark": dict(bg="#0d1117", ink="#e6edf3", ink2="#9198a1", box="#161b22", line="#6e7681",
                  a="#3987e5", b="#d95926", c="#199e70", d="#a371f7")}


def box(x, y, w, h, title, sub, col, t):
    s = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{t["box"]}" stroke="{col}" stroke-width="1.8"/>'
    s += f'<text x="{x + 12}" y="{y + 22}" font-weight="600" fill="{t["ink"]}">{title}</text>'
    for i, line in enumerate(sub):
        s += f'<text x="{x + 12}" y="{y + 42 + 16 * i}" font-size="12" fill="{t["ink2"]}">{line}</text>'
    return s


def arrow(x1, y1, x2, y2, t, label="", dash=False, lx=None, ly=None):
    d = ' stroke-dasharray="5 4"' if dash else ""
    s = f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{t["line"]}" stroke-width="1.6"{d} marker-end="url(#ah)"/>'
    if label:
        s += (f'<text x="{lx if lx is not None else (x1 + x2) / 2 + 6}" y="{ly if ly is not None else (y1 + y2) / 2 - 6}" '
              f'font-size="11.5" fill="{t["ink2"]}">{label}</text>')
    return s


for name, t in T.items():
    W, H = 1000, 520
    s = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
         f'font-family="-apple-system,Segoe UI,Helvetica,Arial,sans-serif" font-size="14">',
         f'<rect width="{W}" height="{H}" fill="{t["bg"]}"/>',
         f'<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
         f'<path d="M0,0 L10,5 L0,10 z" fill="{t["line"]}"/></marker></defs>']
    # clients and service
    s.append(box(30, 30, 170, 70, "Clients", ["PUT / GET / DELETE / scan"], t["ink2"], t))
    s.append(box(260, 20, 300, 90, "Leader (FastAPI)", ["/kv, /scan, /admin, /metrics", "one ordered write path"], t["a"], t))
    s.append(arrow(200, 65, 258, 65, t))
    s.append(box(740, 20, 230, 90, "Follower", ["applies shipped WAL records", "promote on leader failure"], t["a"], t))
    s.append(arrow(560, 50, 738, 50, t, "sync: ack after follower", lx=574, ly=42))
    s.append(arrow(560, 85, 738, 85, t, "async: ship every 50 ms", dash=True, lx=574, ly=102))
    # engine
    s.append(f'<rect x="30" y="150" width="940" height="200" rx="10" fill="none" stroke="{t["line"]}" stroke-dasharray="4 4"/>')
    s.append(f'<text x="44" y="172" font-weight="600" fill="{t["ink2"]}">LSM engine (lsmkv/engine)</text>')
    s.append(box(50, 190, 200, 140, "1. WAL", ["CRC per record", "fsync before ack", "torn tail truncated", "on replay"], t["b"], t))
    s.append(box(280, 190, 200, 140, "2. Memtable", ["skip list", "flush at size limit", "tombstones for deletes"], t["b"], t))
    s.append(box(510, 190, 210, 140, "3. SSTables", ["sorted, immutable", "sparse index + bloom", "whole-file checksum", "tmp + fsync + rename"], t["c"], t))
    s.append(box(750, 190, 200, 140, "4. Compaction", ["size-tiered k-way merge", "newest wins", "drops tombstones", "at the bottom tier"], t["c"], t))
    s.append(arrow(410, 110, 150, 188, t))
    s.append(arrow(250, 260, 278, 260, t))
    s.append(arrow(480, 260, 508, 260, t))
    s.append(arrow(720, 245, 748, 245, t))
    s.append(arrow(748, 275, 722, 275, t))
    s.append(f'<text x="530" y="345" font-size="11.5" fill="{t["ink2"]}">MANIFEST (fsync + rename) is the commit point for flushes and compactions</text>')
    # ops
    s.append(box(30, 390, 220, 110, "Prometheus", ["8 alert rules (ops/alerts.yml)", "scrapes /metrics: latency,", "fsync, backlog, lag, disk"], t["d"], t))
    s.append(box(280, 390, 200, 110, "Runbooks", ["one per alert", "ops/runbooks/"], t["d"], t))
    s.append(box(510, 390, 210, 110, "Backup", ["snapshot + archived WAL", "point-in-time restore"], t["d"], t))
    s.append(box(750, 390, 220, 110, "Chaos + CI gate", ["SIGKILL at engine points", "release blocked unless", "0 acked writes lost"], t["d"], t))
    s.append("</svg>")
    (OUT / f"architecture-{name}.svg").write_text("\n".join(s))
print("ok")
