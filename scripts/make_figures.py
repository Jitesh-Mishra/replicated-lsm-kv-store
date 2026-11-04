"""README figures (light + dark) from reports/: incident timeline and the SQLite comparison."""
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
REP, OUT = ROOT / "reports", ROOT / "docs" / "img"
OUT.mkdir(parents=True, exist_ok=True)
THEMES = {"light": dict(bg="#ffffff", ink="#1f2328", ink2="#59636e", grid="#e6e9ed",
                        cols=["#2a78d6", "#eb6834", "#1baf7a", "#8250df"]),
          "dark": dict(bg="#0d1117", ink="#e6edf3", ink2="#9198a1", grid="#21262d",
                       cols=["#3987e5", "#d95926", "#199e70", "#a371f7"])}


def style(ax, th):
    ax.set_facecolor(th["bg"])
    ax.set_axisbelow(True)
    ax.grid(axis="y", color=th["grid"], lw=0.8)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)


drill = json.loads((REP / "incident_drill.json").read_text())
bench = list(csv.DictReader(open(REP / "bench_vs_sqlite.csv")))

for theme, th in THEMES.items():
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": th["ink2"], "axes.labelcolor": th["ink"],
                         "xtick.color": th["ink2"], "ytick.color": th["ink2"], "text.color": th["ink"]})

    # 1. incident timeline
    s = drill["series"]
    t_inj = drill["inject_at_s"]
    t_fire = t_inj + drill["time_to_detect_s"]
    t_fix = t_inj + drill["time_to_mitigate_s"]
    fig, axes = plt.subplots(2, 1, figsize=(11, 5.2), sharex=True, facecolor=th["bg"])
    ax = axes[0]
    for (name, lab), c in zip([("sstables", "live SSTables"), ("compaction_backlog_tables", "compaction backlog")],
                              th["cols"]):
        xs, ys = zip(*s[name])
        ax.plot(xs, ys, color=c, lw=2, label=lab)
    ax.axhline(12, color=th["ink2"], ls=":", lw=1)
    ax.text(2, 12.5, "alert threshold (backlog >= 12 for 30 s)", fontsize=8.5, color=th["ink2"])
    ax.set_ylabel("tables")
    ax.legend(frameon=False, loc="upper left")
    ax = axes[1]
    for (name, lab), c in zip([("get_p99_ms", "get p99"), ("put_p99_ms", "put p99")], th["cols"][2:]):
        if s[name]:
            xs, ys = zip(*s[name])
            ax.plot(xs, ys, color=c, lw=2, label=lab)
    ax.set_ylabel("ms (15 s window)")
    ax.set_xlabel("seconds into the drill")
    ax.legend(frameon=False, loc="upper left")
    for a in axes:
        style(a, th)
        for x, lab in ((t_inj, "fault injected"), (t_fire, "alert fires"), (t_fix + 1.5, "runbook: restart compactor")):
            a.axvline(x, color=th["ink2"], ls="--", lw=1)
    top = axes[0].get_ylim()[1]
    axes[0].text(t_inj - 1, top * 0.92, "fault injected", ha="right", fontsize=8.5, color=th["ink2"])
    axes[0].text(t_fire + 1, top * 0.92, f"alert fires (+{drill['time_to_detect_s']:.0f} s)\nrunbook fix, resolved "
                 f"+{drill['time_to_resolve_s']:.0f} s", ha="left", fontsize=8.5, color=th["ink2"], va="top")
    fig.suptitle("Incident drill: stuck compaction, caught by Prometheus, fixed by the runbook",
                 x=0.01, ha="left", fontsize=11, color=th["ink"])
    fig.tight_layout()
    fig.savefig(OUT / f"incident-{theme}.png", dpi=150, facecolor=th["bg"])
    plt.close(fig)

    # 2. throughput vs SQLite
    wls = ["load", "W", "A", "B", "C"]
    labels = {"load": "load\n(100% insert)", "W": "W\n(100% update)", "A": "A\n(50% read)", "B": "B\n(95% read)",
              "C": "C\n(100% read)"}
    fig, axes = plt.subplots(1, 2, figsize=(12, 3.8), facecolor=th["bg"], sharey=False)
    for ax, tier, title in zip(axes, ("durable", "relaxed"),
                               ("Durable: fsync on every commit", "Relaxed: no fsync per commit")):
        for j, sysname in enumerate(("lsmkv", "sqlite")):
            vals = [next(float(r["ops_per_s"]) for r in bench if r["system"] == sysname and r["tier"] == tier
                         and r["workload"] == w) for w in wls]
            xs = [i + (j - 0.5) * 0.38 for i in range(len(wls))]
            ax.bar(xs, vals, 0.36, color=th["cols"][j], label="lsmkv (Python)" if j == 0 else "SQLite WAL (C)")
        ax.set_yscale("log")
        ax.set_xticks(range(len(wls)))
        ax.set_xticklabels([labels[w] for w in wls], fontsize=9)
        ax.set_title(title, loc="left", fontsize=10.5)
        ax.set_ylabel("ops/s (log)")
        style(ax, th)
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Single-thread, in-process, 100,000 keys, Zipfian requests, every write its own commit",
                 x=0.01, ha="left", fontsize=11, color=th["ink"])
    fig.tight_layout()
    fig.savefig(OUT / f"bench-{theme}.png", dpi=150, facecolor=th["bg"])
    plt.close(fig)
print("ok")
