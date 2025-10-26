#!/usr/bin/env python3
"""
Step 2 — Networking analysis and charts.
- Reads artifacts/networking/scrape_network_metrics_all.csv
- Generates bar charts for: Time(s), #Requests, Bandwidth(KB), Avg Latency(ms)
- Parses each run's ss_log.txt into a simple "connections vs time" timeline
Outputs PNGs to artifacts/networking/charts/
"""

import os, re, glob
import pandas as pd
import matplotlib.pyplot as plt

ART_ROOT = os.environ.get("ART_ROOT", os.path.join(os.getcwd(), "artifacts", "networking"))
METRICS_CSV = os.path.join(ART_ROOT, "scrape_network_metrics_all.csv")
CHART_DIR = os.path.join(ART_ROOT, "charts")
os.makedirs(CHART_DIR, exist_ok=True)

def chart_bar(df, ycol, title, outname):
    plt.figure()
    ax = df.plot(kind="bar", x="Method", y=ycol, legend=False)
    ax.set_title(title)
    ax.set_xlabel("")
    ax.set_ylabel(ycol)
    plt.tight_layout()
    out = os.path.join(CHART_DIR, outname)
    plt.savefig(out, dpi=160)
    plt.close()
    print("📈 Saved", out)

def parse_ss_log(path):
    """
    ss logger wrote lines like:
      <epoch>
      ESTAB ... users:(("python3",pid=...))
      ESTAB ...
      <epoch>
      ...
    We count the lines between timestamp markers.
    """
    times, counts = [], []
    ts_pat = re.compile(r"^\d{10}$")  # epoch seconds line
    count = 0
    current_ts = None
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if ts_pat.match(line):
                if current_ts is not None:
                    times.append(int(current_ts))
                    counts.append(count)
                current_ts = line
                count = 0
            else:
                if line:
                    count += 1
    if current_ts is not None:
        times.append(int(current_ts))
        counts.append(count)
    return pd.DataFrame({"ts": times, "connections": counts})

def chart_ss_timeline(folder):
    ss_path = os.path.join(folder, "ss_log.txt")
    if not os.path.isfile(ss_path):
        return
    df = parse_ss_log(ss_path)
    if df.empty:
        return
    df = df.sort_values("ts")
    # normalize time axis to start at 0s
    t0 = df["ts"].min()
    df["t_rel_s"] = df["ts"] - t0

    method = "UNKNOWN"
    mfile = os.path.join(folder, "method.txt")
    if os.path.isfile(mfile):
        method = open(mfile, "r", encoding="utf-8").read().strip()

    plt.figure()
    plt.plot(df["t_rel_s"], df["connections"], marker="o")
    plt.title(f"Active Python TCP connections vs time — {method}")
    plt.xlabel("Time (s)")
    plt.ylabel("Connections (count)")
    plt.tight_layout()
    out = os.path.join(CHART_DIR, f"ss_timeline_{os.path.basename(folder)}.png")
    plt.savefig(out, dpi=160)
    plt.close()
    print("📉 Saved", out)

def main():
    if not os.path.isfile(METRICS_CSV):
        print("❌ Missing", METRICS_CSV)
        return
    df = pd.read_csv(METRICS_CSV)

    # Bar charts
    chart_bar(df, "Time(s)", "Total scrape time (by method)", "time_by_method.png")
    chart_bar(df, "#Requests", "HTTP request count (by method)", "requests_by_method.png")
    chart_bar(df, "Bandwidth(KB)", "Estimated bandwidth (by method)", "bandwidth_by_method.png")
    chart_bar(df, "Avg Latency(ms)", "Average latency per request (by method)", "latency_by_method.png")

    # ss timelines for each run folder
    for folder in sorted(glob.glob(os.path.join(ART_ROOT, "*_*"))):
        chart_ss_timeline(folder)

    print("\n✅ Networking analysis complete. Charts in", CHART_DIR)

if __name__ == "__main__":
    main()
