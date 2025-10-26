#!/usr/bin/env python3
"""
Step 2 — Merge per-run metrics into one CSV (Method, bandwidth(KB), time(s), #requests, avg latency(ms))
Looks under artifacts/networking/<method>_TIMESTAMP/
"""

import json, os, glob, csv

ART_ROOT = os.environ.get("ART_ROOT", os.path.join(os.getcwd(), "artifacts", "networking"))
OUT_CSV  = os.path.join(ART_ROOT, "scrape_network_metrics_all.csv")

def load_metrics(folder):
    method_txt = os.path.join(folder, "method.txt")
    metrics_json = os.path.join(folder, "metrics.json")
    if not (os.path.isfile(method_txt) and os.path.isfile(metrics_json)):
        return None
    with open(method_txt, "r", encoding="utf-8") as f:
        method = f.read().strip()
    with open(metrics_json, "r", encoding="utf-8") as f:
        mj = json.load(f)

    key = method.lower()
    if key == "api": key = "api"
    elif key == "bs4": key = "bs4"
    elif key == "selenium": key = "selenium"
    else: return None

    m = mj.get(key, {})
    # Some runners store elapsed time in m["elapsed_s"]
    elapsed = m.get("elapsed_s", 0)
    return {
        "Method": method,
        "Bandwidth(KB)": round(m.get("bytes", 0) / 1024.0, 2),
        "Time(s)": elapsed,
        "#Requests": m.get("requests", 0),
        "Avg Latency(ms)": round(m.get("avg_latency_ms", 0.0), 2),
        "folder": os.path.basename(folder)
    }

def main():
    rows = []
    for folder in sorted(glob.glob(os.path.join(ART_ROOT, "*_*"))):
        r = load_metrics(folder)
        if r: rows.append(r)

    if not rows:
        print("No metrics found under", ART_ROOT)
        return

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Method","Bandwidth(KB)","Time(s)","#Requests","Avg Latency(ms)","folder"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"✅ Wrote {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
