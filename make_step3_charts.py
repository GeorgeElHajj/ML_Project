#!/usr/bin/env python3
"""
Step‑3 Charts — CLI
Usage:
  python make_step3_charts.py

Finds latest metrics/artifacts and saves charts into ./charts
"""

import os, re, glob, json, math, pathlib
import pandas as pd
import matplotlib.pyplot as plt

# Do NOT set any specific colors or styles per constraints.

def _latest(paths):
    files = []
    for p in paths:
        files.extend(glob.glob(p, recursive=True))
    return max(files, key=os.path.getmtime) if files else None

def _normalize_methods(df, col="Method"):
    def norm(m):
        m = str(m)
        if "API" in m: return "API"
        if "BS4" in m: return "BS4"
        if "Selenium" in m: return "Selenium"
        return m
    df = df.copy()
    df["MethodNorm"] = df[col].map(norm)
    return df

def _load_metrics_csv(path, scenario):
    df = pd.read_csv(path)
    df = _normalize_methods(df, "Method")
    df["Scenario"] = scenario
    return df[["MethodNorm","Scenario","Bandwidth(KB)","Time(s)","#Requests","Avg Latency(ms)"]]

def load_baseline_and_mt():
    base = None; mt = None
    base_path = _latest(["scrape_network_metrics_all.csv", "artifacts/**/scrape_network_metrics_all.csv"])
    if base_path:
        base = _load_metrics_csv(base_path, "baseline")
    mt_path = _latest(["mt_scrape_network_metrics.csv", "artifacts/security/multithread_*/mt_scrape_network_metrics.csv"])
    if mt_path:
        mt = _load_metrics_csv(mt_path, "multithread")
    return base, mt, {"baseline": base_path, "multithread": mt_path}

def load_proxy_direct_and_tor():
    # Preferred explicit files if you created them
    direct = None; tor = None
    direct_path = _latest(["proxy_metrics_direct.csv"])
    tor_path    = _latest(["proxy_metrics_tor.csv"])

    # Otherwise, discover newest proxy_* folders and read summary json to classify
    discovered = {}
    for d in sorted(glob.glob("artifacts/security/proxy_*"), key=os.path.getmtime, reverse=True):
        summ = os.path.join(d, "proxy_scrape_metrics_summary.json")
        csvm = os.path.join(d, "proxy_scrape_network_metrics.csv")
        if os.path.isfile(summ) and os.path.isfile(csvm):
            try:
                meta = json.load(open(summ, "r", encoding="utf-8"))
                mode = meta.get("proxy")
                if mode == "tor" and "tor" not in discovered:
                    discovered["tor"] = csvm
                elif (mode == "none" or mode == "direct") and "direct" not in discovered:
                    discovered["direct"] = csvm
            except Exception:
                continue
        if len(discovered) == 2:
            break

    if not direct_path and "direct" in discovered:
        direct_path = discovered["direct"]
    if not tor_path and "tor" in discovered:
        tor_path = discovered["tor"]

    if direct_path and os.path.isfile(direct_path):
        direct = _load_metrics_csv(direct_path, "proxy_direct")
    if tor_path and os.path.isfile(tor_path):
        tor = _load_metrics_csv(tor_path, "proxy_tor")
    return direct, tor, {"proxy_direct": direct_path, "proxy_tor": tor_path}

def delta_table(a_df, b_df, metric):
    if a_df is None or b_df is None:
        return pd.DataFrame()
    a = a_df.pivot_table(index="MethodNorm", values=metric, aggfunc="first")
    b = b_df.pivot_table(index="MethodNorm", values=metric, aggfunc="first")
    m = a.join(b, lsuffix=f" ({a_df['Scenario'].iloc[0]})", rsuffix=f" ({b_df['Scenario'].iloc[0]})")
    m["Δ"]  = m.iloc[:,1] - m.iloc[:,0]
    with pd.option_context('mode.use_inf_as_na', True):
        m["Δ%"] = (100.0 * m["Δ"] / m.iloc[:,0]).replace([pd.NA, pd.NaT], 0.0)
    return m.round(2)

def bar_compare(a_df, b_df, metric, title, outfile):
    if a_df is None or b_df is None:
        print(f"[skip] Not enough data to plot {title}")
        return
    left  = a_df[["MethodNorm", metric]].rename(columns={metric: a_df["Scenario"].iloc[0]})
    right = b_df[["MethodNorm", metric]].rename(columns={metric: b_df["Scenario"].iloc[0]})
    merged = left.merge(right, on="MethodNorm", how="inner").set_index("MethodNorm")
    # Plot grouped bars (one chart per metric per comparison)
    ax = merged.plot(kind="bar")
    ax.set_title(title)
    ax.set_ylabel(metric)
    ax.set_xlabel("Method")
    fig = ax.get_figure()
    os.makedirs("charts", exist_ok=True)
    fig.savefig(os.path.join("charts", outfile), bbox_inches="tight", dpi=150)
    plt.close(fig)

# -------- nload + pcap helpers --------
def _parse_nload_avg_mbps(nload_clean_txt):
    # Return dict with {'avg_in_mbps': float, 'avg_out_mbps': float} from nload_clean.txt.
    if not nload_clean_txt or not os.path.isfile(nload_clean_txt):
        return None
    avg_in = None; avg_out = None

    # We will parse the *last* reported Avg lines.
    unit_factor = {"kbit/s": 1/1000.0, "kBit/s": 1/1000.0, "mbit/s": 1.0, "MBit/s": 1.0, "gbit/s": 1000.0, "GBit/s": 1000.0}
    with open(nload_clean_txt, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line_l = line.strip()
            # Example: "In:  Curr: 12.34 kBit/s  Avg: 56.78 kBit/s  Min: ...  Max: ..."
            m_in = re.search(r"^\s*In:.*?Avg:\s*([0-9.]+)\s*([kKmMgG][Bb]it/s)", line_l)
            if m_in:
                val = float(m_in.group(1)); unit = m_in.group(2)
                factor = unit_factor.get(unit, 1.0 if "M" in unit else 0.001 if "k" in unit else 1.0)
                avg_in = val * factor
            m_out = re.search(r"^\s*Out:.*?Avg:\s*([0-9.]+)\s*([kKmMgG][Bb]it/s)", line_l)
            if m_out:
                val = float(m_out.group(1)); unit = m_out.group(2)
                factor = unit_factor.get(unit, 1.0 if "M" in unit else 0.001 if "k" in unit else 1.0)
                avg_out = val * factor
    if avg_in is None and avg_out is None:
        return None
    return {"avg_in_mbps": avg_in or 0.0, "avg_out_mbps": avg_out or 0.0}

def _pcap_size_mb(pcap_path):
    if not pcap_path or not os.path.isfile(pcap_path):
        return None
    return os.path.getsize(pcap_path) / (1024.0*1024.0)

def discover_artifacts():
    # Return dict with latest mt and proxy artifact dirs + helpful file paths if present.
    out = {"multithread": None, "proxy": None}
    mt_dir = _latest(["artifacts/security/multithread_*"])
    pr_dir = _latest(["artifacts/security/proxy_*"])
    out["multithread"] = mt_dir
    out["proxy"] = pr_dir
    # Attach known files if they exist
    def add_files(d):
        if not d: return {}
        return {
            "nload_clean": os.path.join(d, "nload_clean.txt"),
            "pcap_80443": os.path.join(d, "trace_80_443.pcap"),
            "tshark_summary": os.path.join(d, "tshark_summary.txt"),
            "pcap_socks9050": os.path.join(d, "trace_socks9050.pcap"),
            "tshark_socks9050": os.path.join(d, "tshark_socks9050.txt"),
        }
    out["mt_files"] = add_files(mt_dir)
    out["proxy_files"] = add_files(pr_dir)
    return out

def nload_pcap_summary():
    art = discover_artifacts()
    rows = []
    if art["multithread"]:
        n = _parse_nload_avg_mbps(art["mt_files"].get("nload_clean"))
        p1 = _pcap_size_mb(art["mt_files"].get("pcap_80443"))
        if n or p1 is not None:
            rows.append({"Scenario":"multithread","avg_in_mbps":(n or {}).get("avg_in_mbps",None),
                         "avg_out_mbps":(n or {}).get("avg_out_mbps",None),
                         "pcap_80_443_MB":p1})
    if art["proxy"]:
        n = _parse_nload_avg_mbps(art["proxy_files"].get("nload_clean"))
        p1 = _pcap_size_mb(art["proxy_files"].get("pcap_80443"))
        p2 = _pcap_size_mb(art["proxy_files"].get("pcap_socks9050"))
        total = (p1 or 0.0) + (p2 or 0.0) if (p1 or p2) else None
        if n or total is not None:
            rows.append({"Scenario":"proxy_run","avg_in_mbps":(n or {}).get("avg_in_mbps",None),
                         "avg_out_mbps":(n or {}).get("avg_out_mbps",None),
                         "pcap_total_MB":total})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    base, mt, paths_bm = load_baseline_and_mt()
    direct, tor, paths_pt = load_proxy_direct_and_tor()

    print("Loaded:")
    print(paths_bm)
    print(paths_pt)

    os.makedirs("charts", exist_ok=True)

    # Baseline vs Multithread
    bar_compare(base, mt, "Time(s)",          "Multithread vs Baseline — Time(s)",            "mt_vs_base_time.png")
    bar_compare(base, mt, "Avg Latency(ms)",  "Multithread vs Baseline — Avg Latency (ms)",   "mt_vs_base_latency.png")
    bar_compare(base, mt, "Bandwidth(KB)",    "Multithread vs Baseline — Bandwidth (KB)",     "mt_vs_base_bandwidth.png")
    bar_compare(base, mt, "#Requests",        "Multithread vs Baseline — #Requests",          "mt_vs_base_requests.png")

    # Direct vs Tor
    bar_compare(direct, tor, "Time(s)",          "Tor vs Direct — Time(s)",            "tor_vs_direct_time.png")
    bar_compare(direct, tor, "Avg Latency(ms)",  "Tor vs Direct — Avg Latency (ms)",   "tor_vs_direct_latency.png")
    bar_compare(direct, tor, "Bandwidth(KB)",    "Tor vs Direct — Bandwidth (KB)",     "tor_vs_direct_bandwidth.png")
    bar_compare(direct, tor, "#Requests",        "Tor vs Direct — #Requests",          "tor_vs_direct_requests.png")

    # Delta tables
    delta_mt_time   = delta_table(base, mt, "Time(s)")
    delta_mt_lat    = delta_table(base, mt, "Avg Latency(ms)")
    delta_mt_bw     = delta_table(base, mt, "Bandwidth(KB)")
    delta_mt_req    = delta_table(base, mt, "#Requests")

    delta_tor_time  = delta_table(direct, tor, "Time(s)")
    delta_tor_lat   = delta_table(direct, tor, "Avg Latency(ms)")
    delta_tor_bw    = delta_table(direct, tor, "Bandwidth(KB)")
    delta_tor_req   = delta_table(direct, tor, "#Requests")

    if not delta_mt_time.empty:
        delta_mt_time.to_csv("charts/delta_multithread_time.csv")
        delta_mt_lat.to_csv("charts/delta_multithread_latency.csv")
        delta_mt_bw.to_csv("charts/delta_multithread_bandwidth.csv")
        delta_mt_req.to_csv("charts/delta_multithread_requests.csv")
    if not delta_tor_time.empty:
        delta_tor_time.to_csv("charts/delta_tor_time.csv")
        delta_tor_lat.to_csv("charts/delta_tor_latency.csv")
        delta_tor_bw.to_csv("charts/delta_tor_bandwidth.csv")
        delta_tor_req.to_csv("charts/delta_tor_requests.csv")

    # ---- Combined Comparison: Baseline vs Multithread vs Tor ----
    def combine_three(base_df, mt_df, tor_df):
        dfs = []
        if base_df is not None: dfs.append(base_df.assign(Source="Baseline"))
        if mt_df is not None: dfs.append(mt_df.assign(Source="Multithread"))
        if tor_df is not None: dfs.append(tor_df.assign(Source="Tor Proxy"))
        if not dfs: return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    combined_df = combine_three(base, mt, tor)

    if not combined_df.empty:
        for metric in ["Time(s)", "Avg Latency(ms)", "Bandwidth(KB)", "#Requests"]:
            pivoted = combined_df.pivot_table(index="MethodNorm", columns="Source", values=metric, aggfunc="first")
            ax = pivoted.plot(kind="bar")
            ax.set_title(f"Baseline vs Multithread vs Tor — {metric}")
            ax.set_ylabel(metric)
            ax.set_xlabel("Scraping Method")
            fig = ax.get_figure()
            fig.savefig(os.path.join("charts", f"combined_{metric.replace(' ','_').replace('(','').replace(')','')}.png"),
                        bbox_inches="tight", dpi=150)
            plt.close(fig)

    # Optional: nload + pcap
    np_df = nload_pcap_summary()
    if not np_df.empty:
        print("\n[nload/pcap]")
        print(np_df)
        for col in ["avg_in_mbps", "avg_out_mbps"]:
            if col in np_df.columns and np_df[col].notna().any():
                ax = np_df.set_index("Scenario")[col].plot(kind="bar")
                ax.set_title(f"nload — {col.replace('_',' ')}")
                ax.set_ylabel("Mbit/s")
                ax.set_xlabel("Scenario")
                fig = ax.get_figure()
                fig.savefig(os.path.join("charts", f"{col}.png"), bbox_inches="tight", dpi=150)
                plt.close(fig)

        pcap_col = "pcap_total_MB" if "pcap_total_MB" in np_df.columns else "pcap_80_443_MB"
        if pcap_col in np_df.columns and np_df[pcap_col].notna().any():
            ax = np_df.set_index("Scenario")[pcap_col].plot(kind="bar")
            ax.set_title("pcap size (MB)")
            ax.set_ylabel("MB")
            ax.set_xlabel("Scenario")
            fig = ax.get_figure()
            fig.savefig(os.path.join("charts", "pcap_size_mb.png"), bbox_inches="tight", dpi=150)
            plt.close(fig)

    print("✅ Charts saved under ./charts")
