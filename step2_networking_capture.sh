#!/usr/bin/env bash
# Step 2 — Networking capture orchestration
# Runs each scraping method and collects: ss, iftop, nload, tcpdump (+tshark)
# Outputs per-method artifacts under artifacts/networking/<method>_<timestamp>/

set -euo pipefail

# --------- CONFIG ---------
PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
VENV_PATH="${VENV_PATH:-$PROJECT_DIR/scraper_env}"   # adjust if different
SCRAPER_ENTRY="${SCRAPER_ENTRY:-$PROJECT_DIR/step1_scraping_unified.py}"  # or LabNetwork.py
MAX_MOVIES="${MAX_MOVIES:-30}"     # per run; keep modest so captures fit
DURATION="${DURATION:-90}"         # seconds to run iftop/nload during scrape
IFTOP_S="${IFTOP_S:-30}"           # iftop stats window (seconds)
ART_ROOT="$PROJECT_DIR/artifacts/networking"
mkdir -p "$ART_ROOT"

# --------- FUNCTIONS ---------
detect_iface() {
  local iface
  iface="$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')"
  if [[ -z "${iface:-}" ]]; then
    iface="ens33"   # fallback if detection fails
  fi
  echo "$iface"
}

start_ss_logger() {
  local outfile="$1"
  # timestamp and active python connections; -p includes process name/pid
  ( while true; do
      printf "%s\n" "$(date +%s)"
      ss -t -a -p 2>/dev/null | grep -i python || true
      sleep 2
    done ) >> "$outfile" &
  echo $!
}

start_tcpdump() {
  local pcap="$1"
  sudo tcpdump -i any '(tcp port 80 or 443)' -w "$pcap" >/dev/null 2>&1 &
  echo $!
}

start_iftop() {
  local iface="$1" ; local outfile="$2"
  # -t text mode; -s sample seconds; wrap with timeout
  sudo timeout "$DURATION" iftop -t -s "$IFTOP_S" -i "$iface" > "$outfile" 2>&1 &
  echo $!
}

start_nload() {
  local iface="$1" ; local rawfile="$2"
  # capture interactive screen to text using 'script'; later we clean ANSI codes
  script -q -c "sudo timeout $DURATION nload -u M -t 1000 -m $iface" "$rawfile" >/dev/null 2>&1 &
  echo $!
}

clean_nload() {
  local rawfile="$1" ; local cleanfile="$2"
  # strip ANSI escape sequences; normalize carriage returns
  sed -r 's/\x1B\[[0-9;]*[A-Za-z]//g' "$rawfile" | sed 's/\r/\n/g' > "$cleanfile" || true
}

tshark_summary() {
  local pcap="$1" ; local outfile="$2"
  if command -v tshark >/dev/null 2>&1; then
    tshark -r "$pcap" -q -z io,phs -z conv,tcp > "$outfile" 2>&1 || true
  else
    echo "tshark not installed; skipping summary." > "$outfile"
  fi
}

run_method() {
  local method="$1"         # API | BS4 | Selenium
  local iface="$2"
  local stamp
  stamp="$(date +%Y%m%d-%H%M%S)"
  local outdir="$ART_ROOT/${method,,}_$stamp"
  mkdir -p "$outdir"

  echo "$method" > "$outdir/method.txt"
  echo "▶️  Running $method (iface=$iface) — artifacts → $outdir"

  # --- Start monitors ---
  local ss_pid tcpdump_pid iftop_pid nload_pid
  ss_pid=$(start_ss_logger "$outdir/ss_log.txt")
  tcpdump_pid=$(start_tcpdump "$outdir/scraper_trace.pcap")
  iftop_pid=$(start_iftop "$iface" "$outdir/iftop_output.txt")
  nload_pid=$(start_nload "$iface" "$outdir/nload_raw.txt")

  # --- Run scraper (one method at a time) ---
  # Uses step1 runner outputs (movies_*.csv, scrape_* files)
  pushd "$PROJECT_DIR" >/dev/null

  # Build flags so we run only the selected method
  local flags=("--max" "$MAX_MOVIES")
  case "$method" in
    API)      flags+=("--no-bs4" "--no-selenium") ;;
    BS4)      flags+=("--no-api" "--no-selenium") ;;
    Selenium) flags+=("--no-api" "--no-bs4") ;;
    *) echo "Unknown method: $method"; exit 1 ;;
  esac

  # optional venv
  if [[ -f "$VENV_PATH/bin/activate" ]]; then
    # shellcheck disable=SC1090
    source "$VENV_PATH/bin/activate"
  fi

  python "$SCRAPER_ENTRY" "${flags[@]}" | tee "$outdir/scraper_stdout.log"

  # --- Collect outputs ---
  # metrics from step1
  [[ -f "scrape_network_metrics.csv" ]] && cp "scrape_network_metrics.csv" "$outdir/metrics.csv"
  [[ -f "scrape_metrics_summary.json" ]] && cp "scrape_metrics_summary.json" "$outdir/metrics.json"

  # per-method CSV (only one should be present)
  case "$method" in
    API)      [[ -f "movies_api.csv" ]] && cp "movies_api.csv" "$outdir/";;
    BS4)      [[ -f "movies_bs4.csv" ]] && cp "movies_bs4.csv" "$outdir/";;
    Selenium) [[ -f "movies_selenium.csv" ]] && cp "movies_selenium.csv" "$outdir/";;
  esac

  popd >/dev/null

  # --- Stop monitors ---
  for pid in "$iftop_pid" "$nload_pid"; do
    if kill -0 "$pid" >/dev/null 2>&1; then kill "$pid" || true; fi
  done
  if kill -0 "$tcpdump_pid" >/dev/null 2>&1; then sudo kill "$tcpdump_pid" || true; fi
  if kill -0 "$ss_pid" >/dev/null 2>&1; then kill "$ss_pid" || true; fi

  # --- Post-process ---
  clean_nload "$outdir/nload_raw.txt" "$outdir/nload_clean.txt"
  tshark_summary "$outdir/scraper_trace.pcap" "$outdir/tshark_summary.txt"

  echo "✅ Completed $method. Artifacts in: $outdir"
}

main() {
  mkdir -p "$ART_ROOT"
  local iface="${NET_IFACE:-$(detect_iface)}"
  echo "Detected interface: $iface  (override by exporting NET_IFACE=...)"

  # Run each method separately to get clean, comparable captures
  run_method "API" "$iface"
  run_method "BS4" "$iface"
  run_method "Selenium" "$iface"

  echo
  echo "Next: merge metrics and make charts:"
  echo "  python step2_merge_metrics.py"
  echo "  python step2_networking_analyze.py"
}

main "$@"
