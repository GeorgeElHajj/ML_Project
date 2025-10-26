#!/usr/bin/env bash
# Step 3 — security scenario captures (tshark/tcpdump/nload/ss)
# Usage:
#   bash step3_security_capture.sh ufw-webonly
#   bash step3_security_capture.sh multithread --max 40
#   bash step3_security_capture.sh proxy --max 30 --proxy tor

set -euo pipefail
SCENARIO="${1:-}"
shift || true

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
ART_ROOT="$PROJECT_DIR/artifacts/security"
mkdir -p "$ART_ROOT"

detect_iface() {
  local iface
  iface="$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')"
  echo "${iface:-ens33}"
}
IFACE="${NET_IFACE:-$(detect_iface)}"
STAMP="$(date +%Y%m%d-%H%M%S)"

run_monitors() {
  local outdir="$1"; local dur="${2:-120}"
  ( while true; do echo "$(date +%s)"; ss -t -a -p 2>/dev/null | grep -i python || true; sleep 2; done ) >> "$outdir/ss_log.txt" &
  echo $!
}
start_pcap() {
  local outdir="$1"; local dur="${2:-120}"
  sudo timeout "$dur" tcpdump -i "$IFACE" '(tcp port 80 or 443)' -w "$outdir/trace_80_443.pcap" >/dev/null 2>&1 &
  echo $!
}
start_tshark() {
  local outdir="$1"; local dur="${2:-120}"
  sudo timeout "$dur" tshark -i "$IFACE" -f "tcp and (port 80 or 443)" \
    -q -z io,phs -z conv,tcp > "$outdir/tshark_summary.txt" 2>&1 &
  echo $!
}
start_nload() {
  local outdir="$1"; local dur="${2:-120}"
  script -q -c "sudo timeout $dur nload -u M -t 1000 -m $IFACE" "$outdir/nload_raw.txt" >/dev/null 2>&1 &
  echo $!
}
cleanup_nload() {
  local outdir="$1"
  sed -r 's/\x1B\[[0-9;]*[A-Za-z]//g' "$outdir/nload_raw.txt" | sed 's/\r/\n/g' > "$outdir/nload_clean.txt" || true
}

case "$SCENARIO" in
  ufw-webonly)
    OUT="$ART_ROOT/ufw_webonly_$STAMP"; mkdir -p "$OUT"
    echo "▶️  Applying web-only inbound policy…"
    bash step3a_ufw_webonly.sh apply --allow-ssh | tee "$OUT/ufw_apply.log"
    sudo ufw status verbose > "$OUT/ufw_status.txt"

    echo "▶️  Capturing for 120s… (generate inbound tests from another machine)"
    P1=$(run_monitors "$OUT" 120)
    P2=$(start_pcap "$OUT" 120)
    P3=$(start_tshark "$OUT" 120)
    P4=$(start_nload "$OUT" 120)
    wait || true; cleanup_nload "$OUT"

    echo "Logs: $OUT (include in report)"
    ;;

  multithread)
    OUT="$ART_ROOT/multithread_$STAMP"; mkdir -p "$OUT"
    echo "▶️  Running Step-3b multithread captures…"
    P1=$(run_monitors "$OUT" 200)
    P2=$(start_pcap "$OUT" 200)
    P3=$(start_tshark "$OUT" 200)
    P4=$(start_nload "$OUT" 200)

    python step3b_multithreading_scrapers.py "$@" | tee "$OUT/run.log"

    cp mt_movies_api.csv mt_movies_bs4.csv mt_movies_selenium_mp.csv "$OUT/" 2>/dev/null || true
    cp mt_scrape_network_metrics.csv mt_scrape_metrics_summary.json "$OUT/" 2>/dev/null || true
    wait || true; cleanup_nload "$OUT"
    echo "✅ Artifacts saved to $OUT"
    ;;

  proxy)
    OUT="$ART_ROOT/proxy_$STAMP"; mkdir -p "$OUT"
    echo "▶️  Running Step-3c Tor proxy captures…"
    P1=$(run_monitors "$OUT" 200)
    P2=$(start_pcap "$OUT" 200)
    P3=$(start_tshark "$OUT" 200)
    P4=$(start_nload "$OUT" 200)

    # Additional captures for the Tor proxy on port 9050
    P5=$(sudo timeout 200 tcpdump -i "$IFACE" 'tcp and (host 127.0.0.1 and port 9050)' \
      -w "$OUT/trace_socks9050.pcap" >/dev/null 2>&1 &)
    P6=$(sudo timeout 200 tshark -i "$IFACE" -f 'tcp and (host 127.0.0.1 and port 9050)' \
      -q -z io,phs -z conv,tcp > "$OUT/tshark_socks9050.txt" 2>&1 &)

    python step3c_proxy_tor_runner.py "$@" | tee "$OUT/run.log"

    cp proxy_movies_api.csv proxy_movies_bs4.csv proxy_movies_selenium.csv "$OUT/" 2>/dev/null || true
    cp proxy_scrape_network_metrics.csv proxy_scrape_metrics_summary.json "$OUT/" 2>/dev/null || true
    wait || true; cleanup_nload "$OUT"
    echo "✅ Artifacts saved to $OUT"
    ;;

  *)
    echo "Usage:"
    echo "  $0 ufw-webonly"
    echo "  $0 multithread [--max N]"
    echo "  $0 proxy [--max N] [--proxy tor|none]"
    exit 1
    ;;
esac