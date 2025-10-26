#!/usr/bin/env bash
# Step 2 — Firewall control test (deny 443)
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
SCRAPER_ENTRY="${SCRAPER_ENTRY:-$PROJECT_DIR/step1_scraping_unified.py}"
ART_ROOT="$PROJECT_DIR"
OUT="$ART_ROOT/firewall_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"

echo "🔐 Enabling ufw and denying outbound 443..."
echo "y" | sudo ufw enable >/dev/null 2>&1 || true
sudo ufw deny out to any port 443

echo "▶️  Running a short API scrape under blocked HTTPS..."
set +e
python "$SCRAPER_ENTRY" --no-bs4 --no-selenium --max 5 \
  > "$OUT/scraper_stdout_blocked.log" 2>&1
status=$?
set -e
[[ -f "scrape_network_metrics.csv" ]] && cp "scrape_network_metrics.csv" "$OUT/metrics_blocked.csv" || true
[[ -f "scrape_metrics_summary.json" ]] && cp "scrape_metrics_summary.json" "$OUT/metrics_blocked.json" || true

echo "🔓 Cleaning up firewall rule..."
yes | sudo ufw delete deny out to any port 443
sudo ufw status verbose > "$OUT/ufw_status_after.txt"

echo "Exit code while blocked: $status" | tee "$OUT/result.txt"
echo "✅ Firewall test complete. See: $OUT"
