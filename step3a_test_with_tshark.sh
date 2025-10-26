#!/usr/bin/env bash
# Step 3a test — verify UFW "web-only inbound" using tshark/tcpdump/nload + UFW logs
# Requires: sudo, tshark, tcpdump, nload
# Usage:
#   bash step3a_test_with_tshark.sh        # runs a 90s capture window

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"
ART_ROOT="$PROJECT_DIR/artifacts/security/ufw_webonly_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$ART_ROOT"

detect_iface() {
  local iface
  iface="$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')"
  echo "${iface:-ens33}"
}
IFACE="${NET_IFACE:-$(detect_iface)}"
MYIP="${MYIP:-$(hostname -I | awk '{print $1}')}"
echo "Interface: $IFACE    IP: $MYIP"

# 1) Apply UFW policy (allow only inbound 80/443); keep SSH open to avoid lockout
bash step3a_ufw_webonly.sh apply --allow-ssh | tee "$ART_ROOT/ufw_apply.log"
sudo ufw logging on

# 2) Start captures (90s window)
echo "Starting captures (90s) … artifacts → $ART_ROOT"
sudo timeout 90 tshark -i "$IFACE" -f "tcp and (port 80 or port 443) and dst host $MYIP" \
  -w "$ART_ROOT/allowed_80_443.pcapng" > "$ART_ROOT/tshark_allowed.log" 2>&1 &

sudo timeout 90 tshark -i "$IFACE" -f "tcp and not (port 80 or port 443) and dst host $MYIP" \
  -w "$ART_ROOT/blocked_other_ports.pcapng" > "$ART_ROOT/tshark_blocked.log" 2>&1 &

sudo timeout 90 tcpdump -i "$IFACE" -w "$ART_ROOT/failsafe_full.pcap" >/dev/null 2>&1 &

# capture nload screen (bandwidth over time)
script -q -c "sudo timeout 90 nload -u M -t 1000 -m $IFACE" "$ART_ROOT/nload_raw.txt" >/dev/null 2>&1 || true
sed -r 's/\x1B\[[0-9;]*[A-Za-z]//g' "$ART_ROOT/nload_raw.txt" | sed 's/\r/\n/g' > "$ART_ROOT/nload_clean.txt" || true

echo
echo "▶️  While captures run (90s), from another machine run:"
echo "   curl -I http://$MYIP/              # should work (HTTP allowed)"
echo "   curl -I http://$MYIP:8080/         # should fail (not allowed)"
echo "   nc -vz $MYIP 80                    # should connect"
echo "   nc -vz $MYIP 22                    # should be blocked"
echo "Or: nmap -Pn -p 22,80,443,8080 $MYIP"
echo

wait || true

# 3) Summaries for the report
if command -v tshark >/dev/null 2>&1; then
  tshark -r "$ART_ROOT/allowed_80_443.pcapng" -q -z conv,tcp -z io,phs > "$ART_ROOT/summary_allowed.txt" || true
  tshark -r "$ART_ROOT/blocked_other_ports.pcapng" -q -z conv,tcp -z io,phs > "$ART_ROOT/summary_blocked.txt" || true
fi

# 4) Collect UFW logs (blocked attempts will appear here)
LOGOUT="$ART_ROOT/ufw_log_excerpt.txt"
if [[ -f /var/log/ufw.log ]]; then
  sudo tail -n 500 /var/log/ufw.log > "$LOGOUT"
else
  sudo journalctl -u ufw --since "1 hour ago" > "$LOGOUT" || true
fi

# 5) Restore defaults if you want (comment out to keep policy)
# bash step3a_ufw_webonly.sh restore | tee "$ART_ROOT/ufw_restore.log"

echo "✅ Done. Add these to your notebook/report:"
echo "   - $ART_ROOT/summary_allowed.txt (allowed 80/443 conversations)"
echo "   - $ART_ROOT/summary_blocked.txt (attempts to other ports)"
echo "   - $ART_ROOT/nload_clean.txt (bandwidth over time)"
echo "   - $ART_ROOT/ufw_log_excerpt.txt (denied packets)"
