#!/usr/bin/env bash
# Step 3a — UFW: allow ONLY inbound HTTP/HTTPS; deny all other inbound.
# Usage:
#   bash step3a_ufw_webonly.sh apply           # web-only inbound (NO SSH)
#   bash step3a_ufw_webonly.sh apply --allow-ssh  # web-only + allow SSH (port 22)
#   bash step3a_ufw_webonly.sh status
#   bash step3a_ufw_webonly.sh restore         # restore defaults (deny incoming, allow outgoing)

set -euo pipefail

cmd="${1:-}"
allow_ssh="${2:-}"

apply_rules() {
  echo "⚙️  Applying UFW web-only inbound policy..."
  echo "y" | sudo ufw enable >/dev/null 2>&1 || true

  # Reset to sane defaults: deny inbound, allow outbound
  sudo ufw default deny incoming
  sudo ufw default allow outgoing

  # Allow ONLY standard web inbound
  sudo ufw allow in 80/tcp
  sudo ufw allow in 443/tcp

  # (Optional) keep SSH open to avoid lockout
  if [[ "$allow_ssh" == "--allow-ssh" ]]; then
    sudo ufw allow in 22/tcp
    echo "🔐 SSH (22/tcp) allowed inbound."
  else
    echo "🚫 SSH inbound NOT allowed (use --allow-ssh to enable)."
  fi

  sudo ufw reload
  sudo ufw status verbose
  echo "✅ UFW web-only policy applied."
}

restore_defaults() {
  echo "🧹 Restoring UFW defaults (deny incoming, allow outgoing) and removing specific rules..."
  echo "y" | sudo ufw enable >/dev/null 2>&1 || true
  sudo ufw --force reset
  sudo ufw default deny incoming
  sudo ufw default allow outgoing
  sudo ufw reload
  sudo ufw status verbose
  echo "✅ UFW restored to defaults."
}

case "$cmd" in
  apply)   apply_rules ;;
  status)  sudo ufw status verbose ;;
  restore) restore_defaults ;;
  *)
    echo "Usage:"
    echo "  $0 apply [--allow-ssh]"
    echo "  $0 status"
    echo "  $0 restore"
    exit 1
    ;;
esac
