#!/usr/bin/env bash
# One-off: give the GitHub runner what it needs to build and deploy.
#   - Docker (the node has containerd only; the runner needs a build engine)
#   - helm  (the runner user needs it on PATH)
#   - adds the runner user to the docker group
#
# Run once, after script 04.
set -euo pipefail
if [[ $EUID -ne 0 ]]; then echo "Run with sudo: sudo $0"; exit 1; fi

RUNNER_USER=ghrunner
log() { echo -e "\033[1;32m[+]\033[0m $*"; }

if ! command -v docker >/dev/null; then
  log "Installing Docker"
  apt-get update -y
  apt-get install -y docker.io
  systemctl enable --now docker
else
  log "Docker already present: $(docker --version)"
fi

log "Adding ${RUNNER_USER} to the docker group"
usermod -aG docker "${RUNNER_USER}"

if ! command -v helm >/dev/null; then
  log "Installing Helm"
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

log "Restarting the runner so it picks up the new group"
systemctl restart "$(systemctl list-units --all --no-legend 'actions.runner.*' | awk '{print $1}' | head -1)"

sleep 5
log "Checks (run as ${RUNNER_USER})"
sudo -u "${RUNNER_USER}" docker ps >/dev/null 2>&1 \
  && echo "  OK   docker works" || echo "  FAIL docker - check group membership"
sudo -u "${RUNNER_USER}" helm version --short >/dev/null 2>&1 \
  && echo "  OK   helm works"   || echo "  FAIL helm not on PATH"
sudo -u "${RUNNER_USER}" env KUBECONFIG=/home/${RUNNER_USER}/.kube/config \
  kubectl auth can-i create deployments -n ml-apps
