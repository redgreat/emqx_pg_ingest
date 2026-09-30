#!/usr/bin/env bash
# EMQX PG Ingest 发布/部署脚本。
#   ./scripts/release.sh -t v0.1.0       推送标签，触发 Release + GHCR
#   ./scripts/release.sh                 拉取 latest 并部署
#   ./scripts/release.sh --no-pull       使用本地镜像部署
#   ./scripts/release.sh --sync-config   覆盖部署目录中的插件配置
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-/vol1/1000/docker/emqx_pg_ingest}"
IMAGE="${IMAGE:-ghcr.io/redgreat/emqx_pg_ingest:latest}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-120}"

DO_TAG=""
DO_PULL=1
SYNC_CONFIG=0
DO_HEALTH=1
FOLLOW_LOGS=0

log()  { printf '[release] %s\n' "$*"; }
ok()   { printf '  OK  %s\n' "$*"; }
warn() { printf '  !!  %s\n' "$*" >&2; }
die()  { printf '  XX  %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: ./scripts/release.sh [options]
  -t, --tag vX.Y.Z       Push release tag; use auto/+ to increment patch
  -d, --deploy-dir DIR   Deployment directory
  --image IMAGE          Container image (default GHCR latest)
  --no-pull              Do not pull image
  --sync-config          Replace deployed emqx_pg_ingest.json
  --no-health            Skip health/plugin checks
  --logs                 Follow logs after deployment
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--tag) DO_TAG="${2:-}"; shift 2 ;;
    -d|--deploy-dir) DEPLOY_DIR="${2:-}"; shift 2 ;;
    --image) IMAGE="${2:-}"; shift 2 ;;
    --no-pull) DO_PULL=0; shift ;;
    --sync-config) SYNC_CONFIG=1; shift ;;
    --no-health) DO_HEALTH=0; shift ;;
    --logs|-f) FOLLOW_LOGS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
done

command -v git >/dev/null 2>&1 || die "git not found"
command -v docker >/dev/null 2>&1 || die "docker not found"
docker compose version >/dev/null 2>&1 || die "docker compose v2 not found"

if [[ -n "$DO_TAG" ]]; then
  cd "$REPO_ROOT"
  [[ -z "$(git status --porcelain)" ]] || die "Working tree is not clean; commit changes first"
  git fetch --tags origin
  version="$DO_TAG"
  if [[ "$version" == "auto" || "$version" == "+" ]]; then
    latest="$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-version:refname | head -n1)"
    if [[ -z "$latest" ]]; then
      version="v0.1.0"
    else
      IFS=. read -r major minor patch <<< "${latest#v}"
      version="v${major}.${minor}.$((patch + 1))"
    fi
  fi
  [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "Tag must be vMAJOR.MINOR.PATCH"
  git rev-parse -q --verify "refs/tags/$version" >/dev/null && die "Tag already exists: $version"
  git tag -a "$version" -m "Release $version"
  git push origin "$version"
  ok "Pushed $version; GitHub Actions is building the plugin package and GHCR image"
  exit 0
fi

mkdir -p "$DEPLOY_DIR/etc" "$DEPLOY_DIR/data" "$DEPLOY_DIR/log"
cp "$REPO_ROOT/docker-compose.yml" "$DEPLOY_DIR/docker-compose.yml"
if [[ ! -f "$DEPLOY_DIR/etc/emqx_pg_ingest.json" || "$SYNC_CONFIG" -eq 1 ]]; then
  cp "$REPO_ROOT/priv/emqx_pg_ingest.json" "$DEPLOY_DIR/etc/emqx_pg_ingest.json"
  warn "Review PG credentials in $DEPLOY_DIR/etc/emqx_pg_ingest.json before production use"
fi

cd "$DEPLOY_DIR"
export EMQX_IMAGE="$IMAGE"
if [[ "$DO_PULL" -eq 1 ]]; then
  log "Pulling $IMAGE"
  docker pull "$IMAGE"
fi
pull_mode="$([[ "$DO_PULL" -eq 1 ]] && echo always || echo never)"
docker compose up -d --remove-orphans --pull="$pull_mode"
ok "EMQX container started"

if [[ "$DO_HEALTH" -eq 1 ]]; then
  waited=0
  until docker exec emqx /opt/emqx/bin/emqx ctl status >/dev/null 2>&1; do
    (( waited >= HEALTH_TIMEOUT )) && die "EMQX did not become healthy in ${HEALTH_TIMEOUT}s"
    sleep 3
    waited=$((waited + 3))
  done
  plugins="$(docker exec emqx /opt/emqx/bin/emqx ctl plugins list 2>&1)"
  printf '%s\n' "$plugins"
  grep -q 'emqx_pg_ingest' <<<"$plugins" || die "emqx_pg_ingest is not installed"
  docker logs emqx 2>&1 | grep -q '\[emqx_pg_ingest\] plugin started' ||
    die "Plugin is installed but startup confirmation was not found in logs"
  ok "emqx_pg_ingest is running"
fi

docker compose ps
if [[ "$FOLLOW_LOGS" -eq 1 ]]; then docker compose logs -f --tail=200; fi
