#!/usr/bin/env bash
# =============================================================================
# EMQX PostgreSQL 插件部署机侧全量重部署脚本
#
# 功能：停止并删除容器、删除本地旧镜像、清空 EMQX 文件日志、重新拉取并部署。
# 不会删除 data/，数据库、EMQX 状态及持久化配置都会保留。
#
# 目录结构：
#   docker-compose.yml
#   scripts/redeploy.sh
#   data/  etc/  log/
#
# 用法：
#   ./scripts/redeploy.sh
#   ./scripts/redeploy.sh --logs      # 部署成功后继续跟随日志
#   DEPLOY_DIR=/path/to/emqx ./scripts/redeploy.sh
# =============================================================================
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-120}"
FOLLOW_LOGS=0

c_reset=$'\033[0m'; c_red=$'\033[31m'; c_green=$'\033[32m'; c_yellow=$'\033[33m'; c_blue=$'\033[36m'
if [[ -n "${NO_COLOR:-}" || ! -t 1 ]]; then c_reset=""; c_red=""; c_green=""; c_yellow=""; c_blue=""; fi

log()  { printf '%s[redeploy]%s %s\n' "$c_blue" "$c_reset" "$*"; }
ok()   { printf '%s  ✓%s %s\n' "$c_green" "$c_reset" "$*"; }
warn() { printf '%s  !%s %s\n' "$c_yellow" "$c_reset" "$*" >&2; }
die()  { printf '%s  ✗%s %s\n' "$c_red" "$c_reset" "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --logs|-f) FOLLOW_LOGS=1; shift ;;
    -h|--help) sed -n '2,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "未知参数: $1（用 -h 查看用法）" ;;
  esac
done

[[ -f "$DEPLOY_DIR/docker-compose.yml" ]] || die "未找到 $DEPLOY_DIR/docker-compose.yml"
command -v docker >/dev/null 2>&1 || die "未找到 docker"
docker info >/dev/null 2>&1 || die "Docker daemon 不可用"

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  die "未找到 docker compose（v2 插件或 docker-compose）"
fi

cd "$DEPLOY_DIR"
mapfile -t IMAGES < <("${COMPOSE[@]}" config --images | sed '/^[[:space:]]*$/d' | sort -u)
[[ ${#IMAGES[@]} -gt 0 ]] || die "未从 docker-compose.yml 解析到镜像"

log "停止并删除 Compose 容器"
"${COMPOSE[@]}" down --remove-orphans

# 仅清理部署目录下明确的 log/，不触碰 data/ 和 etc/。
LOG_DIR="$DEPLOY_DIR/log"
case "$LOG_DIR" in
  "$DEPLOY_DIR"/log) ;;
  *) die "日志目录越界: $LOG_DIR" ;;
esac
if [[ -d "$LOG_DIR" ]]; then
  log "清理文件日志: $LOG_DIR"
  # 用旧 EMQX 镜像内的 root 清理，避免容器写出的日志属主导致宿主机权限错误。
  if docker image inspect "${IMAGES[0]}" >/dev/null 2>&1; then
    docker run --rm --user 0:0 --entrypoint sh \
      -v "$LOG_DIR:/logs" "${IMAGES[0]}" -c 'find /logs -mindepth 1 -delete'
  else
    find "$LOG_DIR" -mindepth 1 -delete
  fi
else
  mkdir -p "$LOG_DIR"
fi

for image in "${IMAGES[@]}"; do
  if docker image inspect "$image" >/dev/null 2>&1; then
    log "删除旧镜像: $image"
    docker image rm -f "$image"
  else
    log "本地镜像不存在，跳过: $image"
  fi
done

log "重新拉取镜像"
"${COMPOSE[@]}" pull

log "重新创建并启动 EMQX"
"${COMPOSE[@]}" up -d --remove-orphans --force-recreate

container_id="$("${COMPOSE[@]}" ps -q emqx)"
[[ -n "$container_id" ]] || die "未找到 emqx 容器"

log "等待 EMQX 就绪（最多 ${HEALTH_TIMEOUT}s）"
ready=0
for ((waited = 0; waited < HEALTH_TIMEOUT; waited += 3)); do
  state="$(docker inspect --format '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
  case "$state" in
    "running healthy") ready=1; break ;;
    "running ")
      if docker exec "$container_id" emqx ctl status >/dev/null 2>&1; then ready=1; break; fi
      ;;
    exited*|dead*) die "EMQX 容器已退出，请执行 docker compose logs --tail=200 emqx" ;;
  esac
  sleep 3
done

if [[ "$ready" -ne 1 ]]; then
  "${COMPOSE[@]}" ps
  "${COMPOSE[@]}" logs --tail=200 emqx || true
  die "EMQX 在 ${HEALTH_TIMEOUT}s 内未就绪"
fi

log "插件状态："
plugin_status="$(docker exec "$container_id" emqx ctl plugins list 2>&1)" || {
  printf '%s\n' "$plugin_status" >&2
  die "无法读取插件状态"
}
printf '%s\n' "$plugin_status"
if [[ "$plugin_status" != *"emqx_pg_ingest-"* ]]; then
  log "容器内插件文件："
  docker exec "$container_id" sh -c 'find /opt/emqx/plugins -maxdepth 2 -type f -o -type d' || true
  die "emqx_pg_ingest 未被 EMQX 识别，本次部署失败"
fi
if ! grep -Eq '"running_status"[[:space:]]*:[[:space:]]*"running"' <<<"$plugin_status"; then
  "${COMPOSE[@]}" logs --tail=200 emqx || true
  die "emqx_pg_ingest 已识别但未成功运行，本次部署失败"
fi

ok "EMQX 和 emqx_pg_ingest 已完成全量重部署"
"${COMPOSE[@]}" ps

if [[ "$FOLLOW_LOGS" -eq 1 ]]; then
  log "跟随日志（Ctrl+C 退出，不会停止服务）"
  "${COMPOSE[@]}" logs -f --tail=200 emqx
fi
