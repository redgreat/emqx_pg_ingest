#!/usr/bin/env bash
# 推送版本标签并触发 GitHub Actions。无参数时自动递增 patch 版本。
#   ./scripts/release.sh
#   ./scripts/release.sh v0.2.0
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${1:-auto}"

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
command -v git >/dev/null 2>&1 || die "git not found"
cd "$REPO_ROOT"

[[ -z "$(git status --porcelain)" ]] || die "Working tree is not clean; commit all changes first"
git fetch --tags origin

UPSTREAM="$(git rev-parse --abbrev-ref '@{upstream}' 2>/dev/null)" ||
  die "Current branch has no upstream; push the branch first"
[[ "$(git rev-parse HEAD)" == "$(git rev-parse '@{upstream}')" ]] ||
  die "Current commit is not synchronized with $UPSTREAM; push the branch first"

if [[ -z "$VERSION" || "$VERSION" == "auto" || "$VERSION" == "+" ]]; then
  latest="$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-version:refname | head -n1)"
  if [[ -z "$latest" ]]; then
    VERSION="v0.0.1"
  else
    IFS=. read -r major minor patch <<< "${latest#v}"
    VERSION="v${major}.${minor}.$((patch + 1))"
  fi
fi

[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
  die "Tag must be vMAJOR.MINOR.PATCH, got: $VERSION"
git rev-parse -q --verify "refs/tags/$VERSION" >/dev/null &&
  die "Tag already exists: $VERSION"

git tag -a "$VERSION" -m "Release $VERSION"
if ! git push origin "$VERSION"; then
  git tag -d "$VERSION" >/dev/null
  die "Failed to push $VERSION; local tag was removed"
fi

printf 'Published %s. GitHub Actions will build the plugin package, GHCR image, and GitHub Release.\n' "$VERSION"
