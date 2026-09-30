#!/usr/bin/env pwsh
# 推送版本标签并触发 GitHub Actions。无参数时自动递增 patch 版本。
#   .\scripts\release.ps1
#   .\scripts\release.ps1 -Tag v0.2.0
param(
    [string]$Tag = 'auto'
)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot

function Stop-Fail([string]$Message) { throw $Message }

if (-not (Get-Command git -ErrorAction SilentlyContinue)) { Stop-Fail 'git not found' }
Set-Location $RepoRoot

if (git status --porcelain) { Stop-Fail 'Working tree is not clean; commit all changes first' }
git fetch --tags origin
if ($LASTEXITCODE -ne 0) { Stop-Fail 'Failed to fetch origin tags' }

$upstream = git rev-parse --abbrev-ref '@{upstream}' 2>$null
if ($LASTEXITCODE -ne 0 -or -not $upstream) {
    Stop-Fail 'Current branch has no upstream; push the branch first'
}
$head = git rev-parse HEAD
$upstreamHead = git rev-parse '@{upstream}'
if ($head -ne $upstreamHead) {
    Stop-Fail "Current commit is not synchronized with $upstream; push the branch first"
}

$version = $Tag
if ([string]::IsNullOrWhiteSpace($version) -or $version -eq 'auto' -or $version -eq '+') {
    $latest = git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-version:refname | Select-Object -First 1
    if (-not $latest) {
        $version = 'v0.0.1'
    } elseif ($latest -match '^v(\d+)\.(\d+)\.(\d+)$') {
        $version = "v$($Matches[1]).$($Matches[2]).$([int]$Matches[3] + 1)"
    }
}
if ($version -notmatch '^v\d+\.\d+\.\d+$') {
    Stop-Fail "Tag must be vMAJOR.MINOR.PATCH, got: $version"
}
if (git tag --list $version) { Stop-Fail "Tag already exists: $version" }

git tag -a $version -m "Release $version"
if ($LASTEXITCODE -ne 0) { Stop-Fail "Failed to create tag $version" }
git push origin $version
if ($LASTEXITCODE -ne 0) {
    git tag -d $version *> $null
    Stop-Fail "Failed to push $version; local tag was removed"
}

Write-Host "Published $version. GitHub Actions will build the plugin package, GHCR image, and GitHub Release." -ForegroundColor Green
