#!/usr/bin/env pwsh
# EMQX PG Ingest 发布/部署脚本（PowerShell）。
#   .\scripts\release.ps1 -Tag v0.1.0
#   .\scripts\release.ps1
param(
    [string]$Tag = '',
    [string]$DeployDir = $env:DEPLOY_DIR,
    [string]$Image = 'ghcr.io/redgreat/emqx_pg_ingest:latest',
    [int]$HealthTimeout = 120,
    [switch]$NoPull,
    [switch]$SyncConfig,
    [switch]$NoHealth,
    [switch]$Logs
)
$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($DeployDir)) { $DeployDir = 'D:\docker\emqx_pg_ingest' }
$RepoRoot = Split-Path -Parent $PSScriptRoot

function Write-Info([string]$Message) { Write-Host "[release] $Message" -ForegroundColor Cyan }
function Write-Ok([string]$Message) { Write-Host "  OK  $Message" -ForegroundColor Green }
function Stop-Fail([string]$Message) { throw $Message }

if (-not (Get-Command git -ErrorAction SilentlyContinue)) { Stop-Fail 'git not found' }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Stop-Fail 'docker not found' }
docker compose version *> $null
if ($LASTEXITCODE -ne 0) { Stop-Fail 'docker compose v2 not found' }

if ($Tag) {
    Set-Location $RepoRoot
    if (git status --porcelain) { Stop-Fail 'Working tree is not clean; commit changes first' }
    git fetch --tags origin
    $version = $Tag
    if ($version -eq 'auto' -or $version -eq '+') {
        $latest = git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-version:refname | Select-Object -First 1
        if (-not $latest) { $version = 'v0.1.0' }
        elseif ($latest -match '^v(\d+)\.(\d+)\.(\d+)$') {
            $version = "v$($Matches[1]).$($Matches[2]).$([int]$Matches[3] + 1)"
        }
    }
    if ($version -notmatch '^v\d+\.\d+\.\d+$') { Stop-Fail 'Tag must be vMAJOR.MINOR.PATCH' }
    if (git tag --list $version) { Stop-Fail "Tag already exists: $version" }
    git tag -a $version -m "Release $version"
    git push origin $version
    Write-Ok "Pushed $version; GitHub Actions is building the plugin package and GHCR image"
    return
}

New-Item -ItemType Directory -Force -Path (
    (Join-Path $DeployDir 'etc'),
    (Join-Path $DeployDir 'data'),
    (Join-Path $DeployDir 'log')
) | Out-Null
Copy-Item (Join-Path $RepoRoot 'docker-compose.yml') (Join-Path $DeployDir 'docker-compose.yml') -Force
$targetConfig = Join-Path $DeployDir 'etc/emqx_pg_ingest.json'
if ((-not (Test-Path $targetConfig)) -or $SyncConfig) {
    Copy-Item (Join-Path $RepoRoot 'priv/emqx_pg_ingest.json') $targetConfig -Force
    Write-Warning "Review PG credentials in $targetConfig before production use"
}

Set-Location $DeployDir
$env:EMQX_IMAGE = $Image
if (-not $NoPull) {
    Write-Info "Pulling $Image"
    docker pull $Image
    if ($LASTEXITCODE -ne 0) { Stop-Fail 'Image pull failed' }
}
$pullMode = if ($NoPull) { 'never' } else { 'always' }
docker compose up -d --remove-orphans "--pull=$pullMode"
if ($LASTEXITCODE -ne 0) { Stop-Fail 'docker compose up failed' }
Write-Ok 'EMQX container started'

if (-not $NoHealth) {
    $waited = 0
    while ($waited -lt $HealthTimeout) {
        docker exec emqx /opt/emqx/bin/emqx ctl status *> $null
        if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Seconds 3
        $waited += 3
    }
    if ($waited -ge $HealthTimeout) { Stop-Fail "EMQX did not become healthy in ${HealthTimeout}s" }

    $plugins = docker exec emqx /opt/emqx/bin/emqx ctl plugins list 2>&1 | Out-String
    Write-Host $plugins
    if ($plugins -notmatch 'emqx_pg_ingest') { Stop-Fail 'emqx_pg_ingest is not installed' }
    $pluginLog = docker logs emqx 2>&1 | Select-String '\[emqx_pg_ingest\] plugin started'
    if (-not $pluginLog) { Stop-Fail 'Plugin startup confirmation was not found in logs' }
    Write-Ok 'emqx_pg_ingest is running'
}

docker compose ps
if ($Logs) { docker compose logs -f --tail=200 }
