<#
.SYNOPSIS
    Start the Sentinel stack, then the shared Airflow stack, then wait until healthy.

.DESCRIPTION
    Order matters: the shared Airflow joins Sentinel's `sentinel-net` network, so
    Sentinel must be up first. After starting both, runs scripts/check.ps1 and waits
    (up to -WaitMinutes) for every check to pass.

.EXAMPLE
    .\scripts\start.ps1
    .\scripts\start.ps1 -NoWait
    .\scripts\start.ps1 -AirflowDir "D:\code\pipelines"
#>
param(
    # Folder holding the shared Airflow docker-compose.yml. Defaults to the
    # dispatchledger\pipelines folder next to this repo.
    [string]$AirflowDir = (Join-Path $PSScriptRoot "..\..\dispatchledger\pipelines"),
    # How long to wait for everything to report healthy (Spark's start-up grace is 10 min).
    [int]$WaitMinutes = 12,
    # Start the containers and return without waiting for health.
    [switch]$NoWait,
    # Start Sentinel only, without the shared Airflow.
    [switch]$SkipAirflow
)

$ErrorActionPreference = "Stop"
$RepoDir = Resolve-Path (Join-Path $PSScriptRoot "..")

function Write-Step([string]$Text) { Write-Host "`n==> $Text" -ForegroundColor Cyan }

# ------------------------------------------------------------------ Docker running?
Write-Step "Checking Docker"
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    $desktop = Join-Path $env:ProgramFiles "Docker\Docker\Docker Desktop.exe"
    if (Test-Path $desktop) {
        Write-Host "Docker is not running. Starting Docker Desktop..."
        Start-Process $desktop
        $deadline = (Get-Date).AddMinutes(3)
        do {
            Start-Sleep -Seconds 5
            docker info *> $null
        } while ($LASTEXITCODE -ne 0 -and (Get-Date) -lt $deadline)
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Docker is not running. Start Docker Desktop and run this again." -ForegroundColor Red
        exit 1
    }
}
Write-Host "Docker is running."

# ------------------------------------------------------------------ Sentinel
Write-Step "Starting Sentinel ($RepoDir)"
Push-Location $RepoDir
try {
    docker compose up -d
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed for Sentinel" }
} finally { Pop-Location }

# ------------------------------------------------------------------ shared Airflow
if (-not $SkipAirflow) {
    if (Test-Path (Join-Path $AirflowDir "docker-compose.yml")) {
        $AirflowDir = Resolve-Path $AirflowDir
        Write-Step "Starting shared Airflow ($AirflowDir)"
        Push-Location $AirflowDir
        try {
            docker compose up -d
            if ($LASTEXITCODE -ne 0) { throw "docker compose up failed for Airflow" }
        } finally { Pop-Location }
    } else {
        Write-Host "No docker-compose.yml in $AirflowDir; skipping Airflow. Pass -AirflowDir to point at it." -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "Grafana : http://localhost:13000"
Write-Host "Airflow : http://localhost:8081  (admin / admin)"
Write-Host "Spark UI: http://localhost:14040"
Write-Host "Postgres: localhost:15432, database sentinel (DBeaver)"

if ($NoWait) {
    Write-Host "`nStarted. Run .\scripts\check.ps1 in about 10 minutes to confirm health."
    exit 0
}

# ------------------------------------------------------------------ wait for health
Write-Step "Waiting for health (up to $WaitMinutes min; Spark needs a few minutes)"
& (Join-Path $PSScriptRoot "check.ps1") -WaitMinutes $WaitMinutes -SkipAirflow:$SkipAirflow
exit $LASTEXITCODE
