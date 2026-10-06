<#
.SYNOPSIS
    Health check for the running Sentinel stack. Prints PASS / WAIT / FAIL per check.

.DESCRIPTION
    Checks, in order:
      1. Core containers are running (and healthy, where they have a healthcheck).
      2. The streaming job's own healthcheck: every stream heartbeating.
      3. Postgres: no stream silent for 10+ minutes; age of the newest alert.
      4. Airflow: state of the latest sentinel_batch run (warning only).
    Exit code 0 when everything required passes, 1 otherwise.

.EXAMPLE
    .\scripts\check.ps1                  # check once
    .\scripts\check.ps1 -WaitMinutes 12  # keep re-checking until healthy or time runs out
#>
param(
    [int]$WaitMinutes = 0,
    [string]$AirflowUrl = "http://localhost:8081",
    [string]$AirflowUser = "admin",
    [string]$AirflowPassword = "admin",
    [switch]$SkipAirflow
)

$ErrorActionPreference = "Continue"
$RepoDir = Resolve-Path (Join-Path $PSScriptRoot "..")
$Project = "sentinel-lakehouse"

function Write-Result([string]$Status, [string]$Name, [string]$Detail) {
    $color = @{ PASS = "Green"; WAIT = "Yellow"; WARN = "Yellow"; FAIL = "Red"; INFO = "Gray" }[$Status]
    Write-Host ("  [{0}] {1,-28} {2}" -f $Status, $Name, $Detail) -ForegroundColor $color
}

function Get-ContainerState([string]$Service) {
    $name = "$Project-$Service-1"
    $out = docker inspect --format "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}|{{.RestartCount}}" $name 2> $null
    if ($LASTEXITCODE -ne 0 -or -not $out) { return $null }
    $parts = "$out".Trim().Split("|")
    return [pscustomobject]@{ Status = $parts[0]; Health = $parts[1]; Restarts = [int]$parts[2] }
}

function Invoke-Psql([string]$Sql) {
    Push-Location $RepoDir
    try {
        $out = docker compose exec -T postgres psql -U sentinel -d sentinel -tAc $Sql 2> $null
        if ($LASTEXITCODE -ne 0) { return $null }
        return "$out".Trim()
    } finally { Pop-Location }
}

# One pass over every check. Returns "pass", "wait" (still starting) or "fail".
function Invoke-Checks {
    $failed = $false
    $waiting = $false

    Write-Host "`nContainers"
    foreach ($svc in "kafka", "s3", "postgres", "generator", "spark-streaming", "grafana") {
        $st = Get-ContainerState $svc
        if ($null -eq $st) {
            Write-Result "FAIL" $svc "not found (run .\scripts\start.ps1)"; $failed = $true; continue
        }
        $detail = $st.Status
        if ($st.Health) { $detail += " / $($st.Health)" }
        if ($st.Restarts -gt 0) { $detail += " / restarts=$($st.Restarts)" }
        if ($st.Status -ne "running" -or $st.Health -eq "unhealthy") {
            Write-Result "FAIL" $svc $detail; $failed = $true
        } elseif ($st.Health -eq "starting") {
            Write-Result "WAIT" $svc $detail; $waiting = $true
        } else {
            Write-Result "PASS" $svc $detail
        }
    }

    Write-Host "`nStreaming"
    Push-Location $RepoDir
    try {
        $hc = docker compose exec -T spark-streaming python3 /opt/sentinel/scripts/healthcheck.py 2>&1
        $hcCode = $LASTEXITCODE
    } finally { Pop-Location }
    $hcText = ("" + (@($hc) | Select-Object -Last 1)).Trim()
    if ($hcCode -eq 0) {
        Write-Result "PASS" "stream heartbeats" $hcText
    } elseif ($hcText -match "not started|no streaming queries|no health file") {
        Write-Result "WAIT" "stream heartbeats" $hcText; $waiting = $true
    } else {
        Write-Result "FAIL" "stream heartbeats" $hcText; $failed = $true
    }

    Write-Host "`nData"
    $stalled = Invoke-Psql "select count(*) filter (where last_seen_at < now() - interval '10 minutes' or status in ('stalled','terminated')) || '/' || count(*) from pipeline_heartbeat"
    if ($null -eq $stalled -or $stalled -eq "") {
        Write-Result "WAIT" "heartbeat table" "no heartbeats in Postgres yet"; $waiting = $true
    } elseif ($stalled -match "^0/") {
        Write-Result "PASS" "heartbeat table" "0 of $($stalled.Split('/')[1]) streams stalled"
    } else {
        Write-Result "FAIL" "heartbeat table" "$stalled streams stalled (see Grafana)"; $failed = $true
    }
    $alertAge = Invoke-Psql "select coalesce(round(extract(epoch from now() - max(detected_at)) / 60)::text, 'none') from alerts"
    if ($alertAge -eq "none" -or $null -eq $alertAge) {
        Write-Result "INFO" "newest alert" "no alerts yet (normal for the first few minutes)"
    } else {
        Write-Result "INFO" "newest alert" "$alertAge min ago"
    }

    if (-not $SkipAirflow) {
        Write-Host "`nBatch (Airflow)"
        Write-AirflowStatus
    }

    if ($failed) { return "fail" }
    if ($waiting) { return "wait" }
    return "pass"
}

# Latest sentinel_batch run via the Airflow 3 REST API. Warning only: Airflow is optional.
function Write-AirflowStatus {
    try {
        $body = @{ username = $AirflowUser; password = $AirflowPassword } | ConvertTo-Json
        $token = (Invoke-RestMethod -Method Post -Uri "$AirflowUrl/auth/token" -Body $body `
                -ContentType "application/json" -TimeoutSec 10).access_token
        $headers = @{ Authorization = "Bearer $token" }
        $base = "$AirflowUrl/api/v2/dags/sentinel_batch/dagRuns"
        try {
            $runs = (Invoke-RestMethod -Uri "$base`?limit=1&order_by=-start_date" -Headers $headers -TimeoutSec 10).dag_runs
        } catch {
            $runs = (Invoke-RestMethod -Uri "$base`?limit=100" -Headers $headers -TimeoutSec 10).dag_runs |
                Sort-Object start_date -Descending | Select-Object -First 1
        }
        $last = @($runs)[0]
        if ($null -eq $last) {
            Write-Result "WARN" "sentinel_batch" "no runs yet"
        } elseif ($last.state -eq "success") {
            Write-Result "PASS" "sentinel_batch" "last run succeeded ($($last.start_date))"
        } elseif ($last.state -in "running", "queued") {
            Write-Result "INFO" "sentinel_batch" "run in progress ($($last.start_date))"
        } else {
            Write-Result "WARN" "sentinel_batch" "last run $($last.state) ($($last.start_date)); check the Airflow UI"
        }
    } catch {
        Write-Result "WARN" "sentinel_batch" "Airflow not reachable at $AirflowUrl"
    }
}

# ------------------------------------------------------------------ main
$deadline = (Get-Date).AddMinutes($WaitMinutes)
while ($true) {
    Write-Host ("`nSentinel health check  {0:HH:mm:ss}" -f (Get-Date)) -ForegroundColor Cyan
    $result = Invoke-Checks
    if ($result -eq "pass") {
        Write-Host "`nAll checks passed. Dashboard: http://localhost:13000" -ForegroundColor Green
        exit 0
    }
    if ((Get-Date) -ge $deadline) { break }
    Write-Host "`nNot ready yet; checking again in 30 s (until $($deadline.ToString('HH:mm')))..." -ForegroundColor Yellow
    Start-Sleep -Seconds 30
}

if ($result -eq "wait") {
    Write-Host "`nStill starting up. Run .\scripts\check.ps1 again in a few minutes." -ForegroundColor Yellow
} else {
    Write-Host "`nSome checks failed. Logs: docker compose logs --tail 100 spark-streaming" -ForegroundColor Red
}
exit 1
