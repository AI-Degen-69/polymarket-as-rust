<#
.SYNOPSIS
    Offline parameter sweep runner over recorded Polymarket feed captures.
.DESCRIPTION
    Builds the release `poly-as` binary once (unless -SkipBuild is passed),
    replays a recorded JSONL capture across a grid of Avellaneda-Stoikov math
    parameters using `poly-as lifecycle --source replay`, and prints a ranked
    comparison table sorted descending by NetPnL.
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -InputPath logs/feeds.jsonl
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -InputPath logs/feeds.jsonl -Gammas 0.02,0.08,0.15 -VarianceSpreads false
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$InputPath,

    [double[]]$Gammas = @(0.02, 0.05, 0.08, 0.12, 0.15),

    [string[]]$VarianceSpreads = @("false", "true"),

    [int[]]$MinSpreadTicks = @(1),

    [uint64]$MaxCombinedBidMicro = 990000,

    [ValidateSet("conservative", "optimistic")]
    [string]$QueueDecay = "optimistic",

    [uint64]$VMax = 50,

    [uint64]$BaseSize = 20,

    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $InputPath)) {
    throw "Input capture file not found: $InputPath"
}
$resolvedInput = (Resolve-Path -LiteralPath $InputPath).Path

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    $binPath = Join-Path $repoRoot "target\release\poly-as.exe"
    if (-not $SkipBuild) {
        Write-Host "[BUILD] Building release binary (cargo build --release -p cli --bin poly-as)..." -ForegroundColor Cyan
        & cargo build --release -p cli --bin poly-as
        if ($LASTEXITCODE -ne 0) {
            throw "Release build failed with exit code $LASTEXITCODE"
        }
    }
    if (-not (Test-Path -LiteralPath $binPath)) {
        throw "Release binary not found at $binPath (re-run without -SkipBuild)"
    }

    $sweepDir = Join-Path $repoRoot "logs\sweeps"
    New-Item -ItemType Directory -Force -Path $sweepDir | Out-Null

    $inv = [System.Globalization.CultureInfo]::InvariantCulture
    $results = [System.Collections.Generic.List[PSCustomObject]]::new()
    $totalRuns = $Gammas.Count * $VarianceSpreads.Count * $MinSpreadTicks.Count
    $runIdx = 0

    foreach ($gamma in $Gammas) {
        $gammaStr = $gamma.ToString("0.####", $inv)
        $gammaTag = $gammaStr.Replace(".", "p")

        foreach ($rawVar in $VarianceSpreads) {
            $varStr = $rawVar.Trim().ToLowerInvariant()
            if ($varStr -ne "true" -and $varStr -ne "false") {
                throw "Invalid VarianceSpreads entry '$rawVar' (expected 'true' or 'false')"
            }

            foreach ($minTicks in $MinSpreadTicks) {
                $runIdx++
                $stem = "sweep-g$gammaTag-var$varStr-t$minTicks-b$MaxCombinedBidMicro-$QueueDecay"
                $outJsonl = Join-Path $sweepDir "$stem.jsonl"
                $outLog = Join-Path $sweepDir "$stem.log"

                Write-Host ("[{0}/{1}] Replaying gamma={2} var_spread={3} min_ticks={4}..." -f $runIdx, $totalRuns, $gammaStr, $varStr, $minTicks)

                $cliArgs = @(
                    "lifecycle",
                    "--source", "replay",
                    "--input", $resolvedInput,
                    "--v-max", $VMax.ToString($inv),
                    "--base-size", $BaseSize.ToString($inv),
                    "--queue-decay", $QueueDecay,
                    "--gamma", $gammaStr,
                    "--include-variance-spread", $varStr,
                    "--min-spread-ticks", $minTicks.ToString($inv),
                    "--max-combined-bid-micro", $MaxCombinedBidMicro.ToString($inv),
                    "--jsonl", $outJsonl
                )

                & $binPath @cliArgs *> $outLog
                if ($LASTEXITCODE -ne 0) {
                    $tail = if (Test-Path -LiteralPath $outLog) { (Get-Content -LiteralPath $outLog -Tail 20) -join "`n" } else { "" }
                    throw "Sweep run failed (exit=$LASTEXITCODE) for $stem.`n$tail"
                }

                $summaryLine = Get-Content -LiteralPath $outJsonl |
                    Where-Object { $_ -match '"event"\s*:\s*"summary"' } |
                    Select-Object -Last 1
                if (-not $summaryLine) {
                    throw "No summary event emitted in $outJsonl"
                }

                $payload = ($summaryLine | ConvertFrom-Json).payload
                $realized = [double]$payload.realized_pnl_micro / 1e6
                $unreal = [double]$payload.unrealized_pnl_micro / 1e6
                $netPnl = [double]$payload.net_pnl_micro / 1e6
                $rebate = [double]$payload.estimated_rebate_micro / 1e6
                $basePnl = [double]$payload.baseline_pnl_micro / 1e6
                $alphaEdge = $netPnl - $basePnl
                $pairs = [double]$payload.total_pairs_merged_micro / 1e6

                $results.Add([PSCustomObject]@{
                    Gamma      = $gammaStr
                    VarSpr     = $varStr
                    MinT       = $minTicks
                    Fills      = [int]$payload.fills_count
                    Merges     = [int]$payload.merges_count
                    Pairs      = $pairs.ToString("F2", $inv)
                    Realized   = $realized.ToString("+0.0000;-0.0000;+0.0000", $inv)
                    Unreal     = $unreal.ToString("+0.0000;-0.0000;+0.0000", $inv)
                    NetPnL     = $netPnl.ToString("+0.0000;-0.0000;+0.0000", $inv)
                    Rebate     = $rebate.ToString("0.0000", $inv)
                    BasePnL    = $basePnl.ToString("+0.0000;-0.0000;+0.0000", $inv)
                    AlphaEdge  = $alphaEdge.ToString("+0.0000;-0.0000;+0.0000", $inv)
                    RPM        = [int]$payload.rpm_60s
                    MO1s       = ([double]$payload.markout_1s_bps).ToString("+0.0;-0.0;+0.0", $inv)
                    MO5s       = ([double]$payload.markout_5s_bps).ToString("+0.0;-0.0;+0.0", $inv)
                    NetPnLNum  = $netPnl
                })
            }
        }
    }

    Write-Host ""
    Write-Host "=== OFFLINE PARAMETER SWEEP RESULTS (sorted by NetPnL desc) ===" -ForegroundColor Green
    Write-Host ("Input: {0} | V_max=${1} | Base={2}sh | MaxPair=${3} | Decay={4}" -f $InputPath, $VMax, $BaseSize, ($MaxCombinedBidMicro / 1e6).ToString("0.00", $inv), $QueueDecay)
    $sorted = $results | Sort-Object -Property NetPnLNum -Descending
    $sorted |
        Select-Object Gamma, VarSpr, MinT, Fills, Merges, Pairs, Realized, Unreal, NetPnL, Rebate, BasePnL, AlphaEdge, RPM, MO1s, MO5s |
        Format-Table -AutoSize
}
finally {
    Pop-Location
}
