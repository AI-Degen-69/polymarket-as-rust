<#
.SYNOPSIS
    Offline parameter sweep runner over recorded Polymarket feed captures.
.DESCRIPTION
    Builds the release `poly-as` binary once (unless -SkipBuild is passed),
    replays a recorded JSONL capture across a grid of Avellaneda-Stoikov math
    parameters using `poly-as lifecycle --source replay`, and prints a ranked
    comparison table sorted descending by NetPnL.
    Primary host is Windows 11 PowerShell (pwsh); the script also resolves the
    correct executable name on Linux/macOS and respects CARGO_TARGET_DIR.
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -InputPath logs/feeds.jsonl
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -InputPath logs/feeds.jsonl -Gammas 0.02,0.08,0.15 -VarianceSpreads false
.NOTES
    Each invocation writes its outputs to a unique timestamped folder under
    logs/sweeps/ so concurrent runs never share output files and can never
    overwrite -InputPath.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$InputPath,

    [string[]]$Gammas = @("0.02", "0.05", "0.08", "0.12", "0.15"),

    [string[]]$VarianceSpreads = @("false", "true"),

    [string[]]$MinSpreadTicks = @("1"),

    [uint64]$MaxCombinedBidMicro = 990000,

    [ValidateSet("conservative", "optimistic")]
    [string]$QueueDecay = "optimistic",

    [uint64]$VMax = 50,

    [uint64]$BaseSize = 20,

    [switch]$SkipBuild,

    [string]$BinPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$inv = [System.Globalization.CultureInfo]::InvariantCulture
$parsedGammas = @($Gammas | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    [double]::Parse($_, $inv)
})
$parsedVarSpreads = @($VarianceSpreads | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim().ToLowerInvariant() } | Where-Object { $_ -ne '' })
$parsedMinTicks = @($MinSpreadTicks | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    [int]::Parse($_, $inv)
})

if ($parsedGammas.Count -eq 0 -or $parsedVarSpreads.Count -eq 0 -or $parsedMinTicks.Count -eq 0) {
    throw "Gammas, VarianceSpreads, and MinSpreadTicks must each contain at least one value"
}
foreach ($v in $parsedVarSpreads) {
    if ($v -ne "true" -and $v -ne "false") {
        throw "Invalid VarianceSpreads entry '$v' (expected 'true' or 'false')"
    }
}

if (-not (Test-Path -LiteralPath $InputPath)) {
    throw "Input capture file not found: $InputPath"
}
$resolvedInput = (Resolve-Path -LiteralPath $InputPath).Path

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    if ($BinPath -ne "") {
        $binPath = $BinPath
        if (-not (Test-Path -LiteralPath $binPath)) {
            throw "Explicit -BinPath not found: $binPath"
        }
    } else {
        $targetDir = $env:CARGO_TARGET_DIR
        if ([string]::IsNullOrWhiteSpace($targetDir)) {
            $targetDir = Join-Path $repoRoot "target"
        }
        if ($IsWindows) { $exeName = "poly-as.exe" } else { $exeName = "poly-as" }
        $binPath = Join-Path (Join-Path $targetDir "release") $exeName
        if (-not $SkipBuild) {
            Write-Host "[BUILD] Building release binary (cargo build --release -p cli --bin poly-as)..." -ForegroundColor Cyan
            & cargo build --release -p cli --bin poly-as
            if ($LASTEXITCODE -ne 0) {
                throw "Release build failed with exit code $LASTEXITCODE"
            }
            $cargoExe = (& cargo build --release -p cli --bin poly-as --message-format=json-render-diagnostics 2>$null | ConvertFrom-Json -ErrorAction SilentlyContinue | Where-Object { $_.reason -eq "compiler-artifact" -and $_.target.name -eq "poly-as" } | Select-Object -Last 1)
            if ($null -ne $cargoExe -and $null -ne $cargoExe.executable) {
                $binPath = $cargoExe.executable
            }
        }
        if (-not (Test-Path -LiteralPath $binPath)) {
            throw "Release binary not found at $binPath (re-run without -SkipBuild; CARGO_TARGET_DIR=$targetDir)"
        }
    }

    $stamp = Get-Date -Format "yyyyMMdd-HHmmss-fff"
    $runId = [guid]::NewGuid().ToString("N").Substring(0, 8)
    $sweepBase = Join-Path $repoRoot "logs/sweeps"
    $sweepDir = Join-Path $sweepBase ("sweep-{0}-{1}" -f $stamp, $runId)
    New-Item -ItemType Directory -Force -Path $sweepDir | Out-Null
    Write-Host "[SWEEP] Output folder: $sweepDir" -ForegroundColor Cyan
    if ($resolvedInput.StartsWith($sweepDir, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Input capture must not live inside this sweep output folder: $resolvedInput"
    }

    $results = [System.Collections.Generic.List[PSCustomObject]]::new()
    $totalRuns = $parsedGammas.Count * $parsedVarSpreads.Count * $parsedMinTicks.Count
    $runIdx = 0

    foreach ($gamma in $parsedGammas) {
        $gammaStr = $gamma.ToString("R", $inv)
        $gammaTag = $gammaStr.Replace(".", "p").Replace("-", "m")

        foreach ($varStr in $parsedVarSpreads) {
            foreach ($minTicks in $parsedMinTicks) {
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

                $summaryLine = Get-Content -LiteralPath $outJsonl -Tail 1
                if (-not $summaryLine) {
                    throw "No summary event emitted in $outJsonl"
                }
                $summaryObj = $summaryLine | ConvertFrom-Json
                if ($summaryObj.event -ne "summary") {
                    throw "Final JSONL row is not a summary event in $outJsonl (found '$($summaryObj.event)')"
                }

                $payload = $summaryObj.payload
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
    Write-Host ('Input: {0} | V_max=${1} | Base={2}sh | MaxPair=${3} | Decay={4}' -f $InputPath, $VMax, $BaseSize, ($MaxCombinedBidMicro / 1e6).ToString("0.00", $inv), $QueueDecay)
    Write-Host ("Outputs: {0}" -f $sweepDir)
    $sorted = $results | Sort-Object -Property NetPnLNum -Descending
    $fmt = "{0,-6} {1,-6} {2,4} {3,5} {4,6} {5,7} {6,9} {7,9} {8,9} {9,7} {10,9} {11,9} {12,4} {13,6} {14,6}"
    Write-Host ($fmt -f "Gamma", "VarSpr", "MinT", "Fills", "Merges", "Pairs", "Realized", "Unreal", "NetPnL", "Rebate", "BasePnL", "AlphaEdge", "RPM", "MO1s", "MO5s")
    Write-Host ($fmt -f "-----", "------", "----", "-----", "------", "-------", "--------", "--------", "--------", "------", "--------", "---------", "---", "-----", "-----")
    foreach ($r in $sorted) {
        Write-Host ($fmt -f $r.Gamma, $r.VarSpr, $r.MinT, $r.Fills, $r.Merges, $r.Pairs, $r.Realized, $r.Unreal, $r.NetPnL, $r.Rebate, $r.BasePnL, $r.AlphaEdge, $r.RPM, $r.MO1s, $r.MO5s)
    }
}
finally {
    Pop-Location
}
