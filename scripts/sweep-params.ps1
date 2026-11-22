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
    pwsh -File scripts/sweep-params.ps1
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -InputPath logs/captures/22-11_05-56-27Z_20s_1w.jsonl -Gammas 0.02,0.08,0.15 -VarianceSpreads false
.NOTES
    Each invocation writes its outputs to a unique UTC-timestamped folder under
    logs/sweeps/<DD-MM_HH-MM-SSZ>_<N>runs_<capture-stem>/ so concurrent runs
    never share output files and can never overwrite -InputPath.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$InputPath = "",

    [switch]$Latest,

    [string[]]$Gammas = @("0.02", "0.05", "0.08", "0.12", "0.15"),

    [string[]]$VarianceSpreads = @("false", "true"),

    [string[]]$MinSpreadTicks = @("1"),

    [string[]]$MaxCrossTickLossMicros = @("off"),

    [string[]]$MaxEntryProbs = @("off"),

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
$parsedCrossLosses = @($MaxCrossTickLossMicros | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    if ($_.Equals("off", [System.StringComparison]::OrdinalIgnoreCase)) {
        "off"
    } else {
        ([uint64]::Parse($_, $inv)).ToString($inv)
    }
})
$parsedMaxEntryProbs = @($MaxEntryProbs | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    if ($_.Equals("off", [System.StringComparison]::OrdinalIgnoreCase)) {
        "off"
    } else {
        $p = [double]::Parse($_, $inv)
        if ([double]::IsNaN($p) -or [double]::IsInfinity($p) -or $p -le 0.5 -or $p -ge 1.0) {
            throw "Invalid MaxEntryProbs entry '$_' (expected 'off' or finite number in (0.5, 1.0))"
        }
        $p.ToString("R", $inv)
    }
})

if ($parsedGammas.Count -eq 0 -or $parsedVarSpreads.Count -eq 0 -or $parsedMinTicks.Count -eq 0 -or $parsedCrossLosses.Count -eq 0 -or $parsedMaxEntryProbs.Count -eq 0) {
    throw "Gammas, VarianceSpreads, MinSpreadTicks, MaxCrossTickLossMicros, and MaxEntryProbs must each contain at least one value"
}
$showCrossLoss = @($parsedCrossLosses | Where-Object { $_ -ne "off" }).Count -gt 0
$showEntryProb = @($parsedMaxEntryProbs | Where-Object { $_ -ne "off" }).Count -gt 0
foreach ($v in $parsedVarSpreads) {
    if ($v -ne "true" -and $v -ne "false") {
        throw "Invalid VarianceSpreads entry '$v' (expected 'true' or 'false')"
    }
}

if ($Latest -and -not [string]::IsNullOrWhiteSpace($InputPath)) {
    throw "Cannot pass both -InputPath and -Latest"
}

$resolvedInput = ""
if (-not [string]::IsNullOrWhiteSpace($InputPath)) {
    if (-not (Test-Path -LiteralPath $InputPath)) {
        throw "Input capture file not found: $InputPath"
    }
    $resolvedInput = (Resolve-Path -LiteralPath $InputPath).Path
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    if ([string]::IsNullOrWhiteSpace($resolvedInput)) {
        $capturesDir = Join-Path $repoRoot "logs/captures"
        if (-not (Test-Path -LiteralPath $capturesDir)) {
            throw "Captures directory not found: $capturesDir (pass -InputPath or run poly-as feeds --discover)"
        }
        $latestFile = Get-ChildItem -LiteralPath $capturesDir -Filter "*.jsonl" -File |
            Where-Object { -not $_.Name.EndsWith(".inprogress.jsonl", [System.StringComparison]::OrdinalIgnoreCase) } |
            Sort-Object -Property LastWriteTimeUtc -Descending |
            Select-Object -First 1
        if ($null -eq $latestFile) {
            throw "No finalized .jsonl captures found in $capturesDir (pass -InputPath or run poly-as feeds --discover)"
        }
        $resolvedInput = $latestFile.FullName
        Write-Host "[SWEEP] Auto-selected latest capture: $resolvedInput" -ForegroundColor Cyan
    }
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

    $totalRuns = $parsedGammas.Count * $parsedVarSpreads.Count * $parsedMinTicks.Count * $parsedCrossLosses.Count * $parsedMaxEntryProbs.Count
    $stamp = [DateTime]::UtcNow.ToString("dd-MM_HH-mm-ssZ", $inv)
    $captureStem = [System.IO.Path]::GetFileNameWithoutExtension($resolvedInput)
    $sweepBase = Join-Path $repoRoot "logs/sweeps"
    if (-not (Test-Path -LiteralPath $sweepBase)) {
        New-Item -ItemType Directory -Force -Path $sweepBase | Out-Null
    }
    $baseDirName = "{0}_{1}runs_{2}" -f $stamp, $totalRuns, $captureStem
    $sweepDir = $null
    for ($suffix = 1; $suffix -le 10000; $suffix++) {
        $dirName = if ($suffix -eq 1) { $baseDirName } else { "{0}-{1}" -f $baseDirName, $suffix }
        $candidate = Join-Path $sweepBase $dirName
        try {
            New-Item -ItemType Directory -Path $candidate -ErrorAction Stop | Out-Null
            $sweepDir = $candidate
            break
        } catch [System.IO.IOException] {
            continue
        }
    }
    if ($null -eq $sweepDir) {
        throw "Failed to allocate unique sweep directory under $sweepBase for $baseDirName"
    }
    Write-Host "[SWEEP] Output folder: $sweepDir" -ForegroundColor Cyan
    if ($resolvedInput.StartsWith($sweepDir, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Input capture must not live inside this sweep output folder: $resolvedInput"
    }

    $results = [System.Collections.Generic.List[PSCustomObject]]::new()
    $runIdx = 0

    foreach ($gamma in $parsedGammas) {
        $gammaStr = $gamma.ToString("R", $inv)
        $gammaTag = $gammaStr.Replace(".", "p").Replace("-", "m")

        foreach ($varStr in $parsedVarSpreads) {
            foreach ($minTicks in $parsedMinTicks) {
                foreach ($crossLoss in $parsedCrossLosses) {
                    foreach ($entryProb in $parsedMaxEntryProbs) {
                        $runIdx++
                        $stem = "g${gammaTag}_var-${varStr}_t${minTicks}_b${MaxCombinedBidMicro}_${QueueDecay}"
                        if ($showCrossLoss) {
                            $stem += "_xt-${crossLoss}"
                        }
                        if ($showEntryProb) {
                            $epTag = $entryProb.Replace(".", "p")
                            $stem += "_ep-${epTag}"
                        }
                        $outJsonl = Join-Path $sweepDir "$stem.jsonl"
                        $outLog = Join-Path $sweepDir "$stem.log"

                        $extraProgress = ""
                        if ($showCrossLoss) { $extraProgress += " xtick=$crossLoss" }
                        if ($showEntryProb) { $extraProgress += " max_p=$entryProb" }
                        Write-Host ("[{0}/{1}] Replaying gamma={2} var_spread={3} min_ticks={4}{5}..." -f $runIdx, $totalRuns, $gammaStr, $varStr, $minTicks, $extraProgress)

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
                        if ($crossLoss -ne "off") {
                            $cliArgs += @("--max-cross-tick-loss-micro", $crossLoss)
                        }
                        if ($entryProb -ne "off") {
                            $cliArgs += @("--max-entry-prob", $entryProb)
                        }

                        $prevCrossLossEnv = $env:MAX_CROSS_TICK_LOSS_MICRO
                        $prevEntryProbEnv = $env:MAX_ENTRY_PROB
                        try {
                            if ($crossLoss -eq "off") {
                                $env:MAX_CROSS_TICK_LOSS_MICRO = "off"
                            }
                            if ($entryProb -eq "off") {
                                $env:MAX_ENTRY_PROB = "off"
                            }
                            & $binPath @cliArgs *> $outLog
                            if ($LASTEXITCODE -ne 0) {
                                $tail = if (Test-Path -LiteralPath $outLog) { (Get-Content -LiteralPath $outLog -Tail 20) -join "`n" } else { "" }
                                throw "Sweep run failed (exit=$LASTEXITCODE) for $stem.`n$tail"
                            }
                        } finally {
                            if ($null -eq $prevCrossLossEnv) {
                                Remove-Item Env:MAX_CROSS_TICK_LOSS_MICRO -ErrorAction SilentlyContinue
                            } else {
                                $env:MAX_CROSS_TICK_LOSS_MICRO = $prevCrossLossEnv
                            }
                            if ($null -eq $prevEntryProbEnv) {
                                Remove-Item Env:MAX_ENTRY_PROB -ErrorAction SilentlyContinue
                            } else {
                                $env:MAX_ENTRY_PROB = $prevEntryProbEnv
                            }
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
                            XTick      = $crossLoss
                            MaxP       = $entryProb
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
        }
    }

    $sorted = $results | Sort-Object -Property NetPnLNum -Descending
    $cols = [System.Collections.Generic.List[hashtable]]::new()
    $cols.Add(@{ Name = "Gamma"; Dash = "-----"; Width = -6; Prop = "Gamma" })
    $cols.Add(@{ Name = "VarSpr"; Dash = "------"; Width = -6; Prop = "VarSpr" })
    $cols.Add(@{ Name = "MinT"; Dash = "----"; Width = 4; Prop = "MinT" })
    if ($showCrossLoss) {
        $cols.Add(@{ Name = "XTick"; Dash = "-----"; Width = 7; Prop = "XTick" })
    }
    if ($showEntryProb) {
        $cols.Add(@{ Name = "MaxP"; Dash = "----"; Width = 5; Prop = "MaxP" })
    }
    $cols.Add(@{ Name = "Fills"; Dash = "-----"; Width = 5; Prop = "Fills" })
    $cols.Add(@{ Name = "Merges"; Dash = "------"; Width = 6; Prop = "Merges" })
    $cols.Add(@{ Name = "Pairs"; Dash = "-------"; Width = 7; Prop = "Pairs" })
    $cols.Add(@{ Name = "Realized"; Dash = "--------"; Width = 9; Prop = "Realized" })
    $cols.Add(@{ Name = "Unreal"; Dash = "--------"; Width = 9; Prop = "Unreal" })
    $cols.Add(@{ Name = "NetPnL"; Dash = "--------"; Width = 9; Prop = "NetPnL" })
    $cols.Add(@{ Name = "Rebate"; Dash = "------"; Width = 7; Prop = "Rebate" })
    $cols.Add(@{ Name = "BasePnL"; Dash = "--------"; Width = 9; Prop = "BasePnL" })
    $cols.Add(@{ Name = "AlphaEdge"; Dash = "---------"; Width = 9; Prop = "AlphaEdge" })
    $cols.Add(@{ Name = "RPM"; Dash = "---"; Width = 4; Prop = "RPM" })
    $cols.Add(@{ Name = "MO1s"; Dash = "-----"; Width = 6; Prop = "MO1s" })
    $cols.Add(@{ Name = "MO5s"; Dash = "-----"; Width = 6; Prop = "MO5s" })

    $fmtParts = for ($ci = 0; $ci -lt $cols.Count; $ci++) { "{$ci,$($cols[$ci].Width)}" }
    $fmt = $fmtParts -join " "
    $headers = @($cols | ForEach-Object { $_.Name })
    $dashes = @($cols | ForEach-Object { $_.Dash })

    $summaryLines = [System.Collections.Generic.List[string]]::new()
    $summaryLines.Add("=== OFFLINE PARAMETER SWEEP RESULTS (sorted by NetPnL desc) ===")
    $summaryLines.Add(('Input: {0} | V_max=${1} | Base={2}sh | MaxPair=${3} | Decay={4}' -f $resolvedInput, $VMax, $BaseSize, ($MaxCombinedBidMicro / 1e6).ToString("0.00", $inv), $QueueDecay))
    $summaryLines.Add(("Outputs: {0}" -f $sweepDir))
    $summaryLines.Add(($fmt -f $headers))
    $summaryLines.Add(($fmt -f $dashes))
    foreach ($r in $sorted) {
        $vals = @($cols | ForEach-Object { $r.($_.Prop) })
        $summaryLines.Add(($fmt -f $vals))
    }

    $summaryPath = Join-Path $sweepDir "summary.txt"
    $summaryLines | Set-Content -LiteralPath $summaryPath -Encoding utf8

    Write-Host ""
    Write-Host $summaryLines[0] -ForegroundColor Green
    for ($i = 1; $i -lt $summaryLines.Count; $i++) {
        Write-Host $summaryLines[$i]
    }
}
finally {
    Pop-Location
}
