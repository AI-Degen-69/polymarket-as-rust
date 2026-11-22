<#
.SYNOPSIS
    Offline parameter sweep runner over recorded Polymarket feed captures.
.DESCRIPTION
    Builds the release `poly-as` binary once (unless -SkipBuild is passed),
    replays one or more recorded JSONL captures across a grid of Avellaneda-Stoikov
    math parameters using `poly-as lifecycle --source replay`, and prints a ranked
    comparison table sorted descending by NetPnL.
    Primary host is Windows 11 PowerShell (pwsh); the script also resolves the
    correct executable name on Linux/macOS and respects CARGO_TARGET_DIR.
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -InputPath logs/captures/22-11_05-56-27Z_20s_1w.jsonl -Gammas 0.02,0.08,0.15 -VarianceSpreads false
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -AllCaptures -Gammas 0.25 -VarianceSpreads true -MinSpreadTicks 2 -MaxCombinedBidMicro 980000
.NOTES
    Each single-capture invocation writes its outputs to a unique UTC-timestamped folder under
    logs/sweeps/<DD-MM_HH-MM-SSZ>_<N>runs_<capture-stem>/.
    Multi-capture invocations write to logs/sweeps/<DD-MM_HH-MM-SSZ>_<N>runs_<M>caps-<W>w/
    with per-capture outputs under <sweepDir>/<capture-stem>/<combo-stem>.{jsonl,log}.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string[]]$InputPath = @(),

    [switch]$Latest,

    [switch]$AllCaptures,

    [int]$MinWindows = 2,

    [string[]]$Gammas = @("0.02", "0.05", "0.08", "0.12", "0.15"),

    [string[]]$VarianceSpreads = @("false", "true"),

    [string[]]$MinSpreadTicks = @("1"),

    [string[]]$MaxCrossTickLossMicros = @("off"),

    [string[]]$MaxEntryProbs = @("off"),

    [string[]]$LeadLagThresholds = @("0.02"),

    [string[]]$LeadLagWindows = @("1000"),

    [string[]]$LeadLagCooldowns = @("1000"),

    [string[]]$LeadLagDirectionals = @("true"),

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
$parsedLLThresholds = @($LeadLagThresholds | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    $t = [double]::Parse($_, $inv)
    if ([double]::IsNaN($t) -or [double]::IsInfinity($t) -or $t -le 0.0 -or $t -ge 1.0) {
        throw "Invalid LeadLagThresholds entry '$_' (expected finite number in (0.0, 1.0))"
    }
    $t.ToString("R", $inv)
})
$parsedLLWindows = @($LeadLagWindows | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    $w = [uint64]::Parse($_, $inv)
    if ($w -gt 6000) {
        throw "Invalid LeadLagWindows entry '$_' (expected 0..=6000)"
    }
    $w
})
$parsedLLCooldowns = @($LeadLagCooldowns | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    $c = [uint64]::Parse($_, $inv)
    if ($c -gt 10000) {
        throw "Invalid LeadLagCooldowns entry '$_' (expected 0..=10000)"
    }
    $c
})
$parsedLLDirectionals = @($LeadLagDirectionals | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim().ToLowerInvariant() } | Where-Object { $_ -ne '' })
foreach ($d in $parsedLLDirectionals) {
    if ($d -ne "true" -and $d -ne "false") {
        throw "Invalid LeadLagDirectionals entry '$d' (expected 'true' or 'false')"
    }
}
if ($parsedLLThresholds.Count -eq 0 -or $parsedLLWindows.Count -eq 0 -or $parsedLLCooldowns.Count -eq 0 -or $parsedLLDirectionals.Count -eq 0) {
    throw "LeadLagThresholds, LeadLagWindows, LeadLagCooldowns, and LeadLagDirectionals must each contain at least one value"
}
$showCrossLoss = @($parsedCrossLosses | Where-Object { $_ -ne "off" }).Count -gt 0
$showEntryProb = @($parsedMaxEntryProbs | Where-Object { $_ -ne "off" }).Count -gt 0
$showLLThresh = ($parsedLLThresholds.Count -gt 1) -or (@($parsedLLThresholds | Where-Object { [double]::Parse($_, $inv) -ne 0.02 }).Count -gt 0)
$showLLWin = ($parsedLLWindows.Count -gt 1) -or (@($parsedLLWindows | Where-Object { $_ -ne 1000 }).Count -gt 0)
$showLLCd = ($parsedLLCooldowns.Count -gt 1) -or (@($parsedLLCooldowns | Where-Object { $_ -ne 1000 }).Count -gt 0)
$showLLDir = ($parsedLLDirectionals.Count -gt 1) -or (@($parsedLLDirectionals | Where-Object { $_ -ne "true" }).Count -gt 0)
foreach ($v in $parsedVarSpreads) {
    if ($v -ne "true" -and $v -ne "false") {
        throw "Invalid VarianceSpreads entry '$v' (expected 'true' or 'false')"
    }
}

if ($MinWindows -lt 1) {
    throw "MinWindows must be >= 1 (got $MinWindows)"
}

foreach ($minTicks in $parsedMinTicks) {
    $sumMicro = [int64]$MaxCombinedBidMicro + ([int64]$minTicks * 10000L)
    if ($sumMicro -gt 1000000L) {
        $maxAllowedBid = 1000000L - ([int64]$minTicks * 10000L)
        throw "Invalid config invariant: MaxCombinedBidMicro ($MaxCombinedBidMicro) + MinSpreadTicks ($minTicks) * 10000 = $sumMicro > 1000000. Lower -MaxCombinedBidMicro to <= $maxAllowedBid or lower -MinSpreadTicks."
    }
}

$parsedInputPaths = @($InputPath | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })

if ($AllCaptures -and $parsedInputPaths.Count -gt 0) {
    throw "Cannot pass both -InputPath and -AllCaptures"
}
if ($Latest -and $parsedInputPaths.Count -gt 0) {
    throw "Cannot pass both -InputPath and -Latest"
}
if ($Latest -and $AllCaptures) {
    throw "Cannot pass both -AllCaptures and -Latest"
}

$resolvedInputs = [System.Collections.Generic.List[string]]::new()
$seenStems = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($p in $parsedInputPaths) {
    if (-not (Test-Path -LiteralPath $p -PathType Leaf)) {
        throw "Input capture file not found: $p"
    }
    $resolved = (Resolve-Path -LiteralPath $p).Path
    $stem = [System.IO.Path]::GetFileNameWithoutExtension($resolved)
    if (-not $seenStems.Add($stem)) {
        throw "Duplicate capture stem '$stem' in -InputPath (cannot disambiguate output subfolders): $resolved"
    }
    $resolvedInputs.Add($resolved)
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    if ($resolvedInputs.Count -eq 0) {
        $capturesDir = Join-Path $repoRoot "logs/captures"
        if (-not (Test-Path -LiteralPath $capturesDir)) {
            throw "Captures directory not found: $capturesDir (pass -InputPath or run poly-as feeds --discover)"
        }
        if ($AllCaptures) {
            $matchedFiles = @(Get-ChildItem -LiteralPath $capturesDir -Filter "*.jsonl" -File |
                Where-Object {
                    -not $_.Name.EndsWith(".inprogress.jsonl", [System.StringComparison]::OrdinalIgnoreCase) -and
                    -not $_.Name.StartsWith("polybacktest_", [System.StringComparison]::OrdinalIgnoreCase) -and
                    $_.Name -match '_(\d+)w\.jsonl$' -and
                    [int]$Matches[1] -ge $MinWindows
                } |
                Sort-Object -Property LastWriteTimeUtc)
            if ($matchedFiles.Count -eq 0) {
                throw "No finalized .jsonl captures matching _<N>w.jsonl with N >= $MinWindows found in $capturesDir"
            }
            foreach ($f in $matchedFiles) {
                $resolvedInputs.Add($f.FullName)
            }
            Write-Host ("[SWEEP] Auto-selected {0} capture(s) with >= {1} windows:" -f $resolvedInputs.Count, $MinWindows) -ForegroundColor Cyan
            foreach ($f in $matchedFiles) {
                Write-Host ("  - {0}" -f $f.Name) -ForegroundColor Cyan
            }
        } else {
            $latestFile = Get-ChildItem -LiteralPath $capturesDir -Filter "*.jsonl" -File |
                Where-Object { -not $_.Name.EndsWith(".inprogress.jsonl", [System.StringComparison]::OrdinalIgnoreCase) } |
                Sort-Object -Property LastWriteTimeUtc -Descending |
                Select-Object -First 1
            if ($null -eq $latestFile) {
                throw "No finalized .jsonl captures found in $capturesDir (pass -InputPath or run poly-as feeds --discover)"
            }
            $resolvedInputs.Add($latestFile.FullName)
            Write-Host "[SWEEP] Auto-selected latest capture: $($latestFile.FullName)" -ForegroundColor Cyan
        }
    }

    $captures = [System.Collections.Generic.List[PSCustomObject]]::new()
    $totalWindows = 0
    foreach ($capPath in $resolvedInputs) {
        $capName = [System.IO.Path]::GetFileName($capPath)
        $capStem = [System.IO.Path]::GetFileNameWithoutExtension($capPath)
        $capWindows = if ($capName -match '_(\d+)w\.jsonl$') { [int]$Matches[1] } else { 0 }
        $totalWindows += $capWindows
        $captures.Add([PSCustomObject]@{
            Path    = $capPath
            Name    = $capName
            Stem    = $capStem
            Windows = $capWindows
        })
    }
    $isMultiCapture = $captures.Count -gt 1

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

    $totalRuns = $parsedGammas.Count * $parsedVarSpreads.Count * $parsedMinTicks.Count * $parsedCrossLosses.Count * $parsedMaxEntryProbs.Count * $parsedLLThresholds.Count * $parsedLLWindows.Count * $parsedLLCooldowns.Count * $parsedLLDirectionals.Count
    $stamp = [DateTime]::UtcNow.ToString("dd-MM_HH-mm-ssZ", $inv)
    $sweepBase = Join-Path $repoRoot "logs/sweeps"
    if (-not (Test-Path -LiteralPath $sweepBase)) {
        New-Item -ItemType Directory -Force -Path $sweepBase | Out-Null
    }
    $baseDirName = if ($isMultiCapture) {
        "{0}_{1}runs_{2}caps-{3}w" -f $stamp, $totalRuns, $captures.Count, $totalWindows
    } else {
        "{0}_{1}runs_{2}" -f $stamp, $totalRuns, $captures[0].Stem
    }
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
    foreach ($cap in $captures) {
        if ($cap.Path.StartsWith($sweepDir, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Input capture must not live inside this sweep output folder: $($cap.Path)"
        }
        if ($isMultiCapture) {
            $capSubDir = Join-Path $sweepDir $cap.Stem
            New-Item -ItemType Directory -Force -Path $capSubDir | Out-Null
        }
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
                        foreach ($llThresh in $parsedLLThresholds) {
                            foreach ($llWin in $parsedLLWindows) {
                                foreach ($llCd in $parsedLLCooldowns) {
                                    foreach ($llDir in $parsedLLDirectionals) {
                        $runIdx++
                        $stem = "g${gammaTag}_var-${varStr}_t${minTicks}_b${MaxCombinedBidMicro}_${QueueDecay}"
                        if ($showCrossLoss) {
                            $stem += "_xt-${crossLoss}"
                        }
                        if ($showEntryProb) {
                            $epTag = $entryProb.Replace(".", "p")
                            $stem += "_ep-${epTag}"
                        }
                        if ($showLLThresh) {
                            $lltTag = $llThresh.Replace(".", "p")
                            $stem += "_llt-${lltTag}"
                        }
                        if ($showLLWin) {
                            $stem += "_llw-${llWin}"
                        }
                        if ($showLLCd) {
                            $stem += "_llc-${llCd}"
                        }
                        if ($showLLDir) {
                            $stem += "_lld-${llDir}"
                        }

                        $extraProgress = ""
                        if ($showCrossLoss) { $extraProgress += " xtick=$crossLoss" }
                        if ($showEntryProb) { $extraProgress += " max_p=$entryProb" }
                        if ($showLLThresh) { $extraProgress += " ll_th=$llThresh" }
                        if ($showLLWin) { $extraProgress += " ll_win=$llWin" }
                        if ($showLLCd) { $extraProgress += " ll_cd=$llCd" }
                        if ($showLLDir) { $extraProgress += " ll_dir=$llDir" }

                        $capRuns = [System.Collections.Generic.List[PSCustomObject]]::new()
                        for ($capIdx = 0; $capIdx -lt $captures.Count; $capIdx++) {
                            $cap = $captures[$capIdx]
                            if ($isMultiCapture) {
                                $capOutDir = Join-Path $sweepDir $cap.Stem
                                $outJsonl = Join-Path $capOutDir "$stem.jsonl"
                                $outLog = Join-Path $capOutDir "$stem.log"
                                Write-Host ("[{0}/{1} | {2}/{3}] Replaying {4} gamma={5} var_spread={6} min_ticks={7}{8}..." -f $runIdx, $totalRuns, ($capIdx + 1), $captures.Count, $cap.Stem, $gammaStr, $varStr, $minTicks, $extraProgress)
                            } else {
                                $outJsonl = Join-Path $sweepDir "$stem.jsonl"
                                $outLog = Join-Path $sweepDir "$stem.log"
                                Write-Host ("[{0}/{1}] Replaying gamma={2} var_spread={3} min_ticks={4}{5}..." -f $runIdx, $totalRuns, $gammaStr, $varStr, $minTicks, $extraProgress)
                            }

                            $cliArgs = @(
                                "lifecycle",
                                "--source", "replay",
                                "--input", $cap.Path,
                                "--v-max", $VMax.ToString($inv),
                                "--base-size", $BaseSize.ToString($inv),
                                "--queue-decay", $QueueDecay,
                                "--gamma", $gammaStr,
                                "--include-variance-spread", $varStr,
                                "--min-spread-ticks", $minTicks.ToString($inv),
                                "--max-combined-bid-micro", $MaxCombinedBidMicro.ToString($inv),
                                "--lead-lag-threshold", $llThresh,
                                "--lead-lag-window-ms", $llWin.ToString($inv),
                                "--lead-lag-cooldown-ms", $llCd.ToString($inv),
                                "--lead-lag-directional", $llDir,
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
                            $prevLLThreshEnv = $env:LEAD_LAG_CANCEL_THRESHOLD
                            $prevLLWinEnv = $env:LEAD_LAG_WINDOW_MS
                            $prevLLCdEnv = $env:LEAD_LAG_COOLDOWN_MS
                            $prevLLDirEnv = $env:LEAD_LAG_DIRECTIONAL
                            try {
                                if ($crossLoss -eq "off") {
                                    $env:MAX_CROSS_TICK_LOSS_MICRO = "off"
                                }
                                if ($entryProb -eq "off") {
                                    $env:MAX_ENTRY_PROB = "off"
                                }
                                $env:LEAD_LAG_CANCEL_THRESHOLD = $llThresh
                                $env:LEAD_LAG_WINDOW_MS = $llWin.ToString($inv)
                                $env:LEAD_LAG_COOLDOWN_MS = $llCd.ToString($inv)
                                $env:LEAD_LAG_DIRECTIONAL = $llDir
                                & $binPath @cliArgs *> $outLog
                                if ($LASTEXITCODE -ne 0) {
                                    $tail = if (Test-Path -LiteralPath $outLog) { (Get-Content -LiteralPath $outLog -Tail 20) -join "`n" } else { "" }
                                    throw "Sweep run failed (exit=$LASTEXITCODE) for $($cap.Stem)/$stem.`n$tail"
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
                                if ($null -eq $prevLLThreshEnv) {
                                    Remove-Item Env:LEAD_LAG_CANCEL_THRESHOLD -ErrorAction SilentlyContinue
                                } else {
                                    $env:LEAD_LAG_CANCEL_THRESHOLD = $prevLLThreshEnv
                                }
                                if ($null -eq $prevLLWinEnv) {
                                    Remove-Item Env:LEAD_LAG_WINDOW_MS -ErrorAction SilentlyContinue
                                } else {
                                    $env:LEAD_LAG_WINDOW_MS = $prevLLWinEnv
                                }
                                if ($null -eq $prevLLCdEnv) {
                                    Remove-Item Env:LEAD_LAG_COOLDOWN_MS -ErrorAction SilentlyContinue
                                } else {
                                    $env:LEAD_LAG_COOLDOWN_MS = $prevLLCdEnv
                                }
                                if ($null -eq $prevLLDirEnv) {
                                    Remove-Item Env:LEAD_LAG_DIRECTIONAL -ErrorAction SilentlyContinue
                                } else {
                                    $env:LEAD_LAG_DIRECTIONAL = $prevLLDirEnv
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
                            $netPlusReb = $netPnl + $rebate
                            $basePnl = [double]$payload.baseline_pnl_micro / 1e6
                            $alphaEdge = $netPnl - $basePnl
                            $pairs = [double]$payload.total_pairs_merged_micro / 1e6
                            $fills = [int]$payload.fills_count
                            $merges = [int]$payload.merges_count
                            $rpm = [int]$payload.rpm_60s
                            $mo1s = [double]$payload.markout_1s_bps
                            $mo5s = [double]$payload.markout_5s_bps

                            $capRuns.Add([PSCustomObject]@{
                                CaptureName  = $cap.Name
                                CaptureStem  = $cap.Stem
                                Windows      = $cap.Windows
                                Fills        = $fills
                                Merges       = $merges
                                PairsNum     = $pairs
                                RealizedNum  = $realized
                                UnrealNum    = $unreal
                                NetPnLNum    = $netPnl
                                RebateNum    = $rebate
                                NetPlusRebNum = $netPlusReb
                                BasePnLNum   = $basePnl
                                AlphaEdgeNum = $alphaEdge
                                RPM          = $rpm
                                MO1sNum      = $mo1s
                                MO5sNum      = $mo5s
                            })
                        }

                        if (-not $isMultiCapture) {
                            $c = $capRuns[0]
                            $results.Add([PSCustomObject]@{
                                Gamma      = $gammaStr
                                VarSpr     = $varStr
                                MinT       = $minTicks
                                XTick      = $crossLoss
                                MaxP       = $entryProb
                                LLTh       = $llThresh
                                LLWin      = $llWin
                                LLCd       = $llCd
                                LLDir      = $llDir
                                Fills      = $c.Fills
                                Merges     = $c.Merges
                                Pairs      = $c.PairsNum.ToString("F2", $inv)
                                Realized   = $c.RealizedNum.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                Unreal     = $c.UnrealNum.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                NetPnL     = $c.NetPnLNum.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                Rebate     = $c.RebateNum.ToString("0.0000", $inv)
                                BasePnL    = $c.BasePnLNum.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                AlphaEdge  = $c.AlphaEdgeNum.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                RPM        = $c.RPM
                                MO1s       = $c.MO1sNum.ToString("+0.0;-0.0;+0.0", $inv)
                                MO5s       = $c.MO5sNum.ToString("+0.0;-0.0;+0.0", $inv)
                                NetPnLNum  = $c.NetPnLNum
                            })
                        } else {
                            $sumFills = 0
                            $sumMerges = 0
                            $sumPairs = 0.0
                            $sumRealized = 0.0
                            $sumUnreal = 0.0
                            $sumNetPnl = 0.0
                            $sumRebate = 0.0
                            $sumBasePnl = 0.0
                            $sumAlphaEdge = 0.0
                            $maxRpm = 0
                            $weightedMo1s = 0.0
                            $weightedMo5s = 0.0
                            $winCaps = 0

                            foreach ($c in $capRuns) {
                                $sumFills += $c.Fills
                                $sumMerges += $c.Merges
                                $sumPairs += $c.PairsNum
                                $sumRealized += $c.RealizedNum
                                $sumUnreal += $c.UnrealNum
                                $sumNetPnl += $c.NetPnLNum
                                $sumRebate += $c.RebateNum
                                $sumBasePnl += $c.BasePnLNum
                                $sumAlphaEdge += $c.AlphaEdgeNum
                                if ($c.RPM -gt $maxRpm) { $maxRpm = $c.RPM }
                                $weightedMo1s += ($c.Fills * $c.MO1sNum)
                                $weightedMo5s += ($c.Fills * $c.MO5sNum)
                                if ($c.NetPlusRebNum -gt 0.0) { $winCaps++ }
                            }

                            $sumNetPlusReb = $sumNetPnl + $sumRebate
                            $avgMo1s = if ($sumFills -gt 0) { $weightedMo1s / $sumFills } else { 0.0 }
                            $avgMo5s = if ($sumFills -gt 0) { $weightedMo5s / $sumFills } else { 0.0 }

                            $results.Add([PSCustomObject]@{
                                Gamma      = $gammaStr
                                VarSpr     = $varStr
                                MinT       = $minTicks
                                XTick      = $crossLoss
                                MaxP       = $entryProb
                                LLTh       = $llThresh
                                LLWin      = $llWin
                                LLCd       = $llCd
                                LLDir      = $llDir
                                Fills      = $sumFills
                                Merges     = $sumMerges
                                Pairs      = $sumPairs.ToString("F2", $inv)
                                Realized   = $sumRealized.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                Unreal     = $sumUnreal.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                NetPnL     = $sumNetPnl.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                Rebate     = $sumRebate.ToString("0.0000", $inv)
                                NetPlusReb = $sumNetPlusReb.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                BasePnL    = $sumBasePnl.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                AlphaEdge  = $sumAlphaEdge.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                RPM        = $maxRpm
                                MO1s       = $avgMo1s.ToString("+0.0;-0.0;+0.0", $inv)
                                MO5s       = $avgMo5s.ToString("+0.0;-0.0;+0.0", $inv)
                                WinCap     = ("{0}/{1}" -f $winCaps, $captures.Count)
                                NetPnLNum  = $sumNetPnl
                                CapRuns    = $capRuns
                            })
                        }
                                    }
                                }
                            }
                        }
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
    if ($showLLThresh) {
        $cols.Add(@{ Name = "LLTh"; Dash = "-----"; Width = 5; Prop = "LLTh" })
    }
    if ($showLLWin) {
        $cols.Add(@{ Name = "LLWin"; Dash = "-----"; Width = 5; Prop = "LLWin" })
    }
    if ($showLLCd) {
        $cols.Add(@{ Name = "LLCd"; Dash = "----"; Width = 4; Prop = "LLCd" })
    }
    if ($showLLDir) {
        $cols.Add(@{ Name = "LLDir"; Dash = "-----"; Width = 5; Prop = "LLDir" })
    }
    $cols.Add(@{ Name = "Fills"; Dash = "-----"; Width = 5; Prop = "Fills" })
    $cols.Add(@{ Name = "Merges"; Dash = "------"; Width = 6; Prop = "Merges" })
    $cols.Add(@{ Name = "Pairs"; Dash = "-------"; Width = 7; Prop = "Pairs" })
    $cols.Add(@{ Name = "Realized"; Dash = "--------"; Width = 9; Prop = "Realized" })
    $cols.Add(@{ Name = "Unreal"; Dash = "--------"; Width = 9; Prop = "Unreal" })
    $cols.Add(@{ Name = "NetPnL"; Dash = "--------"; Width = 9; Prop = "NetPnL" })
    $cols.Add(@{ Name = "Rebate"; Dash = "------"; Width = 7; Prop = "Rebate" })
    if ($isMultiCapture) {
        $cols.Add(@{ Name = "Net+Reb"; Dash = "--------"; Width = 9; Prop = "NetPlusReb" })
    }
    $cols.Add(@{ Name = "BasePnL"; Dash = "--------"; Width = 9; Prop = "BasePnL" })
    $cols.Add(@{ Name = "AlphaEdge"; Dash = "---------"; Width = 9; Prop = "AlphaEdge" })
    $cols.Add(@{ Name = "RPM"; Dash = "---"; Width = 4; Prop = "RPM" })
    $cols.Add(@{ Name = "MO1s"; Dash = "-----"; Width = 6; Prop = "MO1s" })
    $cols.Add(@{ Name = "MO5s"; Dash = "-----"; Width = 6; Prop = "MO5s" })
    if ($isMultiCapture) {
        $cols.Add(@{ Name = "WinCap"; Dash = "------"; Width = 6; Prop = "WinCap" })
    }

    $fmtParts = for ($ci = 0; $ci -lt $cols.Count; $ci++) { "{$ci,$($cols[$ci].Width)}" }
    $fmt = $fmtParts -join " "
    $headers = @($cols | ForEach-Object { $_.Name })
    $dashes = @($cols | ForEach-Object { $_.Dash })

    $summaryLines = [System.Collections.Generic.List[string]]::new()
    if (-not $isMultiCapture) {
        $summaryLines.Add("=== OFFLINE PARAMETER SWEEP RESULTS (sorted by NetPnL desc) ===")
        $summaryLines.Add(('Input: {0} | V_max=${1} | Base={2}sh | MaxPair=${3} | Decay={4}' -f $captures[0].Path, $VMax, $BaseSize, ($MaxCombinedBidMicro / 1e6).ToString("0.00", $inv), $QueueDecay))
        $summaryLines.Add(("Outputs: {0}" -f $sweepDir))
        $summaryLines.Add(($fmt -f $headers))
        $summaryLines.Add(($fmt -f $dashes))
        foreach ($r in $sorted) {
            $vals = @($cols | ForEach-Object { $r.($_.Prop) })
            $summaryLines.Add(($fmt -f $vals))
        }
    } else {
        $summaryLines.Add("=== OFFLINE MULTI-CAPTURE PARAMETER SWEEP RESULTS (sorted by NetPnL desc) ===")
        $summaryLines.Add(('Captures: {0} ({1}w total) | V_max=${2} | Base={3}sh | MaxPair=${4} | Decay={5}' -f $captures.Count, $totalWindows, $VMax, $BaseSize, ($MaxCombinedBidMicro / 1e6).ToString("0.00", $inv), $QueueDecay))
        $summaryLines.Add(("Outputs: {0}" -f $sweepDir))
        $summaryLines.Add("Included Captures:")
        for ($ci = 0; $ci -lt $captures.Count; $ci++) {
            $c = $captures[$ci]
            $winTag = if ($c.Windows -gt 0) { "{0}w" -f $c.Windows } else { "?w" }
            $summaryLines.Add(("  [{0}] {1} ({2})" -f ($ci + 1), $c.Name, $winTag))
        }
        $summaryLines.Add("")
        $summaryLines.Add(($fmt -f $headers))
        $summaryLines.Add(($fmt -f $dashes))
        foreach ($r in $sorted) {
            $vals = @($cols | ForEach-Object { $r.($_.Prop) })
            $summaryLines.Add(($fmt -f $vals))
        }

        $summaryLines.Add("")
        $summaryLines.Add("--- Per-Capture Breakdown ---")
        $capStemMaxLen = ($captures | ForEach-Object { $_.Stem.Length } | Measure-Object -Maximum).Maximum
        $capWidth = -[Math]::Max(12, $capStemMaxLen)
        $capFmt = "  {0,$capWidth} {1,4} {2,5} {3,6} {4,7} {5,9} {6,9} {7,9} {8,7} {9,9} {10,9} {11,9} {12,4} {13,6} {14,6}"
        foreach ($r in $sorted) {
            $comboDesc = "gamma={0} var={1} min_t={2}" -f $r.Gamma, $r.VarSpr, $r.MinT
            if ($showCrossLoss) { $comboDesc += " xtick=$($r.XTick)" }
            if ($showEntryProb) { $comboDesc += " max_p=$($r.MaxP)" }
            if ($showLLThresh) { $comboDesc += " ll_th=$($r.LLTh)" }
            if ($showLLWin) { $comboDesc += " ll_win=$($r.LLWin)" }
            if ($showLLCd) { $comboDesc += " ll_cd=$($r.LLCd)" }
            if ($showLLDir) { $comboDesc += " ll_dir=$($r.LLDir)" }
            $summaryLines.Add(("[{0} | NetPnL={1} Net+Reb={2} WinCap={3}]" -f $comboDesc, $r.NetPnL, $r.NetPlusReb, $r.WinCap))
            $summaryLines.Add(($capFmt -f "Capture", "Win", "Fills", "Merges", "Pairs", "Realized", "Unreal", "NetPnL", "Rebate", "Net+Reb", "BasePnL", "AlphaEdge", "RPM", "MO1s", "MO5s"))
            foreach ($cr in $r.CapRuns) {
                $wTag = if ($cr.Windows -gt 0) { "{0}w" -f $cr.Windows } else { "?w" }
                $summaryLines.Add(($capFmt -f
                    $cr.CaptureStem,
                    $wTag,
                    $cr.Fills,
                    $cr.Merges,
                    $cr.PairsNum.ToString("F2", $inv),
                    $cr.RealizedNum.ToString("+0.0000;-0.0000;+0.0000", $inv),
                    $cr.UnrealNum.ToString("+0.0000;-0.0000;+0.0000", $inv),
                    $cr.NetPnLNum.ToString("+0.0000;-0.0000;+0.0000", $inv),
                    $cr.RebateNum.ToString("0.0000", $inv),
                    $cr.NetPlusRebNum.ToString("+0.0000;-0.0000;+0.0000", $inv),
                    $cr.BasePnLNum.ToString("+0.0000;-0.0000;+0.0000", $inv),
                    $cr.AlphaEdgeNum.ToString("+0.0000;-0.0000;+0.0000", $inv),
                    $cr.RPM,
                    $cr.MO1sNum.ToString("+0.0;-0.0;+0.0", $inv),
                    $cr.MO5sNum.ToString("+0.0;-0.0;+0.0", $inv)
                ))
            }
        }
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
