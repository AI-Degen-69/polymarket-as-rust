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

    [string[]]$MaxCrossTickLossMicros = @("80000"),

    [string[]]$MaxEntryProbs = @("off"),

    [string[]]$MaxEntryPrices = @("0.60"),

    [string[]]$LeadLagThresholds = @("0.015"),

    [string[]]$LeadLagWindows = @("1000"),

    [string[]]$LeadLagCooldowns = @("1000"),

    [string[]]$LeadLagDirectionals = @("true"),

    [string[]]$HedgeSkewMults = @("1.0"),

    [string[]]$CapTier1Hedges = @("false"),

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

# Ensure UTF-8 console rendering for box-drawing characters and status icons
try {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
} catch {
    # Ignore if stdout is redirected in a non-interactive host
}

# ---------------------------------------------------------------------------
# Phase 1: Width-Safe ANSI Styling & 2-Column Parameter Diff Banner Helpers
# ---------------------------------------------------------------------------
$UseAnsi = ($null -eq $env:NO_COLOR) -and -not [Console]::IsOutputRedirected
$Esc = [char]27
$AnsiReset      = "$Esc[0m"
$AnsiBoldCyan   = "$Esc[1;36m"
$AnsiCyan       = "$Esc[36m"
$AnsiBoldYellow = "$Esc[1;33m"
$AnsiYellow     = "$Esc[33m"
$AnsiBoldGreen  = "$Esc[1;32m"
$AnsiGreen      = "$Esc[32m"
$AnsiBoldRed    = "$Esc[1;31m"
$AnsiRed        = "$Esc[31m"
$AnsiGray       = "$Esc[90m"
$AnsiBoldWhite  = "$Esc[1;37m"

function Format-ColorText {
    param(
        [string]$Text,
        [string]$Code
    )
    if (-not $UseAnsi -or [string]::IsNullOrEmpty($Code)) {
        return $Text
    }
    return "${Code}${Text}${AnsiReset}"
}

function Format-SignedPnlCell {
    param(
        [double]$Value,
        [int]$Width = 9,
        [bool]$Bold = $false,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $raw = $Value.ToString("+0.0000;-0.0000;+0.0000", $Culture).PadLeft($Width)
    if ($Value -gt 1e-9) {
        $code = if ($Bold) { $AnsiBoldGreen } else { $AnsiGreen }
        return (Format-ColorText -Text $raw -Code $code)
    } elseif ($Value -lt -1e-9) {
        $code = if ($Bold) { $AnsiBoldRed } else { $AnsiRed }
        return (Format-ColorText -Text $raw -Code $code)
    } else {
        return (Format-ColorText -Text $raw -Code $AnsiGray)
    }
}

function Format-SignedMarkoutCell {
    param(
        [double]$Value,
        [int]$Width = 6,
        [bool]$Bold = $false,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $raw = $Value.ToString("+0.0;-0.0;+0.0", $Culture).PadLeft($Width)
    if ($Value -gt 1e-6) {
        $code = if ($Bold) { $AnsiBoldGreen } else { $AnsiGreen }
        return (Format-ColorText -Text $raw -Code $code)
    } elseif ($Value -lt -1e-6) {
        $code = if ($Bold) { $AnsiBoldRed } else { $AnsiRed }
        return (Format-ColorText -Text $raw -Code $code)
    } else {
        return (Format-ColorText -Text $raw -Code $AnsiGray)
    }
}

function Format-RebateCell {
    param(
        [double]$Value,
        [int]$Width = 7,
        [bool]$Bold = $false,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $raw = $Value.ToString("0.0000", $Culture).PadLeft($Width)
    if ($Value -gt 1e-9) {
        $code = if ($Bold) { $AnsiBoldCyan } else { $AnsiCyan }
        return (Format-ColorText -Text $raw -Code $code)
    } elseif ($Value -lt -1e-9) {
        $code = if ($Bold) { $AnsiBoldRed } else { $AnsiRed }
        return (Format-ColorText -Text $raw -Code $code)
    } else {
        return (Format-ColorText -Text $raw -Code $AnsiGray)
    }
}

function Format-WinRateCell {
    param(
        [double]$Pct,
        [int]$Width = 7,
        [bool]$Bold = $false,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $raw = ("{0:F0}%" -f $Pct).PadLeft($Width)
    if ($Pct -ge 70.0) {
        $code = if ($Bold) { $AnsiBoldGreen } else { $AnsiGreen }
        return (Format-ColorText -Text $raw -Code $code)
    } elseif ($Pct -ge 40.0) {
        $code = if ($Bold) { $AnsiBoldYellow } else { $AnsiYellow }
        return (Format-ColorText -Text $raw -Code $code)
    } else {
        $code = if ($Bold) { $AnsiBoldRed } else { $AnsiRed }
        return (Format-ColorText -Text $raw -Code $code)
    }
}

function Test-EntryPriceSwept {
    param(
        [string[]]$Prices,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    if ($Prices.Count -gt 1) { return $true }
    return (@($Prices | Where-Object {
        -not $_.Equals("off", [System.StringComparison]::OrdinalIgnoreCase) -and
        [double]::Parse($_, $Culture) -ne 0.60
    }).Count -gt 0)
}

function Format-ParamCell {
    param(
        [string]$Label,
        [string]$CurrVal,
        [string]$PrevVal,
        [bool]$IsLeftColumn
    )
    $isChanged = (-not [string]::IsNullOrEmpty($PrevVal)) -and ($CurrVal -ne $PrevVal)
    $paddedLabel = $Label.PadRight(11)
    if ($IsLeftColumn) {
        if ($isChanged) {
            $tag = "{0}  ⚡ (was: {1})" -f $CurrVal, $PrevVal
            $padTo = if ($tag.Contains("⚡")) { 23 } else { 24 }
            $paddedVal = $tag.PadRight($padTo)
            $coloredVal = Format-ColorText -Text $paddedVal -Code $AnsiBoldYellow
            return ("{0}: {1}" -f $paddedLabel, $coloredVal)
        } else {
            return ("{0}: {1}" -f $paddedLabel, $CurrVal.PadRight(24))
        }
    } else {
        if ($isChanged) {
            $tag = "{0}  ⚡ (was: {1})" -f $CurrVal, $PrevVal
            $coloredVal = Format-ColorText -Text $tag -Code $AnsiBoldYellow
            return ("{0}: {1}" -f $paddedLabel, $coloredVal)
        } else {
            return ("{0}: {1}" -f $paddedLabel, $CurrVal)
        }
    }
}

function Write-ConfigBanner {
    param(
        [int]$Index,
        [int]$Total,
        [hashtable]$Curr,
        [hashtable]$Prev
    )
    $headerPrefix = "⚙️  [CONFIG {0} / {1}] " -f $Index, $Total
    $dashCount = [Math]::Max(10, 100 - $headerPrefix.Length)
    $topRule = (Format-ColorText -Text $headerPrefix -Code $AnsiBoldCyan) + (Format-ColorText -Text ("-" * $dashCount) -Code $AnsiGray)
    $botRule = Format-ColorText -Text ("-" * 100) -Code $AnsiGray
    $pipe = Format-ColorText -Text " |  " -Code $AnsiGray

    $pairs = @(
        @("Gamma",     "VarSpread"),
        @("MinTicks",  "XTick"),
        @("MaxP",      "MaxPx"),
        @("LLThresh",  "LLWin"),
        @("LLCd",      "LLDir"),
        @("HedgeSkew", "CapTier1")
    )

    Write-Host ""
    Write-Host $topRule
    foreach ($pair in $pairs) {
        $kLeft = $pair[0]
        $kRight = $pair[1]
        $prevL = if ($null -ne $Prev) { [string]$Prev[$kLeft] } else { "" }
        $prevR = if ($null -ne $Prev) { [string]$Prev[$kRight] } else { "" }
        $leftStr = Format-ParamCell -Label $kLeft -CurrVal ([string]$Curr[$kLeft]) -PrevVal $prevL -IsLeftColumn $true
        $rightStr = Format-ParamCell -Label $kRight -CurrVal ([string]$Curr[$kRight]) -PrevVal $prevR -IsLeftColumn $false
        Write-Host ("{0}{1}{2}" -f $leftStr, $pipe, $rightStr)
    }
    Write-Host $botRule
}

# ---------------------------------------------------------------------------
# Parameter Parsing & Validation
# ---------------------------------------------------------------------------
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
$parsedMaxEntryPrices = @($MaxEntryPrices | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    if ($_.Equals("off", [System.StringComparison]::OrdinalIgnoreCase)) {
        "off"
    } else {
        $p = [double]::Parse($_, $inv)
        if ([double]::IsNaN($p) -or [double]::IsInfinity($p) -or $p -le 0.0 -or $p -ge 1.0) {
            throw "Invalid MaxEntryPrices entry '$_' (expected 'off' or finite number in (0.0, 1.0))"
        }
        $p.ToString("R", $inv)
    }
})

if ($parsedGammas.Count -eq 0 -or $parsedVarSpreads.Count -eq 0 -or $parsedMinTicks.Count -eq 0 -or $parsedCrossLosses.Count -eq 0 -or $parsedMaxEntryProbs.Count -eq 0 -or $parsedMaxEntryPrices.Count -eq 0) {
    throw "Gammas, VarianceSpreads, MinSpreadTicks, MaxCrossTickLossMicros, MaxEntryProbs, and MaxEntryPrices must each contain at least one value"
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
    if ($w -gt 6000 -or ($w -gt 0 -and $w -lt 50)) {
        throw "Invalid LeadLagWindows entry '$_' (expected 0 or 50..=6000)"
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
$parsedHedgeSkews = @($HedgeSkewMults | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    $m = [double]::Parse($_, $inv)
    if ([double]::IsNaN($m) -or [double]::IsInfinity($m) -or $m -lt 0.0 -or $m -gt 1.0) {
        throw "Invalid HedgeSkewMults entry '$_' (expected finite number in [0.0, 1.0])"
    }
    $m
})
$parsedCapTier1 = @($CapTier1Hedges | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim().ToLowerInvariant() } | Where-Object { $_ -ne '' })
foreach ($c in $parsedCapTier1) {
    if ($c -ne "true" -and $c -ne "false") {
        throw "Invalid CapTier1Hedges entry '$c' (expected 'true' or 'false')"
    }
}
if ($parsedHedgeSkews.Count -eq 0 -or $parsedCapTier1.Count -eq 0) {
    throw "HedgeSkewMults and CapTier1Hedges must each contain at least one value"
}

$showCrossLoss = @($parsedCrossLosses | Where-Object { $_ -ne "off" }).Count -gt 0
$showEntryProb = @($parsedMaxEntryProbs | Where-Object { $_ -ne "off" }).Count -gt 0
$showEntryPrice = Test-EntryPriceSwept -Prices $parsedMaxEntryPrices -Culture $inv
$showLLThresh = ($parsedLLThresholds.Count -gt 1) -or (@($parsedLLThresholds | Where-Object { [double]::Parse($_, $inv) -ne 0.015 }).Count -gt 0)
$showLLWin = ($parsedLLWindows.Count -gt 1) -or (@($parsedLLWindows | Where-Object { $_ -ne 1000 }).Count -gt 0)
$showLLCd = ($parsedLLCooldowns.Count -gt 1) -or (@($parsedLLCooldowns | Where-Object { $_ -ne 1000 }).Count -gt 0)
$showLLDir = ($parsedLLDirectionals.Count -gt 1) -or (@($parsedLLDirectionals | Where-Object { $_ -ne "true" }).Count -gt 0)
$showHedgeSkew = ($parsedHedgeSkews.Count -gt 1) -or (@($parsedHedgeSkews | Where-Object { $_ -ne 1.0 }).Count -gt 0)
$showCapTier1 = ($parsedCapTier1.Count -gt 1) -or (@($parsedCapTier1 | Where-Object { $_ -ne "false" }).Count -gt 0)

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
        } else {
            $latestFile = Get-ChildItem -LiteralPath $capturesDir -Filter "*.jsonl" -File |
                Where-Object { -not $_.Name.EndsWith(".inprogress.jsonl", [System.StringComparison]::OrdinalIgnoreCase) } |
                Sort-Object -Property LastWriteTimeUtc -Descending |
                Select-Object -First 1
            if ($null -eq $latestFile) {
                throw "No finalized .jsonl captures found in $capturesDir (pass -InputPath or run poly-as feeds --discover)"
            }
            $resolvedInputs.Add($latestFile.FullName)
        }
    }

    $captures = [System.Collections.Generic.List[PSCustomObject]]::new()
    $totalWindows = 0
    $seenLabels = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    foreach ($capPath in $resolvedInputs) {
        # Identity stays raw (output paths, result matching); only the displayed label is sanitized.
        $capName = [System.IO.Path]::GetFileName($capPath)
        $capStem = [System.IO.Path]::GetFileNameWithoutExtension($capPath)
        $capLabel = $capStem -replace '[\x00-\x1F\x7F]', '?'
        if (-not $seenLabels.Add($capLabel)) {
            $capLabel = "{0} #{1}" -f $capLabel, ($captures.Count + 1)
        }
        $capWindows = if ($capName -match '_(\d+)w\.jsonl$') { [int]$Matches[1] } else { 0 }
        $totalWindows += $capWindows
        $captures.Add([PSCustomObject]@{
            Path    = $capPath
            Name    = $capName
            Stem    = $capStem
            Label   = $capLabel
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
            Write-Host (Format-ColorText -Text "🔨 [BUILD] Building release binary (cargo build --release -p cli --bin poly-as)..." -Code $AnsiCyan)
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

    $totalRuns = $parsedGammas.Count * $parsedVarSpreads.Count * $parsedMinTicks.Count * $parsedCrossLosses.Count * $parsedMaxEntryProbs.Count * $parsedMaxEntryPrices.Count * $parsedLLThresholds.Count * $parsedLLWindows.Count * $parsedLLCooldowns.Count * $parsedLLDirectionals.Count * $parsedHedgeSkews.Count * $parsedCapTier1.Count
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

    foreach ($cap in $captures) {
        if ($cap.Path.StartsWith($sweepDir, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Input capture must not live inside this sweep output folder: $($cap.Path)"
        }
        if ($isMultiCapture) {
            $capSubDir = Join-Path $sweepDir $cap.Stem
            New-Item -ItemType Directory -Force -Path $capSubDir | Out-Null
        }
    }

    # -----------------------------------------------------------------------
    # Phase 2: Top-Level Run Header & 2-Level Hierarchical Streaming Loop
    # -----------------------------------------------------------------------
    $majorRule = Format-ColorText -Text ("=" * 100) -Code $AnsiGray
    $maxPairFormatted = ($MaxCombinedBidMicro / 1e6).ToString("0.00", $inv)
    $sweepTitle = if ($isMultiCapture) {
        "🚀 OFFLINE MULTI-CAPTURE PARAMETER SWEEP ({0} config sets)"
    } else {
        "🚀 OFFLINE PARAMETER SWEEP ({0} config sets)"
    }
    Write-Host ""
    Write-Host $majorRule
    Write-Host (Format-ColorText -Text ($sweepTitle -f $totalRuns) -Code $AnsiBoldCyan)
    Write-Host ("Captures : {0} file(s) ({1} windows total)  |  Fixed: V_max=`${2}, Base={3}sh, MaxPair=`${4}, Decay={5}" -f $captures.Count, $totalWindows, $VMax, $BaseSize, $maxPairFormatted, $QueueDecay)
    Write-Host ("Output   : {0}" -f $sweepDir)
    Write-Host $majorRule

    $sweepStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $results = [System.Collections.Generic.List[PSCustomObject]]::new()
    $runIdx = 0
    $prevParamMap = $null

    foreach ($gamma in $parsedGammas) {
        $gammaStr = $gamma.ToString("R", $inv)
        $gammaTag = $gammaStr.Replace(".", "p").Replace("-", "m")

        foreach ($varStr in $parsedVarSpreads) {
            foreach ($minTicks in $parsedMinTicks) {
                foreach ($crossLoss in $parsedCrossLosses) {
                    foreach ($entryProb in $parsedMaxEntryProbs) {
                        foreach ($entryPrice in $parsedMaxEntryPrices) {
                        foreach ($llThresh in $parsedLLThresholds) {
                            foreach ($llWin in $parsedLLWindows) {
                                foreach ($llCd in $parsedLLCooldowns) {
                                    foreach ($llDir in $parsedLLDirectionals) {
                                        foreach ($hedgeSkew in $parsedHedgeSkews) {
                                            $hsStr = $hedgeSkew.ToString("R", $inv)
                                            $hsTag = $hsStr.Replace(".", "p").Replace("-", "m")
                                            foreach ($capT1 in $parsedCapTier1) {
                                                $runIdx++
                                                $currParamMap = @{
                                                    Gamma     = $gammaStr
                                                    VarSpread = $varStr
                                                    MinTicks  = $minTicks.ToString($inv)
                                                    XTick     = $crossLoss
                                                    MaxP      = $entryProb
                                                    MaxPx     = $entryPrice
                                                    LLThresh  = $llThresh
                                                    LLWin     = $llWin.ToString($inv)
                                                    LLCd      = $llCd.ToString($inv)
                                                    LLDir     = $llDir
                                                    HedgeSkew = $hsStr
                                                    CapTier1  = $capT1
                                                }

                                                # Level 1 Hierarchy: Config Set Banner with Parameter Diff Highlighting
                                                Write-ConfigBanner -Index $runIdx -Total $totalRuns -Curr $currParamMap -Prev $prevParamMap
                                                $prevParamMap = $currParamMap

                                                $stem = "g${gammaTag}_var-${varStr}_t${minTicks}_b${MaxCombinedBidMicro}_${QueueDecay}"
                                                if ($showCrossLoss)  { $stem += "_xt-${crossLoss}" }
                                                if ($showEntryProb)  { $stem += "_ep-$($entryProb.Replace('.', 'p'))" }
                                                if ($showEntryPrice) { $stem += "_epx-$($entryPrice.Replace('.', 'p'))" }
                                                if ($showLLThresh)   { $stem += "_llt-$($llThresh.Replace('.', 'p'))" }
                                                if ($showLLWin)      { $stem += "_llw-${llWin}" }
                                                if ($showLLCd)       { $stem += "_llc-${llCd}" }
                                                if ($showLLDir)      { $stem += "_lld-${llDir}" }
                                                if ($showHedgeSkew)  { $stem += "_hs-${hsTag}" }
                                                if ($showCapTier1)   { $stem += "_t1c-${capT1}" }

                                                $cfgStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
                                                $capRuns = [System.Collections.Generic.List[PSCustomObject]]::new()

                                                for ($capIdx = 0; $capIdx -lt $captures.Count; $capIdx++) {
                                                    $cap = $captures[$capIdx]
                                                    if ($isMultiCapture) {
                                                        $capOutDir = Join-Path $sweepDir $cap.Stem
                                                        $outJsonl = Join-Path $capOutDir "$stem.jsonl"
                                                        $outLog = Join-Path $capOutDir "$stem.log"
                                                    } else {
                                                        $outJsonl = Join-Path $sweepDir "$stem.jsonl"
                                                        $outLog = Join-Path $sweepDir "$stem.log"
                                                    }

                                                    # Level 2 Hierarchy: Capture File Start Banner
                                                    $winLabel = if ($cap.Windows -eq 1) { "1 window" } else { "{0} windows" -f $cap.Windows }
                                                    $fileTag = Format-ColorText -Text ("[FILE {0} / {1}]" -f ($capIdx + 1), $captures.Count) -Code $AnsiCyan
                                                    $winTagMuted = Format-ColorText -Text ("({0})" -f $winLabel) -Code $AnsiGray
                                                    $capDisplayName = "{0}.jsonl" -f $cap.Label
                                                    Write-Host ("  📄 {0} {1} {2}" -f $fileTag, $capDisplayName, $winTagMuted)
                                                    try { [Console]::Out.Flush() } catch {}

                                                    $capStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

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
                                                        "--hedge-skew-mult", $hsStr,
                                                        "--cap-tier1-hedge", $capT1,
                                                        "--jsonl", $outJsonl,
                                                        "--max-cross-tick-loss-micro", $crossLoss,
                                                        "--max-entry-prob", $entryProb,
                                                        "--max-entry-price", $entryPrice
                                                    )

                                                    $prevCrossLossEnv = $env:MAX_CROSS_TICK_LOSS_MICRO
                                                    $prevEntryProbEnv = $env:MAX_ENTRY_PROB
                                                    $prevEntryPriceEnv = $env:MAX_ENTRY_PRICE
                                                    $prevLLThreshEnv = $env:LEAD_LAG_CANCEL_THRESHOLD
                                                    $prevLLWinEnv = $env:LEAD_LAG_WINDOW_MS
                                                    $prevLLCdEnv = $env:LEAD_LAG_COOLDOWN_MS
                                                    $prevLLDirEnv = $env:LEAD_LAG_DIRECTIONAL
                                                    $prevHedgeSkewEnv = $env:HEDGE_SKEW_MULT
                                                    $prevCapTier1Env = $env:CAP_TIER1_REDUCING_TO_DELTA
                                                    try {
                                                        if ($crossLoss -eq "off") {
                                                            $env:MAX_CROSS_TICK_LOSS_MICRO = "off"
                                                        } else {
                                                            $env:MAX_CROSS_TICK_LOSS_MICRO = $crossLoss
                                                        }
                                                        if ($entryProb -eq "off") {
                                                            $env:MAX_ENTRY_PROB = "off"
                                                        } else {
                                                            $env:MAX_ENTRY_PROB = $entryProb
                                                        }
                                                        if ($entryPrice -eq "off") {
                                                            $env:MAX_ENTRY_PRICE = "off"
                                                        } else {
                                                            $env:MAX_ENTRY_PRICE = $entryPrice
                                                        }
                                                        $env:LEAD_LAG_CANCEL_THRESHOLD = $llThresh
                                                        $env:LEAD_LAG_WINDOW_MS = $llWin.ToString($inv)
                                                        $env:LEAD_LAG_COOLDOWN_MS = $llCd.ToString($inv)
                                                        $env:LEAD_LAG_DIRECTIONAL = $llDir
                                                        $env:HEDGE_SKEW_MULT = $hsStr
                                                        $env:CAP_TIER1_REDUCING_TO_DELTA = $capT1
                                                        & $binPath @cliArgs *> $outLog
                                                        if ($LASTEXITCODE -ne 0) {
                                                            $tail = if (Test-Path -LiteralPath $outLog) { (Get-Content -LiteralPath $outLog -Tail 20) -join "`n" } else { "" }
                                                            throw "Sweep run failed (exit=$LASTEXITCODE) for $($cap.Stem)/$stem.`n$tail"
                                                        }
                                                    } finally {
                                                        if ($null -eq $prevCrossLossEnv) { Remove-Item Env:MAX_CROSS_TICK_LOSS_MICRO -ErrorAction SilentlyContinue } else { $env:MAX_CROSS_TICK_LOSS_MICRO = $prevCrossLossEnv }
                                                        if ($null -eq $prevEntryProbEnv) { Remove-Item Env:MAX_ENTRY_PROB -ErrorAction SilentlyContinue } else { $env:MAX_ENTRY_PROB = $prevEntryProbEnv }
                                                        if ($null -eq $prevEntryPriceEnv) { Remove-Item Env:MAX_ENTRY_PRICE -ErrorAction SilentlyContinue } else { $env:MAX_ENTRY_PRICE = $prevEntryPriceEnv }
                                                        if ($null -eq $prevLLThreshEnv) { Remove-Item Env:LEAD_LAG_CANCEL_THRESHOLD -ErrorAction SilentlyContinue } else { $env:LEAD_LAG_CANCEL_THRESHOLD = $prevLLThreshEnv }
                                                        if ($null -eq $prevLLWinEnv) { Remove-Item Env:LEAD_LAG_WINDOW_MS -ErrorAction SilentlyContinue } else { $env:LEAD_LAG_WINDOW_MS = $prevLLWinEnv }
                                                        if ($null -eq $prevLLCdEnv) { Remove-Item Env:LEAD_LAG_COOLDOWN_MS -ErrorAction SilentlyContinue } else { $env:LEAD_LAG_COOLDOWN_MS = $prevLLCdEnv }
                                                        if ($null -eq $prevLLDirEnv) { Remove-Item Env:LEAD_LAG_DIRECTIONAL -ErrorAction SilentlyContinue } else { $env:LEAD_LAG_DIRECTIONAL = $prevLLDirEnv }
                                                        if ($null -eq $prevHedgeSkewEnv) { Remove-Item Env:HEDGE_SKEW_MULT -ErrorAction SilentlyContinue } else { $env:HEDGE_SKEW_MULT = $prevHedgeSkewEnv }
                                                        if ($null -eq $prevCapTier1Env) { Remove-Item Env:CAP_TIER1_REDUCING_TO_DELTA -ErrorAction SilentlyContinue } else { $env:CAP_TIER1_REDUCING_TO_DELTA = $prevCapTier1Env }
                                                    }

                                                    $capStopwatch.Stop()
                                                    $capElapsedSec = $capStopwatch.Elapsed.TotalSeconds

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

                                                    # Level 2 Hierarchy: Per-Capture Color-Coded Summary Line
                                                    $treeBranch = Format-ColorText -Text "     └─" -Code $AnsiGray
                                                    $sepBar = Format-ColorText -Text " | " -Code $AnsiGray
                                                    $netPnlCol = Format-SignedPnlCell -Value $netPnl -Width 1 -Bold $false -Culture $inv
                                                    $netRebCol = Format-SignedPnlCell -Value $netPlusReb -Width 1 -Bold $false -Culture $inv
                                                    $alphaCol = Format-SignedPnlCell -Value $alphaEdge -Width 1 -Bold $false -Culture $inv
                                                    $rpmCol = if ($rpm -eq 0) { Format-ColorText -Text "0" -Code $AnsiGray } else { $rpm.ToString($inv) }
                                                    $mo1sCol = Format-SignedMarkoutCell -Value $mo1s -Width 1 -Bold $false -Culture $inv
                                                    $mo5sCol = Format-SignedMarkoutCell -Value $mo5s -Width 1 -Bold $false -Culture $inv

                                                    Write-Host ("{0} Done ({1:F2}s){2}Fills: {3}{2}Merges: {4}{2}Pairs: {5:F2}{2}NetPnL: {6}{2}Net+Reb: {7}{2}Alpha: {8}{2}RPM: {9}{2}MO1s: {10}{2}MO5s: {11}" -f `
                                                        $treeBranch, $capElapsedSec, $sepBar, $fills, $merges, $pairs, $netPnlCol, $netRebCol, $alphaCol, $rpmCol, $mo1sCol, $mo5sCol)

                                                    $capRuns.Add([PSCustomObject]@{
                                                        CaptureName   = $cap.Name
                                                        CaptureStem   = $cap.Stem
                                                        Windows       = $cap.Windows
                                                        Fills         = $fills
                                                        Merges        = $merges
                                                        PairsNum      = $pairs
                                                        RealizedNum   = $realized
                                                        UnrealNum     = $unreal
                                                        NetPnLNum     = $netPnl
                                                        RebateNum     = $rebate
                                                        NetPlusRebNum = $netPlusReb
                                                        BasePnLNum    = $basePnl
                                                        AlphaEdgeNum  = $alphaEdge
                                                        RPM           = $rpm
                                                        MO1sNum       = $mo1s
                                                        MO5sNum       = $mo5s
                                                    })
                                                }

                                                $cfgStopwatch.Stop()
                                                $cfgElapsedSec = $cfgStopwatch.Elapsed.TotalSeconds

                                                # Aggregate Config Set Totals
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
                                                $winRatePct = if ($captures.Count -gt 0) { ($winCaps / $captures.Count) * 100.0 } else { 0.0 }

                                                # Config Set Subtotal Banner
                                                $subHeader = Format-ColorText -Text ("  📊 [SET {0} / {1} SUBTOTAL | {2:F2}s]" -f $runIdx, $totalRuns, $cfgElapsedSec) -Code $AnsiBoldCyan
                                                $sepBar = Format-ColorText -Text " | " -Code $AnsiGray
                                                $subNetPnlCol = Format-SignedPnlCell -Value $sumNetPnl -Width 1 -Bold $true -Culture $inv
                                                $subNetRebCol = Format-SignedPnlCell -Value $sumNetPlusReb -Width 1 -Bold $true -Culture $inv
                                                $subAlphaCol = Format-SignedPnlCell -Value $sumAlphaEdge -Width 1 -Bold $true -Culture $inv
                                                $winCapRaw = "{0}/{1}" -f $winCaps, $captures.Count
                                                $winCapCode = if ($winRatePct -ge 70.0) { $AnsiBoldGreen } elseif ($winRatePct -ge 40.0) { $AnsiBoldYellow } else { $AnsiBoldRed }
                                                $winCapCol = Format-ColorText -Text $winCapRaw -Code $winCapCode

                                                Write-Host ""
                                                Write-Host $subHeader
                                                Write-Host ("     Fills: {0}{1}Merges: {2}{1}Pairs: {3:F2}{1}NetPnL: {4}{1}Net+Reb: {5}{1}Alpha: {6}{1}WinCap: {7}" -f `
                                                    $sumFills, $sepBar, $sumMerges, $sumPairs, $subNetPnlCol, $subNetRebCol, $subAlphaCol, $winCapCol)

                                                $results.Add([PSCustomObject]@{
                                                    Gamma         = $gammaStr
                                                    VarSpr        = $varStr
                                                    MinT          = $minTicks
                                                    XTick         = $crossLoss
                                                    MaxP          = $entryProb
                                                    MaxPx         = $entryPrice
                                                    LLTh          = $llThresh
                                                    LLWin         = $llWin
                                                    LLCd          = $llCd
                                                    LLDir         = $llDir
                                                    HSkew         = $hsStr
                                                    T1Cap         = $capT1
                                                    Fills         = $sumFills
                                                    Merges        = $sumMerges
                                                    PairsNum      = $sumPairs
                                                    Pairs         = $sumPairs.ToString("F2", $inv)
                                                    RealizedNum   = $sumRealized
                                                    Realized      = $sumRealized.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    UnrealNum     = $sumUnreal
                                                    Unreal        = $sumUnreal.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    NetPnLNum     = $sumNetPnl
                                                    NetPnL        = $sumNetPnl.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    RebateNum     = $sumRebate
                                                    Rebate        = $sumRebate.ToString("0.0000", $inv)
                                                    NetPlusRebNum = $sumNetPlusReb
                                                    NetPlusReb    = $sumNetPlusReb.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    BasePnLNum    = $sumBasePnl
                                                    BasePnL       = $sumBasePnl.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    AlphaEdgeNum  = $sumAlphaEdge
                                                    AlphaEdge     = $sumAlphaEdge.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    RPM           = $maxRpm
                                                    MO1sNum       = $avgMo1s
                                                    MO1s          = $avgMo1s.ToString("+0.0;-0.0;+0.0", $inv)
                                                    MO5sNum       = $avgMo5s
                                                    MO5s          = $avgMo5s.ToString("+0.0;-0.0;+0.0", $inv)
                                                    WinRatePct    = $winRatePct
                                                    WinRate       = ("{0:F0}%" -f $winRatePct)
                                                    WinCaps       = $winCaps
                                                    CapRuns       = $capRuns
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
        }
    }

    $sweepStopwatch.Stop()
    $totalElapsedSec = [Math]::Max(0.001, $sweepStopwatch.Elapsed.TotalSeconds)
    $avgConfigSec = $totalElapsedSec / [Math]::Max(1, $totalRuns)
    $totalReplayWindows = $totalRuns * [Math]::Max(1, $totalWindows)
    $throughputWinPerSec = $totalReplayWindows / $totalElapsedSec
    $throughputText = if ($totalWindows -gt 0) { "{0:F2} win/s" -f $throughputWinPerSec } else { "n/a (windows unknown)" }

    # -----------------------------------------------------------------------
    # Phase 3: Final Aggregation, Color-Coded Leaderboard & Per-Capture Summary
    # -----------------------------------------------------------------------
    $sorted = @($results | Sort-Object -Property NetPnLNum -Descending)

    # Build Leaderboard Column Metadata
    $cols = [System.Collections.Generic.List[hashtable]]::new()
    $cols.Add(@{ Name = "Rank";      Dash = "----";      Width = 4;  Prop = "Rank";      Kind = "rank" })
    $cols.Add(@{ Name = "Gamma";     Dash = "-----";     Width = -6; Prop = "Gamma";     Kind = "param" })
    $cols.Add(@{ Name = "VarSpr";    Dash = "------";    Width = -6; Prop = "VarSpr";    Kind = "param" })
    $cols.Add(@{ Name = "MinT";      Dash = "----";      Width = 4;  Prop = "MinT";      Kind = "param" })
    if ($showCrossLoss)  { $cols.Add(@{ Name = "XTick"; Dash = "-----"; Width = 7; Prop = "XTick"; Kind = "param" }) }
    if ($showEntryProb)  { $cols.Add(@{ Name = "MaxP";  Dash = "----";  Width = 5; Prop = "MaxP";  Kind = "param" }) }
    if ($showEntryPrice) { $cols.Add(@{ Name = "MaxPx"; Dash = "-----"; Width = 5; Prop = "MaxPx"; Kind = "param" }) }
    if ($showLLThresh)   { $cols.Add(@{ Name = "LLTh";  Dash = "-----"; Width = 5; Prop = "LLTh";  Kind = "param" }) }
    if ($showLLWin)      { $cols.Add(@{ Name = "LLWin"; Dash = "-----"; Width = 5; Prop = "LLWin"; Kind = "param" }) }
    if ($showLLCd)       { $cols.Add(@{ Name = "LLCd";  Dash = "----";  Width = 4; Prop = "LLCd";  Kind = "param" }) }
    if ($showLLDir)      { $cols.Add(@{ Name = "LLDir"; Dash = "-----"; Width = 5; Prop = "LLDir"; Kind = "param" }) }
    if ($showHedgeSkew)  { $cols.Add(@{ Name = "HSkew"; Dash = "-----"; Width = 5; Prop = "HSkew"; Kind = "param" }) }
    if ($showCapTier1)   { $cols.Add(@{ Name = "T1Cap"; Dash = "-----"; Width = 5; Prop = "T1Cap"; Kind = "param" }) }

    $cols.Add(@{ Name = "Fills";     Dash = "-----";     Width = 5;  Prop = "Fills";         NumProp = "Fills";         Kind = "int" })
    $cols.Add(@{ Name = "Merges";    Dash = "------";    Width = 6;  Prop = "Merges";        NumProp = "Merges";        Kind = "int" })
    $cols.Add(@{ Name = "Pairs";     Dash = "-------";   Width = 7;  Prop = "Pairs";         NumProp = "PairsNum";      Kind = "float" })
    $cols.Add(@{ Name = "Realized";  Dash = "--------";  Width = 9;  Prop = "Realized";      NumProp = "RealizedNum";   Kind = "pnl" })
    $cols.Add(@{ Name = "Unreal";    Dash = "--------";  Width = 9;  Prop = "Unreal";        NumProp = "UnrealNum";     Kind = "pnl" })
    $cols.Add(@{ Name = "NetPnL";    Dash = "--------";  Width = 9;  Prop = "NetPnL";        NumProp = "NetPnLNum";     Kind = "pnl_bold" })
    $cols.Add(@{ Name = "Rebate";    Dash = "------";    Width = 7;  Prop = "Rebate";        NumProp = "RebateNum";     Kind = "rebate" })
    if ($isMultiCapture) {
        $cols.Add(@{ Name = "Net+Reb";   Dash = "--------";  Width = 9;  Prop = "NetPlusReb";    NumProp = "NetPlusRebNum"; Kind = "pnl_bold" })
    }
    $cols.Add(@{ Name = "BasePnL";   Dash = "--------";  Width = 9;  Prop = "BasePnL";       NumProp = "BasePnLNum";    Kind = "pnl" })
    $cols.Add(@{ Name = "AlphaEdge"; Dash = "---------"; Width = 9;  Prop = "AlphaEdge";     NumProp = "AlphaEdgeNum";  Kind = "pnl" })
    $cols.Add(@{ Name = "RPM";       Dash = "---";       Width = 4;  Prop = "RPM";           NumProp = "RPM";           Kind = "rpm" })
    $cols.Add(@{ Name = "MO1s";      Dash = "-----";     Width = 6;  Prop = "MO1s";          NumProp = "MO1sNum";       Kind = "markout" })
    $cols.Add(@{ Name = "MO5s";      Dash = "-----";     Width = 6;  Prop = "MO5s";          NumProp = "MO5sNum";       Kind = "markout" })
    if ($isMultiCapture) {
        $cols.Add(@{ Name = "WinRate";   Dash = "-------";   Width = 7;  Prop = "WinRate";       NumProp = "WinRatePct";    Kind = "winrate" })
    }

    # Compute Leaderboard Totals / Averages across all config runs
    $lbTotFills = 0; $lbTotMerges = 0; $lbTotPairs = 0.0
    $lbTotRealized = 0.0; $lbTotUnreal = 0.0; $lbTotNetPnl = 0.0
    $lbTotRebate = 0.0; $lbTotNetPlusReb = 0.0; $lbTotBasePnl = 0.0; $lbTotAlpha = 0.0
    $lbSumRpm = 0.0; $lbSumMo1s = 0.0; $lbSumMo5s = 0.0; $lbSumWinRate = 0.0

    foreach ($r in $sorted) {
        $lbTotFills += $r.Fills
        $lbTotMerges += $r.Merges
        $lbTotPairs += $r.PairsNum
        $lbTotRealized += $r.RealizedNum
        $lbTotUnreal += $r.UnrealNum
        $lbTotNetPnl += $r.NetPnLNum
        $lbTotRebate += $r.RebateNum
        $lbTotNetPlusReb += $r.NetPlusRebNum
        $lbTotBasePnl += $r.BasePnLNum
        $lbTotAlpha += $r.AlphaEdgeNum
        $lbSumRpm += $r.RPM
        $lbSumMo1s += $r.MO1sNum
        $lbSumMo5s += $r.MO5sNum
        $lbSumWinRate += $r.WinRatePct
    }
    $nRuns = [Math]::Max(1, $sorted.Count)
    $lbAvgRpm = $lbSumRpm / $nRuns
    $lbAvgMo1s = $lbSumMo1s / $nRuns
    $lbAvgMo5s = $lbSumMo5s / $nRuns
    $lbAvgWinRate = $lbSumWinRate / $nRuns

    $fmtParts = for ($ci = 0; $ci -lt $cols.Count; $ci++) { "{$ci,$($cols[$ci].Width)}" }
    $plainRowFmt = $fmtParts -join " "
    $headers = @($cols | ForEach-Object { $_.Name })
    $dashes = @($cols | ForEach-Object { $_.Dash })

    $summaryPath = Join-Path $sweepDir "summary.txt"

    # Helper to format a single leaderboard row (plain or ANSI-colored)
    function Format-LeaderboardRow {
        param(
            [int]$RankNum,
            [PSCustomObject]$Row,
            [bool]$Colorize
        )
        $cells = [System.Collections.Generic.List[string]]::new()
        foreach ($col in $cols) {
            $w = [Math]::Abs([int]$col.Width)
            $leftAlign = ([int]$col.Width -lt 0)
            switch ($col.Kind) {
                "rank" {
                    $raw = $RankNum.ToString($inv).PadLeft($w)
                    if ($Colorize -and $RankNum -eq 1) {
                        $cells.Add((Format-ColorText -Text $raw -Code $AnsiBoldYellow))
                    } else {
                        $cells.Add($raw)
                    }
                }
                "param" {
                    $valStr = [string]$Row.($col.Prop)
                    $cells.Add($(if ($leftAlign) { $valStr.PadRight($w) } else { $valStr.PadLeft($w) }))
                }
                "int" {
                    $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                }
                "float" {
                    $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                }
                "pnl" {
                    if ($Colorize) {
                        $cells.Add((Format-SignedPnlCell -Value ([double]$Row.($col.NumProp)) -Width $w -Bold $false -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                "pnl_bold" {
                    if ($Colorize) {
                        $cells.Add((Format-SignedPnlCell -Value ([double]$Row.($col.NumProp)) -Width $w -Bold $true -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                "rebate" {
                    if ($Colorize) {
                        $cells.Add((Format-RebateCell -Value ([double]$Row.($col.NumProp)) -Width $w -Bold $false -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                "rpm" {
                    $rpmVal = [int]$Row.RPM
                    $raw = $rpmVal.ToString($inv).PadLeft($w)
                    if ($Colorize -and $rpmVal -eq 0) {
                        $cells.Add((Format-ColorText -Text $raw -Code $AnsiGray))
                    } else {
                        $cells.Add($raw)
                    }
                }
                "markout" {
                    if ($Colorize) {
                        $cells.Add((Format-SignedMarkoutCell -Value ([double]$Row.($col.NumProp)) -Width $w -Bold $false -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                "winrate" {
                    if ($Colorize) {
                        $cells.Add((Format-WinRateCell -Pct ([double]$Row.WinRatePct) -Width $w -Bold $false -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.WinRate).PadLeft($w))
                    }
                }
            }
        }
        return ($cells -join " ")
    }

    function Format-LeaderboardFooterRow {
        param([bool]$Colorize)
        $cells = [System.Collections.Generic.List[string]]::new()
        foreach ($col in $cols) {
            $w = [Math]::Abs([int]$col.Width)
            $leftAlign = ([int]$col.Width -lt 0)
            switch ($col.Kind) {
                "rank" {
                    $raw = "TOT".PadLeft($w)
                    $cells.Add($(if ($Colorize) { Format-ColorText -Text $raw -Code $AnsiBoldWhite } else { $raw }))
                }
                "param" {
                    $raw = if ($leftAlign) { "-".PadRight($w) } else { "-".PadLeft($w) }
                    $cells.Add($(if ($Colorize) { Format-ColorText -Text $raw -Code $AnsiGray } else { $raw }))
                }
                "int" {
                    $val = if ($col.Prop -eq "Fills") { $lbTotFills } else { $lbTotMerges }
                    $raw = $val.ToString($inv).PadLeft($w)
                    $cells.Add($(if ($Colorize) { Format-ColorText -Text $raw -Code $AnsiBoldWhite } else { $raw }))
                }
                "float" {
                    $raw = $lbTotPairs.ToString("F2", $inv).PadLeft($w)
                    $cells.Add($(if ($Colorize) { Format-ColorText -Text $raw -Code $AnsiBoldWhite } else { $raw }))
                }
                { $_ -in @("pnl", "pnl_bold") } {
                    $numVal = switch ($col.Prop) {
                        "Realized"   { $lbTotRealized }
                        "Unreal"     { $lbTotUnreal }
                        "NetPnL"     { $lbTotNetPnl }
                        "NetPlusReb" { $lbTotNetPlusReb }
                        "BasePnL"    { $lbTotBasePnl }
                        "AlphaEdge"  { $lbTotAlpha }
                        default      { 0.0 }
                    }
                    if ($Colorize) {
                        $cells.Add((Format-SignedPnlCell -Value $numVal -Width $w -Bold $true -Culture $inv))
                    } else {
                        $cells.Add($numVal.ToString("+0.0000;-0.0000;+0.0000", $inv).PadLeft($w))
                    }
                }
                "rebate" {
                    if ($Colorize) {
                        $cells.Add((Format-RebateCell -Value $lbTotRebate -Width $w -Bold $true -Culture $inv))
                    } else {
                        $cells.Add($lbTotRebate.ToString("0.0000", $inv).PadLeft($w))
                    }
                }
                "rpm" {
                    $raw = $lbAvgRpm.ToString("0.0", $inv).PadLeft($w)
                    $cells.Add($(if ($Colorize) { Format-ColorText -Text $raw -Code $AnsiBoldWhite } else { $raw }))
                }
                "markout" {
                    $mVal = if ($col.Prop -eq "MO1s") { $lbAvgMo1s } else { $lbAvgMo5s }
                    if ($Colorize) {
                        $cells.Add((Format-SignedMarkoutCell -Value $mVal -Width $w -Bold $true -Culture $inv))
                    } else {
                        $cells.Add($mVal.ToString("+0.0;-0.0;+0.0", $inv).PadLeft($w))
                    }
                }
                "winrate" {
                    if ($Colorize) {
                        $cells.Add((Format-WinRateCell -Pct $lbAvgWinRate -Width $w -Bold $true -Culture $inv))
                    } else {
                        $cells.Add(("{0:F0}%" -f $lbAvgWinRate).PadLeft($w))
                    }
                }
            }
        }
        return ($cells -join " ")
    }

    # -----------------------------------------------------------------------
    # Build Plain-Text Summary Lines (for summary.txt) & Color Console Output
    # -----------------------------------------------------------------------
    $plainSummary = [System.Collections.Generic.List[string]]::new()
    $plainSummary.Add("=" * 100)
    $plainSummary.Add(("SWEEP COMPLETE | Total Elapsed: {0:F2}s | Avg/Config: {1:F2}s | Throughput: {2}" -f $totalElapsedSec, $avgConfigSec, $throughputText))
    $plainSummary.Add(("Captures: {0} ({1}w total) | V_max=`${2} | Base={3}sh | MaxPair=`${4} | Decay={5}" -f $captures.Count, $totalWindows, $VMax, $BaseSize, $maxPairFormatted, $QueueDecay))
    if ($isMultiCapture) {
        $plainSummary.Add("Included Captures:")
        for ($ci = 0; $ci -lt $captures.Count; $ci++) {
            $c = $captures[$ci]
            $winTag = if ($c.Windows -gt 0) { "{0}w" -f $c.Windows } else { "?w" }
            $plainSummary.Add(("  [{0}] {1}.jsonl ({2})" -f ($ci + 1), $c.Label, $winTag))
        }
    } else {
        $plainSummary.Add(("Input: {0}" -f $captures[0].Path))
    }
    $plainSummary.Add(("Output Dir: {0}" -f $sweepDir))
    $plainSummary.Add("=" * 100)
    $plainSummary.Add("")
    $plainSummary.Add("CONFIG LEADERBOARD (sorted by NetPnL desc)")
    $plainSummary.Add(($plainRowFmt -f $headers))
    $plainSummary.Add(($plainRowFmt -f $dashes))
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        $plainSummary.Add((Format-LeaderboardRow -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $false))
    }
    $plainSummary.Add(($plainRowFmt -f $dashes))
    $plainSummary.Add((Format-LeaderboardFooterRow -Colorize $false))
    # Write the leaderboard before printing, so an interrupted console dump still leaves the ranked artifact.
    $plainSummary | Set-Content -LiteralPath $summaryPath -Encoding utf8

    # Print Completion Banner to Console
    Write-Host ""
    Write-Host $majorRule
    Write-Host (Format-ColorText -Text ("🏁 SWEEP COMPLETE  |  Total Elapsed: {0:F2}s  |  Avg/Config: {1:F2}s  |  Throughput: {2}" -f $totalElapsedSec, $avgConfigSec, $throughputText) -Code $AnsiBoldCyan)
    Write-Host ("Summary File : {0}" -f $summaryPath)
    Write-Host $majorRule

    # Print Config Leaderboard to Console
    Write-Host ""
    Write-Host (Format-ColorText -Text "🏆 CONFIG LEADERBOARD (sorted by NetPnL desc)" -Code $AnsiBoldCyan)
    Write-Host (Format-ColorText -Text ($plainRowFmt -f $headers) -Code $AnsiBoldWhite)
    Write-Host (Format-ColorText -Text ($plainRowFmt -f $dashes) -Code $AnsiGray)
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        Write-Host (Format-LeaderboardRow -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $true)
    }
    Write-Host (Format-ColorText -Text ($plainRowFmt -f $dashes) -Code $AnsiGray)
    Write-Host (Format-LeaderboardFooterRow -Colorize $true)

    # Cross-Config Per-Capture Summary Table (Multi-Capture Runs)
    if ($isMultiCapture) {
        $capStemMaxLen = ($captures | ForEach-Object { $_.Label.Length } | Measure-Object -Maximum).Maximum
        $capColWidth = [Math]::Max(28, $capStemMaxLen)

        $capHeaderLine = "{0} {1,4} {2,5} {3,6} {4,7} {5,9} {6,9} {7,9} {8,7} {9,9} {10,9} {11,9} {12,4} {13,6} {14,6}" -f `
            "Capture".PadRight($capColWidth), "Win", "Fills", "Merges", "Pairs", "Realized", "Unreal", "NetPnL", "Rebate", "Net+Reb", "BasePnL", "AlphaEdge", "RPM", "MO1s", "MO5s"
        $capDashLine = "{0} {1,4} {2,5} {3,6} {4,7} {5,9} {6,9} {7,9} {8,7} {9,9} {10,9} {11,9} {12,4} {13,6} {14,6}" -f `
            ("-" * $capColWidth), "----", "-----", "------", "-------", "---------", "---------", "---------", "-------", "---------", "---------", "---------", "----", "------", "------"

        $plainSummary.Add("")
        $plainSummary.Add(("PER-CAPTURE SUMMARY (aggregated across {0} config(s))" -f $totalRuns))
        $plainSummary.Add($capHeaderLine)
        $plainSummary.Add($capDashLine)

        Write-Host ""
        Write-Host (Format-ColorText -Text ("📁 PER-CAPTURE SUMMARY (aggregated across {0} config(s))" -f $totalRuns) -Code $AnsiBoldCyan)
        Write-Host (Format-ColorText -Text $capHeaderLine -Code $AnsiBoldWhite)
        Write-Host (Format-ColorText -Text $capDashLine -Code $AnsiGray)

        $grandFills = 0; $grandMerges = 0; $grandPairs = 0.0
        $grandRealized = 0.0; $grandUnreal = 0.0; $grandNetPnl = 0.0
        $grandRebate = 0.0; $grandNetPlusReb = 0.0; $grandBasePnl = 0.0; $grandAlpha = 0.0
        $grandSumRpm = 0.0; $grandWeightedMo1s = 0.0; $grandWeightedMo5s = 0.0

        foreach ($cap in $captures) {
            $cFills = 0; $cMerges = 0; $cPairs = 0.0
            $cRealized = 0.0; $cUnreal = 0.0; $cNetPnl = 0.0
            $cRebate = 0.0; $cNetPlusReb = 0.0; $cBasePnl = 0.0; $cAlpha = 0.0
            $cSumRpm = 0.0; $cWeightedMo1s = 0.0; $cWeightedMo5s = 0.0
            $cCount = 0

            foreach ($r in $sorted) {
                $matchRun = $r.CapRuns | Where-Object { $_.CaptureStem -eq $cap.Stem } | Select-Object -First 1
                if ($null -ne $matchRun) {
                    $cCount++
                    $cFills += $matchRun.Fills
                    $cMerges += $matchRun.Merges
                    $cPairs += $matchRun.PairsNum
                    $cRealized += $matchRun.RealizedNum
                    $cUnreal += $matchRun.UnrealNum
                    $cNetPnl += $matchRun.NetPnLNum
                    $cRebate += $matchRun.RebateNum
                    $cNetPlusReb += $matchRun.NetPlusRebNum
                    $cBasePnl += $matchRun.BasePnLNum
                    $cAlpha += $matchRun.AlphaEdgeNum
                    $cSumRpm += $matchRun.RPM
                    $cWeightedMo1s += ($matchRun.Fills * $matchRun.MO1sNum)
                    $cWeightedMo5s += ($matchRun.Fills * $matchRun.MO5sNum)
                }
            }

            $cAvgRpm = if ($cCount -gt 0) { $cSumRpm / $cCount } else { 0.0 }
            $cAvgMo1s = if ($cFills -gt 0) { $cWeightedMo1s / $cFills } else { 0.0 }
            $cAvgMo5s = if ($cFills -gt 0) { $cWeightedMo5s / $cFills } else { 0.0 }
            $wTag = if ($cap.Windows -gt 0) { "{0}w" -f $cap.Windows } else { "?w" }

            $grandFills += $cFills
            $grandMerges += $cMerges
            $grandPairs += $cPairs
            $grandRealized += $cRealized
            $grandUnreal += $cUnreal
            $grandNetPnl += $cNetPnl
            $grandRebate += $cRebate
            $grandNetPlusReb += $cNetPlusReb
            $grandBasePnl += $cBasePnl
            $grandAlpha += $cAlpha
            $grandSumRpm += $cAvgRpm
            $grandWeightedMo1s += ($cFills * $cAvgMo1s)
            $grandWeightedMo5s += ($cFills * $cAvgMo5s)

            $plainCapLine = "{0} {1,4} {2,5} {3,6} {4,7} {5,9} {6,9} {7,9} {8,7} {9,9} {10,9} {11,9} {12,4} {13,6} {14,6}" -f `
                $cap.Label.PadRight($capColWidth),
                $wTag,
                $cFills,
                $cMerges,
                $cPairs.ToString("F2", $inv),
                $cRealized.ToString("+0.0000;-0.0000;+0.0000", $inv),
                $cUnreal.ToString("+0.0000;-0.0000;+0.0000", $inv),
                $cNetPnl.ToString("+0.0000;-0.0000;+0.0000", $inv),
                $cRebate.ToString("0.0000", $inv),
                $cNetPlusReb.ToString("+0.0000;-0.0000;+0.0000", $inv),
                $cBasePnl.ToString("+0.0000;-0.0000;+0.0000", $inv),
                $cAlpha.ToString("+0.0000;-0.0000;+0.0000", $inv),
                $cAvgRpm.ToString("0.0", $inv),
                $cAvgMo1s.ToString("+0.0;-0.0;+0.0", $inv),
                $cAvgMo5s.ToString("+0.0;-0.0;+0.0", $inv)
            $plainSummary.Add($plainCapLine)

            $cRpmStr = $cAvgRpm.ToString("0.0", $inv).PadLeft(4)
            $cRpmCol = if ($cAvgRpm -le 1e-6) { Format-ColorText -Text $cRpmStr -Code $AnsiGray } else { $cRpmStr }

            $colorCapLine = "{0} {1} {2,5} {3,6} {4,7} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14}" -f `
                $cap.Label.PadRight($capColWidth),
                (Format-ColorText -Text $wTag.PadLeft(4) -Code $AnsiGray),
                $cFills,
                $cMerges,
                $cPairs.ToString("F2", $inv),
                (Format-SignedPnlCell -Value $cRealized -Width 9 -Bold $false -Culture $inv),
                (Format-SignedPnlCell -Value $cUnreal -Width 9 -Bold $false -Culture $inv),
                (Format-SignedPnlCell -Value $cNetPnl -Width 9 -Bold $true -Culture $inv),
                (Format-RebateCell -Value $cRebate -Width 7 -Bold $false -Culture $inv),
                (Format-SignedPnlCell -Value $cNetPlusReb -Width 9 -Bold $true -Culture $inv),
                (Format-SignedPnlCell -Value $cBasePnl -Width 9 -Bold $false -Culture $inv),
                (Format-SignedPnlCell -Value $cAlpha -Width 9 -Bold $false -Culture $inv),
                $cRpmCol,
                (Format-SignedMarkoutCell -Value $cAvgMo1s -Width 6 -Bold $false -Culture $inv),
                (Format-SignedMarkoutCell -Value $cAvgMo5s -Width 6 -Bold $false -Culture $inv)
            Write-Host $colorCapLine
        }

        $grandAvgRpm = if ($captures.Count -gt 0) { $grandSumRpm / $captures.Count } else { 0.0 }
        $grandAvgMo1s = if ($grandFills -gt 0) { $grandWeightedMo1s / $grandFills } else { 0.0 }
        $grandAvgMo5s = if ($grandFills -gt 0) { $grandWeightedMo5s / $grandFills } else { 0.0 }
        $totalLabel = ("TOTAL / AVG ({0} files | {1}w)" -f $captures.Count, $totalWindows).PadRight($capColWidth)
        $totalWinTag = ("{0}w" -f $totalWindows).PadLeft(4)

        $plainSummary.Add($capDashLine)
        $plainSummary.Add(("{0} {1} {2,5} {3,6} {4,7} {5,9} {6,9} {7,9} {8,7} {9,9} {10,9} {11,9} {12,4} {13,6} {14,6}" -f `
            $totalLabel,
            $totalWinTag,
            $grandFills,
            $grandMerges,
            $grandPairs.ToString("F2", $inv),
            $grandRealized.ToString("+0.0000;-0.0000;+0.0000", $inv),
            $grandUnreal.ToString("+0.0000;-0.0000;+0.0000", $inv),
            $grandNetPnl.ToString("+0.0000;-0.0000;+0.0000", $inv),
            $grandRebate.ToString("0.0000", $inv),
            $grandNetPlusReb.ToString("+0.0000;-0.0000;+0.0000", $inv),
            $grandBasePnl.ToString("+0.0000;-0.0000;+0.0000", $inv),
            $grandAlpha.ToString("+0.0000;-0.0000;+0.0000", $inv),
            $grandAvgRpm.ToString("0.0", $inv),
            $grandAvgMo1s.ToString("+0.0;-0.0;+0.0", $inv),
            $grandAvgMo5s.ToString("+0.0;-0.0;+0.0", $inv)))

        Write-Host (Format-ColorText -Text $capDashLine -Code $AnsiGray)
        Write-Host ("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14}" -f `
            (Format-ColorText -Text $totalLabel -Code $AnsiBoldWhite),
            (Format-ColorText -Text $totalWinTag -Code $AnsiBoldWhite),
            (Format-ColorText -Text $grandFills.ToString($inv).PadLeft(5) -Code $AnsiBoldWhite),
            (Format-ColorText -Text $grandMerges.ToString($inv).PadLeft(6) -Code $AnsiBoldWhite),
            (Format-ColorText -Text $grandPairs.ToString("F2", $inv).PadLeft(7) -Code $AnsiBoldWhite),
            (Format-SignedPnlCell -Value $grandRealized -Width 9 -Bold $true -Culture $inv),
            (Format-SignedPnlCell -Value $grandUnreal -Width 9 -Bold $true -Culture $inv),
            (Format-SignedPnlCell -Value $grandNetPnl -Width 9 -Bold $true -Culture $inv),
            (Format-RebateCell -Value $grandRebate -Width 7 -Bold $true -Culture $inv),
            (Format-SignedPnlCell -Value $grandNetPlusReb -Width 9 -Bold $true -Culture $inv),
            (Format-SignedPnlCell -Value $grandBasePnl -Width 9 -Bold $true -Culture $inv),
            (Format-SignedPnlCell -Value $grandAlpha -Width 9 -Bold $true -Culture $inv),
            (Format-ColorText -Text $grandAvgRpm.ToString("0.0", $inv).PadLeft(4) -Code $AnsiBoldWhite),
            (Format-SignedMarkoutCell -Value $grandAvgMo1s -Width 6 -Bold $true -Culture $inv),
            (Format-SignedMarkoutCell -Value $grandAvgMo5s -Width 6 -Bold $true -Culture $inv))
    }

    $plainSummary | Set-Content -LiteralPath $summaryPath -Encoding utf8
    Write-Host ""
}
finally {
    Pop-Location
}