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

function Get-OptionalPayloadDouble {
    param(
        [PSCustomObject]$Payload,
        [string[]]$CandidateKeys,
        [double]$Scale = 1e6,
        [double]$DefaultValue = [double]::NaN
    )
    if ($null -eq $Payload) { return $DefaultValue }
    $props = $Payload.PSObject.Properties
    foreach ($key in $CandidateKeys) {
        $match = $props[$key]
        if ($null -ne $match -and $null -ne $match.Value) {
            return ([double]$match.Value / $Scale)
        }
    }
    return $DefaultValue
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

function Format-SignedPnlWithPctCell {
    param(
        [double]$Value,
        [double]$Pct,
        [int]$Width = 20,
        [bool]$Bold = $false,
        [bool]$IncludeDollar = $true,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $sign = if ($Value -gt 1e-9) { "+" } elseif ($Value -lt -1e-9) { "-" } else { "+" }
    $absVal = [Math]::Abs($Value)
    $valStr = if ($IncludeDollar) {
        "{0}`${1}" -f $sign, $absVal.ToString("0.0000", $Culture)
    } else {
        "{0}{1}" -f $sign, $absVal.ToString("0.0000", $Culture)
    }
    $pctStr = if ([double]::IsNaN($Pct) -or [double]::IsInfinity($Pct)) {
        "n/a"
    } else {
        $Pct.ToString("+0.00%;-0.00%;+0.00%", $Culture)
    }
    $raw = ("{0} ({1})" -f $valStr, $pctStr).PadLeft($Width)
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

function Format-SignedPctCell {
    param(
        [double]$Ratio,
        [int]$Width = 8,
        [bool]$Bold = $false,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    if ([double]::IsNaN($Ratio) -or [double]::IsInfinity($Ratio)) {
        return (Format-ColorText -Text ('n/a'.PadLeft($Width)) -Code $AnsiGray)
    }
    $raw = $Ratio.ToString('+0.00%;-0.00%;+0.00%', $Culture).PadLeft($Width)
    if ($Ratio -gt 1e-9) {
        $code = if ($Bold) { $AnsiBoldGreen } else { $AnsiGreen }
        return (Format-ColorText -Text $raw -Code $code)
    } elseif ($Ratio -lt -1e-9) {
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
        [bool]$IncludeDollar = $false,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $raw = if ($IncludeDollar) {
        $sign = if ($Value -ge 0.0) { "+" } else { "-" }
        ("{0}`${1}" -f $sign, [Math]::Abs($Value).ToString("0.0000", $Culture)).PadLeft($Width)
    } else {
        $Value.ToString("0.0000", $Culture).PadLeft($Width)
    }
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

function Format-FeeCell {
    param(
        [double]$FeePaidPositive,
        [int]$Width = 9,
        [bool]$Bold = $false,
        [bool]$IncludeDollar = $true,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $absFee = [Math]::Abs($FeePaidPositive)
    $numStr = $absFee.ToString("0.0000", $Culture)
    $raw = if ($IncludeDollar) {
        ("-`$" + $numStr).PadLeft($Width)
    } else {
        ("-" + $numStr).PadLeft($Width)
    }
    if ($absFee -le 1e-9) {
        return (Format-ColorText -Text $raw -Code $AnsiGray)
    }
    $code = if ($Bold) { $AnsiBoldRed } else { $AnsiRed }
    return (Format-ColorText -Text $raw -Code $code)
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
        [bool]$IsLeftColumn,
        [bool]$IsHighLevel = $false
    )
    $isChanged = (-not [string]::IsNullOrEmpty($PrevVal)) -and ($CurrVal -ne $PrevVal)
    $paddedLabel = $Label.PadRight(11)
    if ($IsHighLevel) {
        $paddedLabel = Format-ColorText -Text $paddedLabel -Code $AnsiGray
    }

    if ($IsLeftColumn) {
        if ($isChanged) {
            $tag = "{0}  ⚡ (was: {1})" -f $CurrVal, $PrevVal
            $padTo = if ($tag.Contains("⚡")) { 23 } else { 24 }
            $paddedVal = $tag.PadRight($padTo)
            $coloredVal = Format-ColorText -Text $paddedVal -Code $AnsiBoldYellow
            return ("{0}: {1}" -f $paddedLabel, $coloredVal)
        } else {
            $valCell = $CurrVal.PadRight(24)
            if ($IsHighLevel) { $valCell = Format-ColorText -Text $valCell -Code $AnsiBoldWhite }
            return ("{0}: {1}" -f $paddedLabel, $valCell)
        }
    } else {
        if ($isChanged) {
            $tag = "{0}  ⚡ (was: {1})" -f $CurrVal, $PrevVal
            $coloredVal = Format-ColorText -Text $tag -Code $AnsiBoldYellow
            return ("{0}: {1}" -f $paddedLabel, $coloredVal)
        } else {
            $valCell = if ($IsHighLevel) { Format-ColorText -Text $CurrVal -Code $AnsiBoldWhite } else { $CurrVal }
            return ("{0}: {1}" -f $paddedLabel, $valCell)
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
    $subRule = Format-ColorText -Text ("  · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · ·") -Code $AnsiGray
    $botRule = Format-ColorText -Text ("-" * 100) -Code $AnsiGray
    $pipe = Format-ColorText -Text " |  " -Code $AnsiGray

    $highLevelPairs = @(
        @("V-Max",    "BaseSize"),
        @("MaxPair",  "QueueDecay")
    )

    $sweptPairs = @(
        @("Gamma",     "VarSpread"),
        @("MinTicks",  "XTick"),
        @("MaxP",      "MaxPx"),
        @("LL-Thresh", "LL-WinMs"),
        @("LL-CdMs",   "LL-Dir"),
        @("HedgeSkew", "CapTier1")
    )

    Write-Host ""
    Write-Host $topRule
    Write-Host (Format-ColorText -Text "  [High-Level / Session Bounds]" -Code $AnsiGray)
    foreach ($pair in $highLevelPairs) {
        $kLeft = $pair[0]
        $kRight = $pair[1]
        $leftStr = Format-ParamCell -Label $kLeft -CurrVal ([string]$Curr[$kLeft]) -PrevVal "" -IsLeftColumn $true -IsHighLevel $true
        $rightStr = Format-ParamCell -Label $kRight -CurrVal ([string]$Curr[$kRight]) -PrevVal "" -IsLeftColumn $false -IsHighLevel $true
        Write-Host ("  {0}{1}{2}" -f $leftStr, $pipe, $rightStr)
    }
    Write-Host $subRule
    Write-Host (Format-ColorText -Text "  [Swept / Strategy Parameters]" -Code $AnsiCyan)
    foreach ($pair in $sweptPairs) {
        $kLeft = $pair[0]
        $kRight = $pair[1]
        $prevL = if ($null -ne $Prev) { [string]$Prev[$kLeft] } else { "" }
        $prevR = if ($null -ne $Prev) { [string]$Prev[$kRight] } else { "" }
        $leftStr = Format-ParamCell -Label $kLeft -CurrVal ([string]$Curr[$kLeft]) -PrevVal $prevL -IsLeftColumn $true -IsHighLevel $false
        $rightStr = Format-ParamCell -Label $kRight -CurrVal ([string]$Curr[$kRight]) -PrevVal $prevR -IsLeftColumn $false -IsHighLevel $false
        Write-Host ("  {0}{1}{2}" -f $leftStr, $pipe, $rightStr)
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
    Write-Host ("Captures : {0} file(s) ({1} windows total)" -f $captures.Count, $totalWindows)
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
                                                    "V-Max"      = "`$$VMax"
                                                    "BaseSize"   = "${BaseSize}sh"
                                                    "MaxPair"    = "`$$maxPairFormatted"
                                                    "QueueDecay" = $QueueDecay
                                                    "Gamma"      = $gammaStr
                                                    "VarSpread"  = $varStr
                                                    "MinTicks"   = $minTicks.ToString($inv)
                                                    "XTick"      = $crossLoss
                                                    "MaxP"       = $entryProb
                                                    "MaxPx"      = $entryPrice
                                                    "LL-Thresh"  = $llThresh
                                                    "LL-WinMs"   = $llWin.ToString($inv)
                                                    "LL-CdMs"    = $llCd.ToString($inv)
                                                    "LL-Dir"     = $llDir
                                                    "HedgeSkew"  = $hsStr
                                                    "CapTier1"   = $capT1
                                                }

                                                # Level 1 Hierarchy: Config Set Banner (High-Level + Swept Params with Diff Highlighting)
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

                                                    $allLines = @(Get-Content -LiteralPath $outJsonl)
                                                    if ($allLines.Count -eq 0) {
                                                        throw "No events emitted in $outJsonl"
                                                    }
                                                    $summaryLine = $allLines[$allLines.Count - 1]
                                                    $summaryObj = $summaryLine | ConvertFrom-Json
                                                    if ($summaryObj.event -ne "summary") {
                                                        throw "Final JSONL row is not a summary event in $outJsonl (found '$($summaryObj.event)')"
                                                    }

                                                    $payload = $summaryObj.payload
                                                    $realized = [double]$payload.realized_pnl_micro / 1e6
                                                    $unreal = [double]$payload.unrealized_pnl_micro / 1e6
                                                    $grossPnl = $realized + $unreal
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

                                                    # Quantity (UP+DN shares bought)
                                                    $qtyBought = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @(
                                                        "total_shares_bought_micro",
                                                        "shares_bought_micro",
                                                        "total_bought_micro",
                                                        "bought_shares_micro"
                                                    ) -Scale 1e6
                                                    if ([double]::IsNaN($qtyBought)) {
                                                        $upBought = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("up_shares_bought_micro", "yes_shares_bought_micro") -Scale 1e6 -DefaultValue 0.0
                                                        $dnBought = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("dn_shares_bought_micro", "down_shares_bought_micro", "no_shares_bought_micro") -Scale 1e6 -DefaultValue 0.0
                                                        if (($upBought + $dnBought) -gt 0.0) {
                                                            $qtyBought = $upBought + $dnBought
                                                        } else {
                                                            $qtyBought = [double]$fills * [double]$BaseSize
                                                        }
                                                    }

                                                    # Fees paid (stored as positive magnitude, displayed as negative)
                                                    $feesPaid = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @(
                                                        "fees_paid_micro",
                                                        "total_fees_micro",
                                                        "estimated_fees_micro",
                                                        "taker_fees_micro"
                                                    ) -Scale 1e6
                                                    if ([double]::IsNaN($feesPaid)) {
                                                        $derivedFees = ($grossPnl + $rebate) - $netPlusReb
                                                        $feesPaid = if ($derivedFees -gt 1e-6) { $derivedFees } else { 0.0 }
                                                    }

                                                    # Volume-In ($ committed) & Close Volume ($ paid back from trading/merging/settlements)
                                                    $volumeIn = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @(
                                                        "volume_in_micro",
                                                        "total_cost_micro",
                                                        "committed_volume_micro",
                                                        "buy_notional_micro",
                                                        "total_spent_micro"
                                                    ) -Scale 1e6
                                                    $closeVolume = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @(
                                                        "close_volume_micro",
                                                        "total_proceeds_micro",
                                                        "exit_volume_micro",
                                                        "sell_and_merge_notional_micro",
                                                        "returned_volume_micro"
                                                    ) -Scale 1e6
                                                    if ([double]::IsNaN($volumeIn)) {
                                                        $avgEntryPx = [Math]::Min(0.50, ($MaxCombinedBidMicro / 2e6))
                                                        $volumeIn = [Math]::Max(0.0, $qtyBought * $avgEntryPx)
                                                    }
                                                    if ([double]::IsNaN($closeVolume)) {
                                                        $closeVolume = [Math]::Max(0.0, $volumeIn + $grossPnl)
                                                    }

                                                    $grossPct = if ($volumeIn -gt 1e-6) { ($closeVolume / $volumeIn) - 1.0 } else { [double]::NaN }
                                                    $finalNetUsd = $netPlusReb
                                                    $netPct = if ($volumeIn -gt 1e-6) { $finalNetUsd / $volumeIn } else { [double]::NaN }

                                                    # % Win across windows in this capture
                                                    $winPctCap = [double]::NaN
                                                    $winWindowsCount = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("winning_windows", "positive_windows") -Scale 1.0
                                                    $totWindowsPayload = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("windows_count", "total_windows") -Scale 1.0 -DefaultValue ([double]$cap.Windows)
                                                    if (-not [double]::IsNaN($winWindowsCount) -and $totWindowsPayload -gt 0) {
                                                        $winPctCap = ($winWindowsCount / $totWindowsPayload) * 100.0
                                                    } else {
                                                        $winEventPnls = [System.Collections.Generic.List[double]]::new()
                                                        for ($li = 0; $li -lt ($allLines.Count - 1); $li++) {
                                                            $evLine = $allLines[$li]
                                                            if ($evLine -match '"event"\s*:\s*"(window_end|window_summary|window_settled)"') {
                                                                try {
                                                                    $evObj = $evLine | ConvertFrom-Json
                                                                    $evP = $evObj.payload
                                                                    $wNet = Get-OptionalPayloadDouble -Payload $evP -CandidateKeys @("net_pnl_micro", "window_net_pnl_micro", "realized_pnl_micro") -Scale 1e6 -DefaultValue 0.0
                                                                    $wReb = Get-OptionalPayloadDouble -Payload $evP -CandidateKeys @("estimated_rebate_micro", "rebate_micro") -Scale 1e6 -DefaultValue 0.0
                                                                    $winEventPnls.Add($wNet + $wReb)
                                                                } catch {}
                                                            }
                                                        }
                                                        if ($winEventPnls.Count -gt 0) {
                                                            $posWins = @($winEventPnls | Where-Object { $_ -gt 0.0 }).Count
                                                            $winPctCap = ($posWins / $winEventPnls.Count) * 100.0
                                                        } else {
                                                            $winPctCap = if ($finalNetUsd -gt 0.0) { 100.0 } else { 0.0 }
                                                        }
                                                    }

                                                    # Level 2 Hierarchy: Vertically Aligned & Color-Coded Per-Capture Summary (2 Lines)
                                                    # Line 1: Activity & Volume (Shares, Pairs, WinRate, Volume-In, Close Volume)
                                                    # Line 2: P&L Waterfall (Gross P&L -> Rebates -> Fees -> Net P&L)
                                                    $treeBranch1  = Format-ColorText -Text "     ├─" -Code $AnsiGray
                                                    $treeBranch2  = Format-ColorText -Text "     └─" -Code $AnsiGray
                                                    $sepBar       = Format-ColorText -Text " | " -Code $AnsiGray
                                                    $arrowSep     = Format-ColorText -Text "  ->  " -Code $AnsiGray

                                                    $doneCell     = ("Done ({0,5:F2}s)" -f $capElapsedSec).PadRight(13)
                                                    $qtyCell      = ("Quantity: {0,7:F2}" -f $qtyBought)
                                                    $pairsCell    = ("Pairs: {0,7:F2}" -f $pairs)
                                                    $winRateCell  = "% Win: " + (Format-WinRateCell -Pct $winPctCap -Width 4 -Bold $false -Culture $inv)
                                                    $volInCell    = "Volume-In: " + ("`${0:F2}" -f $volumeIn).PadLeft(9)
                                                    $closeVolCell = "Close Volume: " + ("`${0:F2}" -f $closeVolume).PadLeft(9)

                                                    $pnlLabelCell = (Format-ColorText -Text "P&L Breakdown" -Code $AnsiGray).PadRight(13)
                                                    $grossCell    = "Gross P&L: " + (Format-SignedPnlWithPctCell -Value $grossPnl -Pct $grossPct -Width 20 -Bold $false -IncludeDollar $true -Culture $inv)
                                                    $rebateCell   = "Rebates: " + (Format-RebateCell -Value $rebate -Width 8 -Bold $false -IncludeDollar $true -Culture $inv)
                                                    $feesCell     = "Fees: " + (Format-FeeCell -FeePaidPositive $feesPaid -Width 9 -Bold $false -IncludeDollar $true -Culture $inv)
                                                    $netCell      = "Net P&L: " + (Format-SignedPnlWithPctCell -Value $finalNetUsd -Pct $netPct -Width 20 -Bold $true -IncludeDollar $true -Culture $inv)

                                                    # Line 1: Activity & Volume
                                                    Write-Host ("{0} {1}{2}{3}{2}{4}{2}{5}{2}{6}{2}{7}" -f `
                                                        $treeBranch1, $doneCell, $sepBar, $qtyCell, $pairsCell, $winRateCell, $volInCell, $closeVolCell)

                                                    # Line 2: P&L Accounting Waterfall
                                                    Write-Host ("{0} {1}{2}{3}{2}{4}{2}{5}{6}{7}" -f `
                                                        $treeBranch2, $pnlLabelCell, $sepBar, $grossCell, $rebateCell, $feesCell, $arrowSep, $netCell)

                                                    $capRuns.Add([PSCustomObject]@{
                                                        CaptureName    = $cap.Name
                                                        CaptureStem    = $cap.Stem
                                                        Windows        = $cap.Windows
                                                        Fills          = $fills
                                                        Merges         = $merges
                                                        QuantityNum    = $qtyBought
                                                        PairsNum       = $pairs
                                                        WinPctNum      = $winPctCap
                                                        VolumeInNum    = $volumeIn
                                                        CloseVolumeNum = $closeVolume
                                                        RealizedNum    = $realized
                                                        UnrealNum      = $unreal
                                                        GrossPnlNum    = $grossPnl
                                                        NetPnLNum      = $netPnl
                                                        RebateNum      = $rebate
                                                        FeesNum        = $feesPaid
                                                        NetPlusRebNum  = $netPlusReb
                                                        BasePnLNum     = $basePnl
                                                        AlphaEdgeNum   = $alphaEdge
                                                        RPM            = $rpm
                                                        MO1sNum        = $mo1s
                                                        MO5sNum        = $mo5s
                                                    })
                                                }

                                                $cfgStopwatch.Stop()
                                                $cfgElapsedSec = $cfgStopwatch.Elapsed.TotalSeconds

                                                # Aggregate Config Set Totals
                                                $sumFills = 0
                                                $sumMerges = 0
                                                $sumQty = 0.0
                                                $sumPairs = 0.0
                                                $sumVolIn = 0.0
                                                $sumCloseVol = 0.0
                                                $sumRealized = 0.0
                                                $sumUnreal = 0.0
                                                $sumGrossPnl = 0.0
                                                $sumNetPnl = 0.0
                                                $sumRebate = 0.0
                                                $sumFeesPaid = 0.0
                                                $sumBasePnl = 0.0
                                                $sumAlphaEdge = 0.0
                                                $maxRpm = 0
                                                $weightedMo1s = 0.0
                                                $weightedMo5s = 0.0
                                                $winCaps = 0

                                                foreach ($c in $capRuns) {
                                                    $sumFills += $c.Fills
                                                    $sumMerges += $c.Merges
                                                    $sumQty += $c.QuantityNum
                                                    $sumPairs += $c.PairsNum
                                                    $sumVolIn += $c.VolumeInNum
                                                    $sumCloseVol += $c.CloseVolumeNum
                                                    $sumRealized += $c.RealizedNum
                                                    $sumUnreal += $c.UnrealNum
                                                    $sumGrossPnl += $c.GrossPnlNum
                                                    $sumNetPnl += $c.NetPnLNum
                                                    $sumRebate += $c.RebateNum
                                                    $sumFeesPaid += $c.FeesNum
                                                    $sumBasePnl += $c.BasePnLNum
                                                    $sumAlphaEdge += $c.AlphaEdgeNum
                                                    if ($c.RPM -gt $maxRpm) { $maxRpm = $c.RPM }
                                                    $weightedMo1s += ($c.Fills * $c.MO1sNum)
                                                    $weightedMo5s += ($c.Fills * $c.MO5sNum)
                                                    if ($c.NetPlusRebNum -gt 0.0) { $winCaps++ }
                                                }

                                                $sumNetPlusReb = $sumNetPnl + $sumRebate
                                                $sumGrossPct = if ($sumVolIn -gt 1e-6) { ($sumCloseVol / $sumVolIn) - 1.0 } else { [double]::NaN }
                                                $sumNetPct = if ($sumVolIn -gt 1e-6) { $sumNetPlusReb / $sumVolIn } else { [double]::NaN }
                                                $sumRoi = if ($sumVolIn -gt 1e-6) { ($sumCloseVol - $sumVolIn - $sumFeesPaid) / $sumVolIn } else { [double]::NaN }
                                                $roiPlain = if ([double]::IsNaN($sumRoi)) { 'n/a' } else { $sumRoi.ToString('+0.00%;-0.00%;+0.00%', $inv) }
                                                $avgMo1s = if ($sumFills -gt 0) { $weightedMo1s / $sumFills } else { 0.0 }
                                                $avgMo5s = if ($sumFills -gt 0) { $weightedMo5s / $sumFills } else { 0.0 }
                                                $winRatePct = if ($captures.Count -gt 0) { ($winCaps / $captures.Count) * 100.0 } else { 0.0 }

                                                # Config Set Subtotal Banner (Matches Per-Capture 2-Line Hierarchy & Style)
                                                $subHeader = Format-ColorText -Text ("  📊 [SET {0} / {1} SUBTOTAL | {2:F2}s]" -f $runIdx, $totalRuns, $cfgElapsedSec) -Code $AnsiBoldCyan
                                                $sepBar    = Format-ColorText -Text " | " -Code $AnsiGray
                                                $arrowSep  = Format-ColorText -Text "  ->  " -Code $AnsiGray

                                                $subDoneCell     = Format-ColorText -Text (("Done ({0,5:F2}s)" -f $cfgElapsedSec).PadRight(13)) -Code $AnsiBoldCyan
                                                $subQtyCell      = ("Quantity: {0,7:F2}" -f $sumQty)
                                                $subPairsCell    = ("Pairs: {0,7:F2}" -f $sumPairs)
                                                $subWinRateCell  = "% Win: " + (Format-WinRateCell -Pct $winRatePct -Width 4 -Bold $true -Culture $inv)
                                                $subVolInCell    = "Deployed: " + ("`${0:F2}" -f $sumVolIn).PadLeft(9)
                                                $subCloseVolCell = "Returned: " + ("`${0:F2}" -f $sumCloseVol).PadLeft(9)

                                                $subPnlLabelCell = Format-ColorText -Text ("P&L Subtotal".PadRight(13)) -Code $AnsiBoldCyan
                                                $subGrossCell    = "Gross P&L: " + (Format-SignedPnlWithPctCell -Value $sumGrossPnl -Pct $sumGrossPct -Width 20 -Bold $true -IncludeDollar $true -Culture $inv)
                                                $subRebateCell   = "Rebates: " + (Format-RebateCell -Value $sumRebate -Width 8 -Bold $true -IncludeDollar $true -Culture $inv)
                                                $subFeesCell     = "Fees: " + (Format-FeeCell -FeePaidPositive $sumFeesPaid -Width 9 -Bold $true -IncludeDollar $true -Culture $inv)
                                                $subNetCell      = "Net P&L: " + (Format-SignedPnlWithPctCell -Value $sumNetPlusReb -Pct $sumNetPct -Width 20 -Bold $true -IncludeDollar $true -Culture $inv)

                                                Write-Host ""
                                                Write-Host $subHeader
                                                # Line 1: Activity & Volume
                                                Write-Host ("{0} {1}{2}{3}{2}{4}{2}{5}{2}{6}{2}{7}" -f `
                                                    $treeBranch1, $subDoneCell, $sepBar, $subQtyCell, $subPairsCell, $subWinRateCell, $subVolInCell, $subCloseVolCell)
                                                # Line 2: P&L Accounting Waterfall
                                                Write-Host ("{0} {1}{2}{3}{2}{4}{2}{5}{6}{7}" -f `
                                                    $treeBranch2, $subPnlLabelCell, $sepBar, $subGrossCell, $subRebateCell, $subFeesCell, $arrowSep, $subNetCell)

                                                $grossSign = if ($sumGrossPnl -gt 1e-9) { "+" } elseif ($sumGrossPnl -lt -1e-9) { "-" } else { "+" }
                                                $grossPctStr = if ([double]::IsNaN($sumGrossPct) -or [double]::IsInfinity($sumGrossPct)) { "n/a" } else { $sumGrossPct.ToString("+0.00%;-0.00%;+0.00%", $inv) }
                                                $grossPlain = ("{0}`${1} ({2})" -f $grossSign, [Math]::Abs($sumGrossPnl).ToString("0.0000", $inv), $grossPctStr)
                                                $netSign = if ($sumNetPlusReb -gt 1e-9) { "+" } elseif ($sumNetPlusReb -lt -1e-9) { "-" } else { "+" }
                                                $netPctStr = if ([double]::IsNaN($sumNetPct) -or [double]::IsInfinity($sumNetPct)) { "n/a" } else { $sumNetPct.ToString("+0.00%;-0.00%;+0.00%", $inv) }
                                                $netPlusRebPlain = ("{0}`${1} ({2})" -f $netSign, [Math]::Abs($sumNetPlusReb).ToString("0.0000", $inv), $netPctStr)
                                                $rebateSign = if ($sumRebate -ge 0.0) { "+" } else { "-" }
                                                $rebatePlain = ("{0}`${1}" -f $rebateSign, [Math]::Abs($sumRebate).ToString("0.0000", $inv))
                                                $feesPlain = ("-`$" + [Math]::Abs($sumFeesPaid).ToString("0.0000", $inv))
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
                                                    QuantityNum   = $sumQty
                                                    Quantity      = $sumQty.ToString("F2", $inv)
                                                    PairsNum      = $sumPairs
                                                    Pairs         = $sumPairs.ToString("F2", $inv)
                                                    VolumeInNum   = $sumVolIn
                                                    VolumeIn      = ("`$" + $sumVolIn.ToString("F2", $inv))
                                                    CloseVolumeNum = $sumCloseVol
                                                    CloseVol      = ("`$" + $sumCloseVol.ToString("F2", $inv))
                                                    RoiNum        = $sumRoi
                                                    Roi           = $roiPlain
                                                    RealizedNum   = $sumRealized
                                                    Realized      = $sumRealized.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    UnrealNum     = $sumUnreal
                                                    Unreal        = $sumUnreal.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    GrossPnlNum   = $sumGrossPnl
                                                    GrossPnl      = $grossPlain
                                                    GrossPctNum   = $sumGrossPct
                                                    NetPnLNum     = $sumNetPnl
                                                    NetPnL        = $sumNetPnl.ToString("+0.0000;-0.0000;+0.0000", $inv)
                                                    RebateNum     = $sumRebate
                                                    Rebate        = $rebatePlain
                                                    FeesNum       = $sumFeesPaid
                                                    Fees          = $feesPlain
                                                    NetPlusRebNum = $sumNetPlusReb
                                                    NetPlusReb    = $netPlusRebPlain
                                                    NetPctNum     = $sumNetPct
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
    # Phase 3: Final Aggregation, Color-Coded Leaderboard (static column widths)
    # -----------------------------------------------------------------------
    $sorted = @($results | Sort-Object -Property NetPlusRebNum -Descending)

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

    $cols.Add(@{ Name = "Fills";     Dash = "-----";                  Width = 5;  Prop = "Fills";      NumProp = "Fills";          Kind = "int" })
    $cols.Add(@{ Name = "Merges";    Dash = "------";                 Width = 6;  Prop = "Merges";     NumProp = "Merges";         Kind = "int" })
    $cols.Add(@{ Name = "Quantity";  Dash = "--------";               Width = 8;  Prop = "Quantity";   NumProp = "QuantityNum";    Kind = "qty" })
    $cols.Add(@{ Name = "Pairs";     Dash = "-------";                Width = 7;  Prop = "Pairs";      NumProp = "PairsNum";       Kind = "float" })
    $cols.Add(@{ Name = "Win Rate";  Dash = "--------";               Width = 8;  Prop = "WinRate";    NumProp = "WinRatePct";     Kind = "winrate" })
    $cols.Add(@{ Name = "Deployed";  Dash = "---------";              Width = 9;  Prop = "VolumeIn";   NumProp = "VolumeInNum";    Kind = "money" })
    $cols.Add(@{ Name = "ROI";       Dash = "--------";               Width = 8;  Prop = "Roi";        NumProp = "RoiNum";         Kind = "roi" })
    $cols.Add(@{ Name = "Returned";  Dash = "---------";              Width = 9;  Prop = "CloseVol";   NumProp = "CloseVolumeNum"; Kind = "money" })
    $cols.Add(@{ Name = "Gross P&L"; Dash = "--------------------";  Width = 20; Prop = "GrossPnl";   NumProp = "GrossPnlNum";    PctProp = "GrossPctNum"; Kind = "pnl_pct" })
    $cols.Add(@{ Name = "Rebates";   Dash = "---------";              Width = 9;  Prop = "Rebate";     NumProp = "RebateNum";      Kind = "rebate_dollar" })
    $cols.Add(@{ Name = "Fees";      Dash = "---------";              Width = 9;  Prop = "Fees";       NumProp = "FeesNum";        Kind = "fee" })
    $cols.Add(@{ Name = "Net P&L";   Dash = "--------------------";  Width = 20; Prop = "NetPlusReb"; NumProp = "NetPlusRebNum";  PctProp = "NetPctNum";   Kind = "pnl_pct_bold" })
    $cols.Add(@{ Name = "Base P&L";  Dash = "--------";               Width = 9;  Prop = "BasePnL";    NumProp = "BasePnLNum";     Kind = "pnl" })
    $cols.Add(@{ Name = "Alpha Edge"; Dash = "----------";             Width = 10; Prop = "AlphaEdge";  NumProp = "AlphaEdgeNum";   Kind = "pnl" })
    $cols.Add(@{ Name = "RPM";       Dash = "---";                    Width = 4;  Prop = "RPM";        NumProp = "RPM";            Kind = "rpm" })
    $cols.Add(@{ Name = "MO1s";      Dash = "-----";                  Width = 6;  Prop = "MO1s";       NumProp = "MO1sNum";        Kind = "markout" })
    $cols.Add(@{ Name = "MO5s";      Dash = "-----";                  Width = 6;  Prop = "MO5s";       NumProp = "MO5sNum";        Kind = "markout" })

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
                    $medal = switch ($RankNum) {
                        1 { "🥇" }
                        2 { "🥈" }
                        3 { "🥉" }
                        default { $RankNum.ToString($inv) }
                    }
                    $raw = $medal.PadLeft($w)
                    if ($Colorize) {
                        if ($RankNum -eq 1) {
                            $cells.Add((Format-ColorText -Text $raw -Code $AnsiBoldYellow))
                        } elseif ($RankNum -eq 2) {
                            $cells.Add((Format-ColorText -Text $raw -Code $AnsiBoldWhite))
                        } elseif ($RankNum -eq 3) {
                            $cells.Add((Format-ColorText -Text $raw -Code $AnsiYellow))
                        } else {
                            $cells.Add($raw)
                        }
                    } else {
                        $cells.Add($raw)
                    }
                }
                { $_ -in @("param", "int", "float", "qty", "money") } {
                    $valStr = [string]$Row.($col.Prop)
                    $cells.Add($(if ($leftAlign) { $valStr.PadRight($w) } else { $valStr.PadLeft($w) }))
                }
                { $_ -in @("pnl", "pnl_bold") } {
                    if ($Colorize) {
                        $isBold = ($col.Kind -eq "pnl_bold")
                        $cells.Add((Format-SignedPnlCell -Value ([double]$Row.($col.NumProp)) -Width $w -Bold $isBold -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                { $_ -in @("pnl_pct", "pnl_pct_bold") } {
                    if ($Colorize) {
                        $isBold = ($col.Kind -eq "pnl_pct_bold")
                        $cells.Add((Format-SignedPnlWithPctCell -Value ([double]$Row.($col.NumProp)) -Pct ([double]$Row.($col.PctProp)) -Width $w -Bold $isBold -IncludeDollar $true -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                { $_ -in @("rebate", "rebate_dollar") } {
                    if ($Colorize) {
                        $withDollar = ($col.Kind -eq "rebate_dollar")
                        $cells.Add((Format-RebateCell -Value ([double]$Row.($col.NumProp)) -Width $w -Bold $false -IncludeDollar $withDollar -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                "fee" {
                    if ($Colorize) {
                        $cells.Add((Format-FeeCell -FeePaidPositive ([double]$Row.($col.NumProp)) -Width $w -Bold $false -IncludeDollar $true -Culture $inv))
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
                "roi" {
                    if ($Colorize) {
                        $cells.Add((Format-SignedPctCell -Ratio ([double]$Row.($col.NumProp)) -Width $w -Bold $false -Culture $inv))
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
    $plainSummary.Add("CONFIG LEADERBOARD (sorted by Net P&L desc)")
    $plainSummary.Add(($plainRowFmt -f $headers))
    $plainSummary.Add(($plainRowFmt -f $dashes))
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        $plainSummary.Add((Format-LeaderboardRow -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $false))
    }
    $plainSummary.Add(($plainRowFmt -f $dashes))
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
    Write-Host (Format-ColorText -Text "🏆 CONFIG LEADERBOARD (sorted by Net P&L desc)" -Code $AnsiBoldCyan)
    Write-Host (Format-ColorText -Text ($plainRowFmt -f $headers) -Code $AnsiBoldWhite)
    Write-Host (Format-ColorText -Text ($plainRowFmt -f $dashes) -Code $AnsiGray)
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        Write-Host (Format-LeaderboardRow -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $true)
    }
    Write-Host (Format-ColorText -Text ($plainRowFmt -f $dashes) -Code $AnsiGray)

    Write-Host ""
}
finally {
    Pop-Location
}