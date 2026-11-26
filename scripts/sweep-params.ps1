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
.EXAMPLE
    pwsh -File scripts/sweep-params.ps1 -Quick
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

    [switch]$Quick,

    [int]$MinWindows = 2,

    [string[]]$Gammas = @(),

    [string[]]$VarianceSpreads = @(),

    [string[]]$MinSpreadTicks = @(),

    [string[]]$MaxCrossTickLossMicros = @(),

    [string[]]$MaxEntryProbs = @(),

    [string[]]$MaxEntryPrices = @(),

    [string[]]$LeadLagThresholds = @(),

    [string[]]$LeadLagWindows = @(),

    [string[]]$LeadLagCooldowns = @(),

    [string[]]$LeadLagDirectionals = @(),

    [string[]]$HedgeSkewMults = @(),

    [string[]]$CapTier1Hedges = @(),

    [string[]]$Tier1HedgeTriggers = @(),

    [string[]]$EntryOpenDelays = @(),

    [string[]]$EntryCutoffs = @(),

    [Nullable[uint64]]$MaxCombinedBidMicro = $null,

    [ValidateSet("", "conservative", "optimistic")]
    [string]$QueueDecay = "",

    [Nullable[uint64]]$VMax = $null,

    [Nullable[uint64]]$BaseSize = $null,

    [int]$Workers = 1,

    [string]$OutputDir = "",

    [string]$ResumeDir = "",

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
# Phase 1: Width-Safe ANSI Styling, Bottom-Right Status HUD & Formatting Helpers
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
$AnsiHudBg      = "$Esc[48;5;236;97m"

$script:ActiveHudText = ""
$script:HudVisible = $false

function Get-TerminalDimensions {
    try {
        $w = [Console]::WindowWidth
        $h = [Console]::WindowHeight
        if ($w -gt 20 -and $h -gt 3) {
            return @{ Width = $w; Height = $h }
        }
    } catch {}
    return $null
}

function Draw-BottomRightHud {
    param([string]$StatusText)
    if (-not $UseAnsi -or [string]::IsNullOrEmpty($StatusText)) { return }
    $dims = Get-TerminalDimensions
    if ($null -eq $dims) { return }

    $w = $dims.Width
    $h = $dims.Height
    $padded = " $StatusText "
    if ($padded.Length -gt $w) {
        $padded = $padded.Substring($padded.Length - $w)
    }

    # Read current cursor position from .NET Console (avoids DECSC/DECRC & DECSTBM overwriting lines)
    $origLeft = [Console]::CursorLeft
    $origTop  = [Console]::CursorTop

    # If cursor is currently on the bottom row, scroll output up by 1 line so the badge never covers printed text
    if ($origTop -ge ($h - 1)) {
        [Console]::Write("`n")
        $origTop = [Math]::Max(0, [Console]::CursorTop - 1)
    }

    $row = $h
    $col = [Math]::Max(1, $w - $padded.Length + 1)
    [Console]::Write("${Esc}[${row};1H${Esc}[2K${Esc}[${row};${col}H${AnsiHudBg}${padded}${AnsiReset}")
    [Console]::SetCursorPosition([Math]::Min($w - 1, [Math]::Max(0, $origLeft)), [Math]::Min($h - 1, [Math]::Max(0, $origTop)))
    try { [Console]::Out.Flush() } catch {}
    $script:HudVisible = $true
}

function Update-BottomRightHud {
    param(
        [int]$ConfigIdx,
        [int]$ConfigTotal,
        [int]$FileIdx,
        [int]$FileTotal
    )
    if (-not $UseAnsi) { return }
    $script:ActiveHudText = "Config #$ConfigIdx/$ConfigTotal  |  File #$FileIdx/$FileTotal"
    Draw-BottomRightHud -StatusText $script:ActiveHudText
}

function Clear-BottomRightHud {
    if (-not $UseAnsi) { return }
    if ($script:HudVisible) {
        $dims = Get-TerminalDimensions
        if ($null -ne $dims) {
            $w = $dims.Width
            $h = $dims.Height
            $origLeft = [Console]::CursorLeft
            $origTop  = [Console]::CursorTop
            [Console]::Write("${Esc}[${h};1H${Esc}[2K")
            [Console]::SetCursorPosition([Math]::Min($w - 1, [Math]::Max(0, $origLeft)), [Math]::Min($h - 1, [Math]::Max(0, $origTop)))
            try { [Console]::Out.Flush() } catch {}
        }
        $script:HudVisible = $false
    }
    $script:ActiveHudText = ""
}

function Write-HostHud {
    param([string]$Message = "")
    if ($script:HudVisible) {
        $dims = Get-TerminalDimensions
        if ($null -ne $dims) {
            $w = $dims.Width
            $h = $dims.Height
            $origLeft = [Console]::CursorLeft
            $origTop  = [Console]::CursorTop
            [Console]::Write("${Esc}[${h};1H${Esc}[2K")
            [Console]::SetCursorPosition([Math]::Min($w - 1, [Math]::Max(0, $origLeft)), [Math]::Min($h - 1, [Math]::Max(0, $origTop)))
        }
        $script:HudVisible = $false
    }
    Write-Host $Message
    if (-not [string]::IsNullOrEmpty($script:ActiveHudText)) {
        Draw-BottomRightHud -StatusText $script:ActiveHudText
    }
}

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

function Format-QuantityCell {
    param(
        [double]$Value,
        [int]$Width = 7,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    if ([Math]::Abs($Value - [Math]::Round($Value)) -lt 1e-6) {
        return ([Math]::Round($Value).ToString("0", $Culture)).PadLeft($Width)
    }
    return ($Value.ToString("0.00", $Culture)).PadLeft($Width)
}

function Format-SignedPnlCell {
    param(
        [double]$Value,
        [int]$Width = 9,
        [bool]$Bold = $false,
        [int]$Decimals = 4,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $fmt = "+0." + ("0" * $Decimals) + ";-0." + ("0" * $Decimals) + ";+0." + ("0" * $Decimals)
    $raw = $Value.ToString($fmt, $Culture).PadLeft($Width)
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
        [int]$Width = 18,
        [bool]$Bold = $false,
        [bool]$IncludeDollar = $true,
        [int]$Decimals = 4,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $sign = if ($Value -gt 1e-9) { "+" } elseif ($Value -lt -1e-9) { "-" } else { "+" }
    $absVal = [Math]::Abs($Value)
    $fmtStr = "0." + ("0" * $Decimals)
    $valStr = if ($IncludeDollar) {
        "{0}`${1}" -f $sign, $absVal.ToString($fmtStr, $Culture)
    } else {
        "{0}{1}" -f $sign, $absVal.ToString($fmtStr, $Culture)
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

function Get-TradingRoiRatio {
    param(
        [double]$Deployed,
        [double]$Returned,
        [double]$FeesPaid
    )
    # Single definition of trading ROI: (Returned - Deployed - Fees) / Deployed.
    # Rebates are a separate income source and are intentionally excluded.
    # Returns NaN when nothing was deployed (avoids divide-by-zero).
    if ($Deployed -gt 1e-6) {
        return (($Returned - $Deployed - $FeesPaid) / $Deployed)
    }
    return [double]::NaN
}

function Format-RebateCell {
    param(
        [double]$Value,
        [int]$Width = 8,
        [bool]$Bold = $false,
        [bool]$IncludeDollar = $false,
        [int]$Decimals = 2,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $fmtStr = "0." + ("0" * $Decimals)
    $raw = if ($IncludeDollar) {
        $sign = if ($Value -ge 0.0) { "+" } else { "-" }
        ("{0}`${1}" -f $sign, [Math]::Abs($Value).ToString($fmtStr, $Culture)).PadLeft($Width)
    } else {
        $Value.ToString($fmtStr, $Culture).PadLeft($Width)
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
        [int]$Width = 8,
        [bool]$Bold = $false,
        [bool]$IncludeDollar = $true,
        [int]$Decimals = 4,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $absFee = [Math]::Abs($FeePaidPositive)
    $fmtStr = "0." + ("0" * $Decimals)
    $numStr = $absFee.ToString($fmtStr, $Culture)
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
        [int]$Width = 5,
        [bool]$Bold = $false,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    if ([double]::IsNaN($Pct)) { return (Format-ColorText -Text ('n/a'.PadLeft($Width)) -Code $AnsiGray) }
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

function Format-DollarParam {
    param(
        [string]$RawVal,
        [double]$Scale = 1.0,
        [int]$MinDecimals = 2,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    if ($RawVal.Equals("off", [System.StringComparison]::OrdinalIgnoreCase)) {
        return "off"
    }
    $val = [double]::Parse($RawVal, $Culture) / $Scale
    if ($val -eq 0.0) {
        return "off"
    }
    $fmt = "0." + ("0" * $MinDecimals) + ("#" * [Math]::Max(0, 4 - $MinDecimals))
    return ("`${0}" -f $val.ToString($fmt, $Culture))
}

function Format-GateCell {
    param([string]$RawText, [bool]$Pass, [bool]$IsNA, [int]$Width = 10)
    if ($IsNA) { return (Format-ColorText -Text ('n/a'.PadLeft($Width)) -Code $AnsiGray) }
    $code = if ($Pass) { $AnsiGreen } else { $AnsiBoldRed }
    return (Format-ColorText -Text $RawText.PadLeft($Width) -Code $code)
}

function Format-GateMoneyText {
    param([double]$Value, [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture)
    $sign = if ($Value -lt -1e-9) { "-" } else { "+" }
    return ("{0}`${1}" -f $sign, [Math]::Abs($Value).ToString("0.00", $Culture))
}

function Format-VerdictCell {
    param([string]$Verdict, [int]$Width = 12)
    $raw = $Verdict.PadLeft($Width)
    $code = switch -Wildcard ($Verdict) { "APPROVED" { $AnsiBoldGreen; break } "FAIL*" { $AnsiBoldRed; break } "UNDER*" { $AnsiBoldYellow; break } "NEEDS-*" { $AnsiBoldYellow; break } default { $AnsiGray } }
    return (Format-ColorText -Text $raw -Code $code)
}

function Get-SurvivalInputs {
    param(
        $CapRuns,
        [int]$TotalWindows,
        [bool]$IsMultiCapture,
        [int]$CaptureCount,
        [double]$SumVolIn,
        [double]$SumGross,
        [double]$SumSettle,
        [double]$SumUnreal,
        [double[]]$WindowNets = @(),
        [string]$FallbackProp = "NetPlusRebNum",
        [double]$AllocatedCapital = [double]::NaN,
        [double]$SumMergeNet = [double]::NaN,
        [double]$SumMergePos = [double]::NaN,
        [double]$SumMergeNegLoss = [double]::NaN
    )
    $out = @{ IS = [double]::NaN; OOS = [double]::NaN; Retain = [double]::NaN; MaxDD = [double]::NaN; Drag = 0.0; TailDrag = [double]::NaN; ISWin = 0; OOSWin = 0 }
    $ddDenom = if (-not [double]::IsNaN($AllocatedCapital) -and $AllocatedCapital -gt 0.0) {
        [Math]::Max($AllocatedCapital, 1.0)
    } else {
        [Math]::Max($SumVolIn, 1.0)
    }
    # Preferred: actual per-window net PnL from the window lifecycle events the replay emits
    # (`window_end` or `epoch_rollover` realized + optional 3% rebate delta).
    # Capture-level prorating is only used when no window events exist.
    # Use the per-window series only when it covers the whole sample: a partial series would
    # describe a different sample than the capture totals the same row reports.
    $windowSeriesComplete = ($TotalWindows -le 0) -or ($WindowNets.Count -eq $TotalWindows)
    if ($null -ne $WindowNets -and $WindowNets.Count -gt 0 -and $windowSeriesComplete) {
        # IS/OOS needs >= 4 windows to split; drawdown is valid on any window series.
        if ($WindowNets.Count -ge 4) {
            $isTarget = [int][Math]::Floor($WindowNets.Count * 0.7)
            $oosWin = $WindowNets.Count - $isTarget
            $isAcc = 0.0; $oosAcc = 0.0
            for ($wi = 0; $wi -lt $WindowNets.Count; $wi++) {
                if ($wi -lt $isTarget) { $isAcc += $WindowNets[$wi] } else { $oosAcc += $WindowNets[$wi] }
            }
            $out.IS = $isAcc; $out.OOS = $oosAcc; $out.ISWin = $isTarget; $out.OOSWin = $oosWin
            $isAvg = $isAcc / $isTarget
            $oosAvg = $oosAcc / $oosWin
            if ($isAvg -gt 1e-9) { $out.Retain = ($oosAvg / $isAvg) * 100.0 }
            elseif ($oosAvg -gt 1e-9) { $out.Retain = [double]::PositiveInfinity }
        }
        # Drawdown on the actual per-window equity curve divided by allocated working capital.
        $peak = 0.0; $wDD = 0.0; $wEq = 0.0
        foreach ($wn in $WindowNets) { $wEq += $wn; if ($wEq -gt $peak) { $peak = $wEq }; $wDD = [Math]::Max($wDD, $peak - $wEq) }
        $out.MaxDD = ($wDD / $ddDenom) * 100.0
    }
    elseif ($IsMultiCapture -and $CaptureCount -ge 2 -and $TotalWindows -ge 4) {
        $isTarget = [int][Math]::Floor($TotalWindows * 0.7)
        $oosWin = $TotalWindows - $isTarget
        $cumW = 0; $isAcc = 0.0; $oosAcc = 0.0
        foreach ($c in $CapRuns) {
            $w = [int]$c.Windows
            $capVal = [double]$c.$FallbackProp
            if (($cumW + $w) -le $isTarget) { $isAcc += $capVal }
            elseif ($cumW -ge $isTarget) { $oosAcc += $capVal }
            else {
                $perW = $capVal / [Math]::Max(1, $w)
                $isAcc += $perW * ($isTarget - $cumW); $oosAcc += $perW * ($w - ($isTarget - $cumW))
            }
            $cumW += $w
        }
        $out.IS = $isAcc; $out.OOS = $oosAcc; $out.ISWin = $isTarget; $out.OOSWin = $oosWin
        $isAvg = $isAcc / $isTarget
        $oosAvg = $oosAcc / $oosWin
        if ($isAvg -gt 1e-9) { $out.Retain = ($oosAvg / $isAvg) * 100.0 }
        elseif ($oosAvg -gt 1e-9) { $out.Retain = [double]::PositiveInfinity }
    }
    # Capture-level fallback only: keeps MaxDD at n/a on a single capture with no window nets.
    if ($CapRuns.Count -ge 2 -and [double]::IsNaN($out.MaxDD)) {
        $peak = 0.0; $maxDD = 0.0; $eq = 0.0
        foreach ($c in $CapRuns) { $eq += [double]$c.$FallbackProp; if ($eq -gt $peak) { $peak = $eq }; $maxDD = [Math]::Max($maxDD, $peak - $eq) }
        $out.MaxDD = ($maxDD / $ddDenom) * 100.0
    }
    $settleLoss = [Math]::Max(0.0, -($SumSettle + [Math]::Min($SumUnreal, 0.0)))
    $dragDenom = if (-not [double]::IsNaN($SumMergePos)) {
        [Math]::Max($SumMergePos, 0.0)
    } elseif (-not [double]::IsNaN($SumMergeNet)) {
        [Math]::Max($SumMergeNet, 0.0)
    } else {
        # Fallback when summary predates merge totals: reconstruct pre-settlement gains.
        [Math]::Max($SumGross - $SumSettle, 0.0)
    }
    if ($dragDenom -gt 1e-9) { $out.Drag = ($settleLoss / $dragDenom) * 100.0 }
    elseif ($settleLoss -gt 1e-9) { $out.Drag = 100.0 }

    if (-not [double]::IsNaN($SumMergePos) -and -not [double]::IsNaN($SumMergeNegLoss)) {
        $tailLoss = $settleLoss + [Math]::Max(0.0, $SumMergeNegLoss)
        $posGains = [Math]::Max($SumMergePos, 0.0)
        if ($posGains -gt 1e-9) { $out.TailDrag = ($tailLoss / $posGains) * 100.0 }
        elseif ($tailLoss -gt 1e-9) { $out.TailDrag = 100.0 }
        else { $out.TailDrag = 0.0 }
    }
    return $out
}

# Spec Filter 4 - Minimum Sample Size ([strategy-verification-spec.md](../docs/strategy-verification-spec.md)):
# under this many fills the G1-G6 thresholds cannot decide either way, so the verdict must not read
# as PASS or FAIL. The gate cells keep their measured state so the sample's shape stays visible.
$script:MinValidationFills = 400

function Evaluate-SurvivalGateRow {
    param(
        [double]$ISVal,
        [double]$OOSVal,
        [double]$RetainVal,
        [double]$MaxDDVal,
        [double]$DragVal,
        [double]$NbrVal,
        [double]$TailDragVal = [double]::NaN,
        [double]$TotalFills = [double]::NaN
    )
    $g1 = if ([double]::IsNaN($ISVal)) { "N/A" } elseif ($ISVal -gt 0.0) { "PASS" } else { "FAIL" }
    $g2 = if ([double]::IsNaN($OOSVal)) { "N/A" } elseif ($OOSVal -gt 0.0) { "PASS" } else { "FAIL" }
    $g3 = if ([double]::IsNaN($RetainVal) -or [double]::IsInfinity($RetainVal)) { "N/A" } elseif ($RetainVal -ge 50.0) { "PASS" } else { "FAIL" }
    $g4 = if ([double]::IsNaN($MaxDDVal)) { "N/A" } elseif ($MaxDDVal -le 25.0) { "PASS" } else { "FAIL" }
    $g5ExpiryOk = (-not [double]::IsNaN($DragVal)) -and ($DragVal -le 30.0)
    $g5TailOk = (-not [double]::IsNaN($TailDragVal)) -and ($TailDragVal -le 50.0)
    # Unknown tail drag (legacy/asymmetric summaries) cannot verify G5 and must not read as PASS.
    $g5 = if ([double]::IsNaN($DragVal) -or [double]::IsNaN($TailDragVal)) { "N/A" }
        elseif ($g5ExpiryOk -and $g5TailOk) { "PASS" } else { "FAIL" }
    $g6 = if ([double]::IsNaN($NbrVal)) { "N/A" } elseif ($NbrVal -ge 60.0) { "PASS" } else { "FAIL" }
    $failGate = ""
    foreach ($kv in @(@("G1", $g1), @("G2", $g2), @("G3", $g3), @("G4", $g4), @("G5", $g5), @("G6", $g6))) {
        if ($kv[1] -eq "FAIL") { $failGate = $kv[0]; break }
    }
    # Unknown fill count (legacy callers) stays on the old behaviour rather than silently under-powering.
    $underpowered = (-not [double]::IsNaN($TotalFills)) -and ($TotalFills -lt $script:MinValidationFills)
    $verdict = if ($underpowered) { "UNDERPOWERED" }
        elseif ($failGate -ne "") { "FAIL($failGate)" }
        elseif ($g1 -eq "N/A" -or $g2 -eq "N/A" -or $g3 -eq "N/A") { "NEEDS-OOS" }
        elseif ($g4 -eq "N/A" -or $g5 -eq "N/A" -or $g6 -eq "N/A") { "N/A" }
        else { "APPROVED" }
    return @{ G1 = $g1; G2 = $g2; G3 = $g3; G4 = $g4; G5 = $g5; G6 = $g6; Underpowered = $underpowered; Verdict = $verdict }
}

function Get-AxisSortRank {
    param([string]$Val)
    $s = if ($null -eq $Val) { "" } else { $Val.Trim() }
    $lower = $s.ToLowerInvariant()
    if ($lower -eq "false") { return [double]0.0 }
    if ($lower -eq "true")  { return [double]1.0 }
    if ($lower -eq "off" -or $lower -eq "none" -or $lower -eq "") { return [double]::PositiveInfinity }
    $clean = $s.TrimStart('$').TrimEnd('s', 'S')
    $num = 0.0
    if ([double]::TryParse($clean, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$num)) {
        return $num
    }
    return [double]::NaN
}

function Update-SurvivalVerdicts {
    param($Sorted)
    $axes = @("Gamma", "VarSpr", "MinT", "XTick", "MaxP", "MaxPx", "LLTh", "LLWin", "LLCd", "LLDir", "HSkew", "T1Cap", "T1Trig", "EntryDelay", "EntryCut")
    $axisIndexMaps = @{}
    foreach ($ax in $axes) {
        $uniqueVals = @($Sorted | ForEach-Object { [string]$_.$ax } | Select-Object -Unique)
        $ordered = @($uniqueVals | Sort-Object -Property @{ Expression = { Get-AxisSortRank $_ } }, @{ Expression = { $_ } })
        $idxMap = @{}
        for ($vi = 0; $vi -lt $ordered.Count; $vi++) {
            $idxMap[$ordered[$vi]] = $vi
        }
        $axisIndexMaps[$ax] = $idxMap
    }

    foreach ($grow in $Sorted) {
        $tot = 0; $prof0 = 0; $prof3 = 0
        if ($Sorted.Count -gt 1) {
            foreach ($other in $Sorted) {
                if ($other -eq $grow) { continue }
                $manhattan = 0
                foreach ($ax in $axes) {
                    $map = $axisIndexMaps[$ax]
                    $manhattan += [Math]::Abs($map[[string]$other.$ax] - $map[[string]$grow.$ax])
                    if ($manhattan -gt 1) { break }
                }
                if ($manhattan -eq 1) {
                    $tot++
                    $hasOos0 = ($null -ne $other.PSObject.Properties["ISNum"]) -and ($null -ne $other.PSObject.Properties["OOSNum"]) -and `
                        (-not [double]::IsNaN([double]$other.ISNum)) -and (-not [double]::IsNaN([double]$other.OOSNum))
                    $pass0 = if ($hasOos0) {
                        ([double]$other.ISNum -gt 0.0) -and ([double]$other.OOSNum -gt 0.0)
                    } else {
                        [double]$other.NetPnLNum -gt 0.0
                    }
                    if ($pass0) { $prof0++ }

                    if ($null -ne $other.PSObject.Properties["NetPlusRebNum"]) {
                        $hasOos3 = ($null -ne $other.PSObject.Properties["ISNum3"]) -and ($null -ne $other.PSObject.Properties["OOSNum3"]) -and `
                            (-not [double]::IsNaN([double]$other.ISNum3)) -and (-not [double]::IsNaN([double]$other.OOSNum3))
                        $pass3 = if ($hasOos3) {
                            ([double]$other.ISNum3 -gt 0.0) -and ([double]$other.OOSNum3 -gt 0.0)
                        } else {
                            [double]$other.NetPlusRebNum -gt 0.0
                        }
                        if ($pass3) { $prof3++ }
                    }
                }
            }
        }
        $grow.NbrNum = if ($tot -gt 0) { ($prof0 / $tot) * 100.0 } else { [double]::NaN }
        $tailDrag0 = if ($null -ne $grow.PSObject.Properties["TailDragNum"]) { [double]$grow.TailDragNum } else { [double]::NaN }
        $fillsProp = $grow.PSObject.Properties["FillsNum"]
        $totalFills = if ($null -ne $fillsProp -and $null -ne $fillsProp.Value) { [double]$fillsProp.Value } else { [double]::NaN }
        $eval0 = Evaluate-SurvivalGateRow -ISVal $grow.ISNum -OOSVal $grow.OOSNum -RetainVal $grow.RetainNum -MaxDDVal $grow.MaxDDNum -DragVal $grow.DragNum -NbrVal $grow.NbrNum -TailDragVal $tailDrag0 -TotalFills $totalFills
        $grow.G1 = $eval0.G1; $grow.G2 = $eval0.G2; $grow.G3 = $eval0.G3
        $grow.G4 = $eval0.G4; $grow.G5 = $eval0.G5; $grow.G6 = $eval0.G6
        $grow.Verdict = $eval0.Verdict

        if ($null -ne $grow.PSObject.Properties["ISNum3"]) {
            $grow.NbrNum3 = if ($tot -gt 0) { ($prof3 / $tot) * 100.0 } else { [double]::NaN }
            $eval3 = Evaluate-SurvivalGateRow -ISVal $grow.ISNum3 -OOSVal $grow.OOSNum3 -RetainVal $grow.RetainNum3 -MaxDDVal $grow.MaxDDNum3 -DragVal $grow.DragNum -NbrVal $grow.NbrNum3 -TailDragVal $tailDrag0 -TotalFills $totalFills
            $grow.G1_3 = $eval3.G1; $grow.G2_3 = $eval3.G2; $grow.G3_3 = $eval3.G3
            $grow.G4_3 = $eval3.G4; $grow.G5_3 = $eval3.G5; $grow.G6_3 = $eval3.G6
            $grow.Verdict3 = $eval3.Verdict
        }
    }
}

function Test-EntryPriceSwept {
    param(
        [string[]]$Prices,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture,
        [Nullable[double]]$DefaultEntryPrice = $null
    )
    # No hardcoded fallback: the configured cap must come from DEFAULTS.json, so a caller that
    # omits it fails loudly instead of comparing prices against a stale number. NaN is a real
    # value here (DEFAULTS engine.max_entry_price is null when the cap is off), so only $null
    # means "not supplied".
    if ($null -eq $DefaultEntryPrice) {
        throw "Test-EntryPriceSwept requires -DefaultEntryPrice (pass DEFAULTS.json engine.max_entry_price)"
    }
    $defaultPrice = [double]$DefaultEntryPrice
    if ($Prices.Count -gt 1) { return $true }
    return (@($Prices | Where-Object {
        -not $_.Equals("off", [System.StringComparison]::OrdinalIgnoreCase) -and
        [double]::Parse($_, $Culture) -ne $defaultPrice
    }).Count -gt 0)
}

function Get-ParamStateColor {
    param(
        [string]$Val,
        [string]$DefaultColor = $AnsiBoldWhite
    )
    $lower = $Val.Trim().ToLowerInvariant()
    if ($lower -eq "true" -or $lower -eq "on") {
        return $AnsiBoldGreen
    }
    if ($lower -eq "false" -or $lower -eq "off") {
        return $AnsiGray
    }
    return $DefaultColor
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
    $labelColor = if ($IsHighLevel) { $AnsiGray } else { $AnsiCyan }
    $paddedLabel = Format-ColorText -Text $Label.PadRight(11) -Code $labelColor

    if ($IsLeftColumn) {
        if ($isChanged) {
            $tag = "{0}  ⚡ (was: {1})" -f $CurrVal, $PrevVal
            $padTo = if ($tag.Contains("⚡")) { 23 } else { 24 }
            $paddedVal = $tag.PadRight($padTo)
            $coloredVal = Format-ColorText -Text $paddedVal -Code $AnsiBoldYellow
            return ("{0}: {1}" -f $paddedLabel, $coloredVal)
        } else {
            $stateColor = Get-ParamStateColor -Val $CurrVal -DefaultColor $AnsiBoldWhite
            $valCell = Format-ColorText -Text $CurrVal.PadRight(24) -Code $stateColor
            return ("{0}: {1}" -f $paddedLabel, $valCell)
        }
    } else {
        if ($isChanged) {
            $tag = "{0}  ⚡ (was: {1})" -f $CurrVal, $PrevVal
            $coloredVal = Format-ColorText -Text $tag -Code $AnsiBoldYellow
            return ("{0}: {1}" -f $paddedLabel, $coloredVal)
        } else {
            $stateColor = Get-ParamStateColor -Val $CurrVal -DefaultColor $AnsiBoldWhite
            $valCell = Format-ColorText -Text $CurrVal -Code $stateColor
            return ("{0}: {1}" -f $paddedLabel, $valCell)
        }
    }
}

function Format-ConfigBannerLines {
    param(
        [int]$Index,
        [int]$Total,
        [hashtable]$Curr,
        [hashtable]$Prev,
        [string]$StartTimeText = ""
    )
    $timeSuffix = if ([string]::IsNullOrWhiteSpace($StartTimeText)) { "" } else { " - [$StartTimeText]" }
    $headerPrefix = "⚙️  [CONFIG #{0} / {1}]{2} " -f $Index, $Total, $timeSuffix
    $dashCount = [Math]::Max(10, 100 - $headerPrefix.Length)
    $topRule = (Format-ColorText -Text $headerPrefix -Code $AnsiBoldCyan) + (Format-ColorText -Text ("-" * $dashCount) -Code $AnsiGray)
    $subRule = Format-ColorText -Text ("  · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · ·") -Code $AnsiGray
    $botRule = Format-ColorText -Text ("-" * 100) -Code $AnsiGray
    $pipe = Format-ColorText -Text " |  " -Code $AnsiGray

    $highLevelPairs = @(
        @("V Max",      "Base Size"),
        @("Max Pair",   "Queue Decay")
    )

    $sweptPairs = @(
        @("Gamma",       "Variance"),
        @("Min Ticks",   "Cross Loss"),
        @("Entry Prob",  "Entry Price"),
        @("Threshold",   "Window Ms"),
        @("Cooldown Ms", "Direction"),
        @("Hedge Skew",  "Tier 1 Cap"),
        @("T1 Trigger",  "Entry Delay"),
        @("Entry Cut")
    )

    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add("")
    $lines.Add($topRule)
    $lines.Add((Format-ColorText -Text "  [High-Level / Session Bounds]" -Code $AnsiGray))
    foreach ($pair in $highLevelPairs) {
        $kLeft = $pair[0]
        $kRight = $pair[1]
        $leftStr = Format-ParamCell -Label $kLeft -CurrVal ([string]$Curr[$kLeft]) -PrevVal "" -IsLeftColumn $true -IsHighLevel $true
        $rightStr = Format-ParamCell -Label $kRight -CurrVal ([string]$Curr[$kRight]) -PrevVal "" -IsLeftColumn $false -IsHighLevel $true
        $lines.Add(("  {0}{1}{2}" -f $leftStr, $pipe, $rightStr))
    }
    $lines.Add($subRule)
    $lines.Add((Format-ColorText -Text "  [Swept / Strategy Parameters]" -Code $AnsiCyan))
    foreach ($pair in $sweptPairs) {
        $kLeft = $pair[0]
        $prevL = if ($null -ne $Prev -and $Prev.ContainsKey($kLeft)) { [string]$Prev[$kLeft] } else { "" }
        $currL = if ($Curr.ContainsKey($kLeft)) { [string]$Curr[$kLeft] } else { "" }
        $leftStr = Format-ParamCell -Label $kLeft -CurrVal $currL -PrevVal $prevL -IsLeftColumn $true -IsHighLevel $false
        if ($pair.Count -eq 2) {
            $kRight = $pair[1]
            $prevR = if ($null -ne $Prev -and $Prev.ContainsKey($kRight)) { [string]$Prev[$kRight] } else { "" }
            $currR = if ($Curr.ContainsKey($kRight)) { [string]$Curr[$kRight] } else { "" }
            $rightStr = Format-ParamCell -Label $kRight -CurrVal $currR -PrevVal $prevR -IsLeftColumn $false -IsHighLevel $false
            $lines.Add(("  {0}{1}{2}" -f $leftStr, $pipe, $rightStr))
        } else {
            $lines.Add(("  {0}" -f $leftStr))
        }
    }
    $lines.Add($botRule)
    return $lines.ToArray()
}

function Write-ConfigBanner {
    param(
        [int]$Index,
        [int]$Total,
        [hashtable]$Curr,
        [hashtable]$Prev,
        [string]$StartTimeText = ""
    )
    $lines = Format-ConfigBannerLines -Index $Index -Total $Total -Curr $Curr -Prev $Prev -StartTimeText $StartTimeText
    foreach ($line in $lines) {
        Write-HostHud $line
    }
}

function Resolve-NewSweepOutputDir {
    param(
        [string]$OutputDir,
        [string]$OrigPwd
    )
    $candidateOut = if ([System.IO.Path]::IsPathRooted($OutputDir)) { $OutputDir } else { Join-Path $OrigPwd $OutputDir }
    if (Test-Path -LiteralPath $candidateOut) {
        throw "OutputDir already exists (refusing to overwrite or reuse an existing sweep directory): $candidateOut. Choose a new unique path or use -ResumeDir."
    }
    $parent = Split-Path -Parent $candidateOut
    if (-not [string]::IsNullOrWhiteSpace($parent) -and -not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    New-Item -ItemType Directory -Path $candidateOut -ErrorAction Stop | Out-Null
    return (Resolve-Path -LiteralPath $candidateOut).Path
}

function Format-CaptureTableHeaderLines {
    param([int]$WinColW)
    $tblSep    = Format-ColorText -Text " │ " -Code $AnsiGray
    $tblHeader = "  {0}{1}{2}{1}{3}{1}{4}{1}{5}{1}{6}{1}{7}{1}{8}{1}{9}{1}{10}{1}{11}" -f `
        "#".PadLeft(2),
        $tblSep,
        "# of Windows".PadLeft($WinColW),
        "Deployed".PadLeft(9),
        "Pairs".PadLeft(7),
        "Returned".PadLeft(9),
        "Win".PadLeft(5),
        "Gross ROI / P&L".PadLeft(18),
        "Fees".PadLeft(8),
        "Net @ 0% Rebate".PadLeft(18),
        "Rebate @ 3%".PadLeft(11),
        "Net @ 3% Rebate".PadLeft(18)
    $tblDash   = "  {0}─┼─{1}─┼─{2}─┼─{3}─┼─{4}─┼─{5}─┼─{6}─┼─{7}─┼─{8}─┼─{9}─┼─{10}" -f `
        ("-" * 2), ("-" * $WinColW), ("-" * 9), ("-" * 7), ("-" * 9), ("-" * 5), ("-" * 18), ("-" * 8), ("-" * 18), ("-" * 11), ("-" * 18)
    return @(
        (Format-ColorText -Text $tblHeader -Code $AnsiBoldWhite),
        (Format-ColorText -Text $tblDash -Code $AnsiGray),
        $tblDash,
        $tblSep
    )
}

function Test-JsonlHasSummary {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $false }
    $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        if ($fs.Length -eq 0) { return $false }
        $readLen = [int][Math]::Min(16384L, $fs.Length)
        $null = $fs.Seek(-$readLen, [System.IO.SeekOrigin]::End)
        $buf = New-Object byte[] $readLen
        $offset = 0
        while ($offset -lt $readLen) {
            $n = $fs.Read($buf, $offset, $readLen - $offset)
            if ($n -le 0) { break }
            $offset += $n
        }
        $text = [System.Text.Encoding]::UTF8.GetString($buf, 0, $offset)
        $lines = $text -split "`r?`n"
        for ($i = $lines.Length - 1; $i -ge 0; $i--) {
            $trimmed = $lines[$i].Trim()
            if ($trimmed -ne "") {
                try {
                    $obj = $trimmed | ConvertFrom-Json
                    if ($obj.event -ne "summary") { return $false }
                    # A summary the metrics reader cannot parse is not a complete config.
                    return ($null -ne $obj.payload -and $null -ne $obj.payload.PSObject.Properties["realized_pnl_micro"])
                } catch {
                    return $false
                }
            }
        }
        return $false
    } finally {
        $fs.Dispose()
    }
}

function Get-ConfigDiskStatus {
    param(
        [string]$SweepDir,
        $Captures,
        [bool]$IsMultiCapture,
        [string]$Stem
    )
    $existingJsonl = [System.Collections.Generic.List[string]]::new()
    $existingLogs = [System.Collections.Generic.List[string]]::new()
    $validSummaryCount = 0
    $missingOrIncomplete = [System.Collections.Generic.List[string]]::new()

    foreach ($cap in $Captures) {
        if ($IsMultiCapture) {
            $capDir = Join-Path $SweepDir $cap.Stem
            $jPath = Join-Path $capDir "$Stem.jsonl"
            $lPath = Join-Path $capDir "$Stem.log"
        } else {
            $jPath = Join-Path $SweepDir "$Stem.jsonl"
            $lPath = Join-Path $SweepDir "$Stem.log"
        }
        $hasJ = Test-Path -LiteralPath $jPath -PathType Leaf
        $hasL = Test-Path -LiteralPath $lPath -PathType Leaf
        if ($hasJ) { $existingJsonl.Add($jPath) }
        if ($hasL) { $existingLogs.Add($lPath) }
        if ($hasJ -and (Test-JsonlHasSummary -Path $jPath)) {
            $validSummaryCount++
        } else {
            $missingOrIncomplete.Add($jPath)
        }
    }

    $status = if ($validSummaryCount -eq $Captures.Count) {
        "Complete"
    } elseif ($existingJsonl.Count -eq 0 -and $existingLogs.Count -eq 0) {
        "Pending"
    } else {
        "Partial"
    }

    return [PSCustomObject]@{
        Status              = $status
        ValidSummaryCount   = $validSummaryCount
        ExistingJsonlCount  = $existingJsonl.Count
        ExistingLogCount    = $existingLogs.Count
        ExistingLogs        = $existingLogs.ToArray()
        MissingOrIncomplete = $missingOrIncomplete.ToArray()
    }
}

function Invoke-CaptureReplayProcess {
    param(
        [string]$BinPath,
        [string[]]$CliArgs,
        [hashtable]$EnvOverrides,
        [string]$OutLog,
        [string]$WorkingDirectory,
        [string]$Label
    )
    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $BinPath
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true
    if (-not [string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        $psi.WorkingDirectory = $WorkingDirectory
    }
    foreach ($a in $CliArgs) {
        [void]$psi.ArgumentList.Add([string]$a)
    }
    foreach ($k in $EnvOverrides.Keys) {
        $psi.Environment[$k] = [string]$EnvOverrides[$k]
    }

    $proc = [System.Diagnostics.Process]::Start($psi)
    if ($null -eq $proc) {
        throw "Failed to launch process for $Label"
    }
    $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
    $stderrTask = $proc.StandardError.ReadToEndAsync()
    $proc.WaitForExit()
    [System.Threading.Tasks.Task]::WaitAll(@($stdoutTask, $stderrTask))

    $combined = $stdoutTask.Result + $stderrTask.Result
    [System.IO.File]::WriteAllText($OutLog, $combined, [System.Text.UTF8Encoding]::new($false))

    if ($proc.ExitCode -ne 0) {
        $tail = if (Test-Path -LiteralPath $OutLog) { (Get-Content -LiteralPath $OutLog -Tail 20) -join "`n" } else { "" }
        throw "Sweep run failed (exit=$($proc.ExitCode)) for $Label.`n$tail"
    }
}

function Read-CaptureJsonlMetrics {
    param(
        [string]$OutJsonl,
        $Cap,
        [int]$CapIdx,
        [uint64]$BaseSize,
        [uint64]$MaxCombinedBidMicro,
        [int]$WinColW,
        [string]$TblSep,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
    $allLines = @(Get-Content -LiteralPath $OutJsonl)
    if ($allLines.Count -eq 0) {
        throw "No events emitted in $OutJsonl"
    }
    $summaryLine = $allLines[$allLines.Count - 1]
    $summaryObj = $summaryLine | ConvertFrom-Json
    if ($summaryObj.event -ne "summary") {
        throw "Final JSONL row is not a summary event in $OutJsonl (found '$($summaryObj.event)')"
    }

    $payload = $summaryObj.payload
    $realized = [double]$payload.realized_pnl_micro / 1e6
    $unreal = [double]$payload.unrealized_pnl_micro / 1e6
    $grossPnl = $realized + $unreal
    $netPnl = [double]$payload.net_pnl_micro / 1e6
    $rebate = [double]$payload.estimated_rebate_micro / 1e6
    $netPlusReb = $netPnl + $rebate
    $pairs = [double]$payload.total_pairs_merged_micro / 1e6
    $fills = [int]$payload.fills_count
    $merges = [int]$payload.merges_count
    $rpm = [int]$payload.rpm_60s
    $mo1s = [double]$payload.markout_1s_bps
    $mo5s = [double]$payload.markout_5s_bps
    $settle = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("settlement_pnl_micro") -Scale 1e6 -DefaultValue 0.0
    $mergeNet = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("total_merge_gain_micro") -Scale 1e6
    $mergePos = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("positive_merge_gain_micro") -Scale 1e6
    $mergeNegLoss = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("negative_merge_loss_micro") -Scale 1e6
    $breakers = [int](Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("breaker_trips") -Scale 1.0 -DefaultValue 0.0)
    $flattens = [int](Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("flattens_executed") -Scale 1.0 -DefaultValue 0.0)
    $epochsDone = [int](Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("epochs_completed") -Scale 1.0 -DefaultValue ([double]$Cap.Windows))

    $qtyBought = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @(
        "total_shares_bought_micro",
        "shares_bought_micro",
        "total_bought_micro",
        "bought_shares_micro"
    ) -Scale 1e6

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

    # Single-pass scan for window lifecycle events plus legacy BUY fill/flatten and merge fallbacks.
    $needQtyScan = [double]::IsNaN($qtyBought)
    $needVolScan = [double]::IsNaN($volumeIn)
    $needBuyScan = $needQtyScan -or $needVolScan
    $needMergeScan = [double]::IsNaN($mergePos) -or [double]::IsNaN($mergeNegLoss) -or [double]::IsNaN($mergeNet)
    $scannedBuyEvents = 0; $scannedQtyBoughtMicro = [decimal]0; $scannedVolInMicro = [decimal]0
    $scannedMergeEvents = 0; $scannedMergeNet = 0.0; $scannedMergePos = 0.0; $scannedMergeNegLoss = 0.0
    $winEventPnls0 = [System.Collections.Generic.List[double]]::new()
    $winEventPnls3 = [System.Collections.Generic.List[double]]::new()
    $winSeenSlugs = @{}
    $winPrevRealized = 0.0
    $winPrevRebate = 0.0

    for ($li = 0; $li -lt ($allLines.Count - 1); $li++) {
        $evLine = $allLines[$li]
        if ($needBuyScan -and ($evLine -match '"event"\s*:\s*"(fill|flatten)"')) {
            try {
                $bObj = $evLine | ConvertFrom-Json
                if ($bObj.event -in @("fill", "flatten")) {
                    $bP = $bObj.payload
                    if ($null -ne $bP -and [string]$bP.side -eq "BUY") {
                        $szRaw = Get-OptionalPayloadDouble -Payload $bP -CandidateKeys @("filled_shares_micro", "size_micro") -Scale 1.0
                        $pxRaw = Get-OptionalPayloadDouble -Payload $bP -CandidateKeys @("price_micro") -Scale 1.0
                        if (-not [double]::IsNaN($szRaw) -and -not [double]::IsNaN($pxRaw)) {
                            $scannedBuyEvents++
                            $szMicro = [decimal][Math]::Round($szRaw)
                            $pxMicro = [decimal][Math]::Round($pxRaw)
                            $scannedQtyBoughtMicro += $szMicro
                            $scannedVolInMicro += [Math]::Floor(($szMicro * $pxMicro) / 1000000D)
                        }
                    }
                }
            } catch {}
        } elseif ($needMergeScan -and ($evLine -match '"event"\s*:\s*"merge"')) {
            try {
                $mObj = $evLine | ConvertFrom-Json
                if ($mObj.event -eq "merge") {
                    $scannedMergeEvents++
                    $mGain = [double]$mObj.payload.realized_gain_micro / 1e6
                    $scannedMergeNet += $mGain
                    if ($mGain -ge 0.0) { $scannedMergePos += $mGain } else { $scannedMergeNegLoss += (-$mGain) }
                }
            } catch {}
        } elseif ($evLine -match '"event"\s*:\s*"(window_end|window_summary|window_settled|epoch_rollover)"') {
            try {
                $evObj = $evLine | ConvertFrom-Json
                $evP = $evObj.payload
                $wSlug = $null
                foreach ($slugKey in @("expired_slug", "market_slug", "window_slug", "slug")) {
                    if ($null -eq $evP.PSObject.Properties[$slugKey]) { continue }
                    $slugVal = $evP.$slugKey
                    if ($null -ne $slugVal -and ([string]$slugVal) -ne "") { $wSlug = ([string]$slugVal).ToLowerInvariant(); break }
                }
                if ($null -eq $wSlug -or -not $winSeenSlugs.ContainsKey($wSlug)) {
                    if ($evObj.event -eq "epoch_rollover") {
                        $cumReal = Get-OptionalPayloadDouble -Payload $evP -CandidateKeys @("total_realized_pnl_micro") -Scale 1e6 -DefaultValue ([double]::NaN)
                        $cumReb = Get-OptionalPayloadDouble -Payload $evP -CandidateKeys @("estimated_rebate_micro", "rebate_micro") -Scale 1e6 -DefaultValue ([double]::NaN)
                        $wSettle = Get-OptionalPayloadDouble -Payload $evP -CandidateKeys @("settlement_pnl_delta_micro") -Scale 1e6 -DefaultValue 0.0
                        $wNet = if ([double]::IsNaN($cumReal)) { $wSettle } else { $cumReal - $winPrevRealized }
                        if (-not [double]::IsNaN($cumReal)) { $winPrevRealized = $cumReal }
                        $wReb = if ([double]::IsNaN($cumReb)) { 0.0 } else { $cumReb - $winPrevRebate }
                        if (-not [double]::IsNaN($cumReb)) { $winPrevRebate = $cumReb }
                    } else {
                        $wNet = Get-OptionalPayloadDouble -Payload $evP -CandidateKeys @("net_pnl_micro", "window_net_pnl_micro", "realized_pnl_micro", "settlement_pnl_delta_micro") -Scale 1e6 -DefaultValue 0.0
                        $wReb = Get-OptionalPayloadDouble -Payload $evP -CandidateKeys @("estimated_rebate_micro", "rebate_micro") -Scale 1e6 -DefaultValue 0.0
                        if ([double]::IsNaN($wReb)) { $wReb = 0.0 }
                    }
                    $winEventPnls0.Add($wNet)
                    $winEventPnls3.Add($wNet + $wReb)
                    if ($null -ne $wSlug) { $winSeenSlugs[$wSlug] = $true }
                }
            } catch {}
        }
    }

    if ([double]::IsNaN($qtyBought)) {
        if ($scannedBuyEvents -gt 0 -or ($fills -eq 0 -and $flattens -eq 0)) {
            $qtyBought = [double]$scannedQtyBoughtMicro / 1e6
        } else {
            $upBought = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("up_shares_bought_micro", "yes_shares_bought_micro") -Scale 1e6 -DefaultValue 0.0
            $dnBought = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("dn_shares_bought_micro", "down_shares_bought_micro", "no_shares_bought_micro") -Scale 1e6 -DefaultValue 0.0
            $qtyBought = if (($upBought + $dnBought) -gt 0.0) { $upBought + $dnBought } else { [double]$fills * [double]$BaseSize }
        }
    }
    if ([double]::IsNaN($volumeIn)) {
        if ($scannedBuyEvents -gt 0 -or ($fills -eq 0 -and $flattens -eq 0)) {
            $volumeIn = [double]$scannedVolInMicro / 1e6
        } else {
            $avgEntryPx = [Math]::Min(0.50, ($MaxCombinedBidMicro / 2e6))
            $volumeIn = [Math]::Max(0.0, $qtyBought * $avgEntryPx)
        }
    }
    if ([double]::IsNaN($closeVolume)) {
        $closeVolume = [Math]::Max(0.0, $volumeIn + $grossPnl)
    }
    # Only trust the event scan when it accounts for every merge the summary reports: a partial
    # scan understates negative merges and would let G5 pass on incomplete tail data.
    if (($merges -eq 0) -or ($scannedMergeEvents -gt 0 -and $scannedMergeEvents -eq $merges)) {
        if ([double]::IsNaN($mergeNet)) { $mergeNet = $scannedMergeNet }
        if ([double]::IsNaN($mergePos)) { $mergePos = $scannedMergePos }
        if ([double]::IsNaN($mergeNegLoss)) { $mergeNegLoss = $scannedMergeNegLoss }
    }

    $grossPct = if ($volumeIn -gt 1e-6) { ($closeVolume / $volumeIn) - 1.0 } else { [double]::NaN }
    $net0Pct = if ($volumeIn -gt 1e-6) { $netPnl / $volumeIn } else { [double]::NaN }
    $finalNetUsd = $netPlusReb
    $netPct = if ($volumeIn -gt 1e-6) { $finalNetUsd / $volumeIn } else { [double]::NaN }

    $winPctCap = [double]::NaN
    $winWonCap = 0
    $winTotalCap = 0
    $winWindowsCount = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("winning_windows", "positive_windows") -Scale 1.0
    $totWindowsPayload = Get-OptionalPayloadDouble -Payload $payload -CandidateKeys @("windows_count", "total_windows") -Scale 1.0 -DefaultValue ([double]$Cap.Windows)
    if ($winEventPnls0.Count -gt 0) {
        $winTotalCap = $winEventPnls0.Count
        $winWonCap = @($winEventPnls0 | Where-Object { $_ -gt 0.0 }).Count
        $winPctCap = ($winWonCap / $winTotalCap) * 100.0
    } elseif (-not [double]::IsNaN($winWindowsCount) -and $totWindowsPayload -gt 0) {
        $winTotalCap = [int]$totWindowsPayload
        $winWonCap = [int]$winWindowsCount
        $winPctCap = ($winWonCap / $winTotalCap) * 100.0
    }
    $capWindowNets0 = @($winEventPnls0.ToArray())
    $capWindowNets3 = @($winEventPnls3.ToArray())

    $idxCell      = Format-ColorText -Text ("{0,2}" -f ($CapIdx + 1)) -Code $AnsiCyan
    $winCell      = ("{0}" -f $Cap.Windows).PadLeft($WinColW)
    $volInCell    = ("`${0:F2}" -f $volumeIn).PadLeft(9)
    $pairsCell    = ("{0:F2}" -f $pairs).PadLeft(7)
    $closeVolCell = ("`${0:F2}" -f $closeVolume).PadLeft(9)
    $winRateCell  = Format-WinRateCell -Pct $winPctCap -Width 5 -Bold $false -Culture $Culture
    $grossCell    = Format-SignedPnlWithPctCell -Value $grossPnl -Pct $grossPct -Width 18 -Bold $false -IncludeDollar $true -Decimals 2 -Culture $Culture
    $feesCell     = Format-FeeCell -FeePaidPositive $feesPaid -Width 8 -Bold $false -IncludeDollar $true -Decimals 2 -Culture $Culture
    $net0Cell     = Format-SignedPnlWithPctCell -Value $netPnl -Pct $net0Pct -Width 18 -Bold $true -IncludeDollar $true -Decimals 2 -Culture $Culture
    $rebateCell   = Format-RebateCell -Value $rebate -Width 11 -Bold $false -IncludeDollar $true -Decimals 2 -Culture $Culture
    $netCell      = Format-SignedPnlWithPctCell -Value $finalNetUsd -Pct $netPct -Width 18 -Bold $true -IncludeDollar $true -Decimals 2 -Culture $Culture

    $rowText = "  {0}{1}{2}{1}{3}{1}{4}{1}{5}{1}{6}{1}{7}{1}{8}{1}{9}{1}{10}{1}{11}" -f `
        $idxCell, $TblSep, $winCell, $volInCell, $pairsCell, $closeVolCell, $winRateCell, $grossCell, $feesCell, $net0Cell, $rebateCell, $netCell

    $capRun = [PSCustomObject]@{
        CaptureName     = $Cap.Name
        CaptureStem     = $Cap.Stem
        Windows         = $Cap.Windows
        Fills           = $fills
        Merges          = $merges
        QuantityNum     = $qtyBought
        PairsNum        = $pairs
        WinPctNum       = $winPctCap
        WinWonNum       = $winWonCap
        WinTotalNum     = $winTotalCap
        VolumeInNum     = $volumeIn
        CloseVolumeNum  = $closeVolume
        RealizedNum     = $realized
        UnrealNum       = $unreal
        GrossPnlNum     = $grossPnl
        NetPnLNum       = $netPnl
        RebateNum       = $rebate
        FeesNum         = $feesPaid
        NetPlusRebNum   = $netPlusReb
        SettlementNum   = $settle
        MergeNetNum     = $mergeNet
        MergePosNum     = $mergePos
        MergeNegLossNum = $mergeNegLoss
        Breakers        = $breakers
        Flattens        = $flattens
        EpochsDone      = $epochsDone
        RPM             = $rpm
        MO1sNum         = $mo1s
        MO5sNum         = $mo5s
        WindowNets      = $capWindowNets0
        WindowNets3     = $capWindowNets3
        WindowCount     = $capWindowNets0.Count
    }

    return [PSCustomObject]@{
        RowText = $rowText
        CapRun  = $capRun
    }
}

function Complete-ConfigRunAggregation {
    param(
        $Spec,
        $CapRuns,
        [double]$CfgElapsedSec,
        [int]$TotalWindows,
        [bool]$IsMultiCapture,
        [int]$CaptureCount,
        [int]$WinColW,
        [string]$TblSep,
        [string]$TblDash,
        [double]$AllocatedCapital = [double]::NaN,
        [System.Globalization.CultureInfo]$Culture = [System.Globalization.CultureInfo]::InvariantCulture
    )
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
    $sumSettlement = 0.0
    $sumMergeNet = 0.0
    $sumMergePos = 0.0
    $sumMergeNegLoss = 0.0
    $sumBreakers = 0
    $sumFlattens = 0
    $sumEpochs = 0
    $maxRpm = 0
    $weightedMo1s = 0.0
    $weightedMo5s = 0.0
    $sumWinWon = 0
    $sumWinTotal = 0

    foreach ($c in $CapRuns) {
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
        $sumSettlement += $c.SettlementNum
        $sumMergeNet += $c.MergeNetNum
        $sumMergePos += $c.MergePosNum
        $sumMergeNegLoss += $c.MergeNegLossNum
        $sumBreakers += $c.Breakers
        $sumFlattens += $c.Flattens
        $sumEpochs += $c.EpochsDone
        if ($c.RPM -gt $maxRpm) { $maxRpm = $c.RPM }
        $weightedMo1s += ($c.Fills * $c.MO1sNum)
        $weightedMo5s += ($c.Fills * $c.MO5sNum)
        $sumWinWon += [int]$c.WinWonNum
        $sumWinTotal += [int]$c.WinTotalNum
    }

    $allWindowNets0 = [System.Collections.Generic.List[double]]::new()
    $allWindowNets3 = [System.Collections.Generic.List[double]]::new()
    foreach ($c in $CapRuns) {
        if ($null -ne $c.WindowNets) { foreach ($wn in $c.WindowNets) { $allWindowNets0.Add([double]$wn) } }
        if ($null -ne $c.WindowNets3) { foreach ($wn in $c.WindowNets3) { $allWindowNets3.Add([double]$wn) } }
    }
    $surv0 = Get-SurvivalInputs -CapRuns $CapRuns -TotalWindows $TotalWindows -IsMultiCapture $IsMultiCapture -CaptureCount $CaptureCount -SumVolIn $sumVolIn -SumGross $sumGrossPnl -SumSettle $sumSettlement -SumUnreal $sumUnreal -WindowNets $allWindowNets0.ToArray() -FallbackProp "NetPnLNum" -AllocatedCapital $AllocatedCapital -SumMergeNet $sumMergeNet -SumMergePos $sumMergePos -SumMergeNegLoss $sumMergeNegLoss
    $surv3 = Get-SurvivalInputs -CapRuns $CapRuns -TotalWindows $TotalWindows -IsMultiCapture $IsMultiCapture -CaptureCount $CaptureCount -SumVolIn $sumVolIn -SumGross $sumGrossPnl -SumSettle $sumSettlement -SumUnreal $sumUnreal -WindowNets $allWindowNets3.ToArray() -FallbackProp "NetPlusRebNum" -AllocatedCapital $AllocatedCapital -SumMergeNet $sumMergeNet -SumMergePos $sumMergePos -SumMergeNegLoss $sumMergeNegLoss
    $isNet = $surv0.IS; $oosNet = $surv0.OOS; $retainPct = $surv0.Retain; $maxDDPct = $surv0.MaxDD; $dragPct = $surv0.Drag; $tailDragPct = $surv0.TailDrag
    $isWinCount = $surv0.ISWin; $oosWinCount = $surv0.OOSWin

    $sumNetPlusReb = $sumNetPnl + $sumRebate
    $sumGrossPct = if ($sumVolIn -gt 1e-6) { ($sumCloseVol / $sumVolIn) - 1.0 } else { [double]::NaN }
    $sumNet0Pct = if ($sumVolIn -gt 1e-6) { $sumNetPnl / $sumVolIn } else { [double]::NaN }
    $sumNetPct = if ($sumVolIn -gt 1e-6) { $sumNetPlusReb / $sumVolIn } else { [double]::NaN }
    $winRatePct = if ($sumWinTotal -gt 0) { ($sumWinWon / $sumWinTotal) * 100.0 } else { [double]::NaN }

    $subIdxCell      = Format-ColorText -Text " Σ" -Code $AnsiBoldCyan
    $subWinCell      = Format-ColorText -Text ("SUBTOTAL {0}w" -f $TotalWindows).PadLeft($WinColW) -Code $AnsiBoldCyan
    $subVolInCell    = Format-ColorText -Text ("`${0:F2}" -f $sumVolIn).PadLeft(9) -Code $AnsiBoldWhite
    $subPairsCell    = Format-ColorText -Text ("{0:F2}" -f $sumPairs).PadLeft(7) -Code $AnsiBoldWhite
    $subCloseVolCell = Format-ColorText -Text ("`${0:F2}" -f $sumCloseVol).PadLeft(9) -Code $AnsiBoldWhite
    $subWinRateCell  = Format-WinRateCell -Pct $winRatePct -Width 5 -Bold $true -Culture $Culture
    $subGrossCell    = Format-SignedPnlWithPctCell -Value $sumGrossPnl -Pct $sumGrossPct -Width 18 -Bold $true -IncludeDollar $true -Decimals 2 -Culture $Culture
    $subFeesCell     = Format-FeeCell -FeePaidPositive $sumFeesPaid -Width 8 -Bold $true -IncludeDollar $true -Decimals 2 -Culture $Culture
    $subNet0Cell     = Format-SignedPnlWithPctCell -Value $sumNetPnl -Pct $sumNet0Pct -Width 18 -Bold $true -IncludeDollar $true -Decimals 2 -Culture $Culture
    $subRebateCell   = Format-RebateCell -Value $sumRebate -Width 11 -Bold $true -IncludeDollar $true -Decimals 2 -Culture $Culture
    $subNetCell      = Format-SignedPnlWithPctCell -Value $sumNetPlusReb -Pct $sumNetPct -Width 18 -Bold $true -IncludeDollar $true -Decimals 2 -Culture $Culture

    $footerLines = @(
        (Format-ColorText -Text $TblDash -Code $AnsiGray),
        ("  {0}{1}{2}{1}{3}{1}{4}{1}{5}{1}{6}{1}{7}{1}{8}{1}{9}{1}{10}{1}{11}" -f `
            $subIdxCell, $TblSep, $subWinCell, $subVolInCell, $subPairsCell, $subCloseVolCell, $subWinRateCell, $subGrossCell, $subFeesCell, $subNet0Cell, $subRebateCell, $subNetCell)
    )

    $qtyPlain = if ([Math]::Abs($sumQty - [Math]::Round($sumQty)) -lt 1e-6) {
        [Math]::Round($sumQty).ToString("0", $Culture)
    } else {
        $sumQty.ToString("F2", $Culture)
    }
    $grossSign = if ($sumGrossPnl -gt 1e-9) { "+" } elseif ($sumGrossPnl -lt -1e-9) { "-" } else { "+" }
    $grossPctStr = if ([double]::IsNaN($sumGrossPct) -or [double]::IsInfinity($sumGrossPct)) { "n/a" } else { $sumGrossPct.ToString("+0.00%;-0.00%;+0.00%", $Culture) }
    $grossPlain = ("{0}`${1} ({2})" -f $grossSign, [Math]::Abs($sumGrossPnl).ToString("0.00", $Culture), $grossPctStr)
    $net0Sign = if ($sumNetPnl -gt 1e-9) { "+" } elseif ($sumNetPnl -lt -1e-9) { "-" } else { "+" }
    $net0PctStr = if ([double]::IsNaN($sumNet0Pct) -or [double]::IsInfinity($sumNet0Pct)) { "n/a" } else { $sumNet0Pct.ToString("+0.00%;-0.00%;+0.00%", $Culture) }
    $net0Plain = ("{0}`${1} ({2})" -f $net0Sign, [Math]::Abs($sumNetPnl).ToString("0.00", $Culture), $net0PctStr)
    $netSign = if ($sumNetPlusReb -gt 1e-9) { "+" } elseif ($sumNetPlusReb -lt -1e-9) { "-" } else { "+" }
    $netPctStr = if ([double]::IsNaN($sumNetPct) -or [double]::IsInfinity($sumNetPct)) { "n/a" } else { $sumNetPct.ToString("+0.00%;-0.00%;+0.00%", $Culture) }
    $netPlusRebPlain = ("{0}`${1} ({2})" -f $netSign, [Math]::Abs($sumNetPlusReb).ToString("0.00", $Culture), $netPctStr)
    $rebateSign = if ($sumRebate -ge 0.0) { "+" } else { "-" }
    $rebatePlain = ("{0}`${1}" -f $rebateSign, [Math]::Abs($sumRebate).ToString("0.00", $Culture))
    $feesPlain = ("-`$" + [Math]::Abs($sumFeesPaid).ToString("0.00", $Culture))

    $resultObj = [PSCustomObject]@{
        ConfigIndex    = $Spec.Index
        ConfigId       = ("#{0}/{1}" -f $Spec.Index, $Spec.Total)
        Gamma          = $Spec.GammaStr
        VarSpr         = $Spec.VarStr
        MinT           = $Spec.MinTicks
        XTick          = $Spec.CrossLossDisplay
        MaxP           = $Spec.EntryProb
        MaxPx          = $Spec.EntryPriceDisplay
        LLTh           = $Spec.LLThreshDisplay
        LLWin          = $Spec.LLWin
        LLCd           = $Spec.LLCd
        LLDir          = $Spec.LLDir
        HSkew          = $Spec.HedgeSkewStr
        T1Cap          = $Spec.CapT1
        T1Trig         = $Spec.T1TrigDisplay
        EntryDelay     = $Spec.EntryDelayDisplay
        EntryCut       = $Spec.EntryCutDisplay
        ElapsedSec     = $CfgElapsedSec
        Time           = ("{0:F2}s" -f $CfgElapsedSec)
        QuantityNum    = $sumQty
        FillsNum       = $sumFills
        Quantity       = $qtyPlain
        PairsNum       = $sumPairs
        Pairs          = $sumPairs.ToString("F2", $Culture)
        VolumeInNum    = $sumVolIn
        VolumeIn       = ("`$" + $sumVolIn.ToString("F2", $Culture))
        CloseVolumeNum = $sumCloseVol
        CloseVol       = ("`$" + $sumCloseVol.ToString("F2", $Culture))
        GrossPnlNum    = $sumGrossPnl
        GrossPnl       = $grossPlain
        GrossPctNum    = $sumGrossPct
        NetPnLNum      = $sumNetPnl
        Net0Plain      = $net0Plain
        Net0PctNum     = $sumNet0Pct
        RebateNum      = $sumRebate
        Rebate         = $rebatePlain
        FeesNum        = $sumFeesPaid
        Fees           = $feesPlain
        NetPlusRebNum  = $sumNetPlusReb
        NetPlusReb     = $netPlusRebPlain
        NetPctNum      = $sumNetPct
        WinRatePct     = $winRatePct
        WinRate        = if ([double]::IsNaN($winRatePct)) { 'n/a' } else { ("{0:F0}%" -f $winRatePct) }
        WinWonNum      = $sumWinWon
        WinTotalNum    = $sumWinTotal
        ISNum          = $isNet
        OOSNum         = $oosNet
        RetainNum      = $retainPct
        MaxDDNum       = $maxDDPct
        DragNum        = $dragPct
        TailDragNum    = $tailDragPct
        NbrNum         = [double]::NaN
        G1             = "N/A"
        G2             = "N/A"
        G3             = "N/A"
        G4             = "N/A"
        G5             = "N/A"
        G6             = "N/A"
        Verdict        = "N/A"
        ISNum3         = $surv3.IS
        OOSNum3        = $surv3.OOS
        RetainNum3     = $surv3.Retain
        MaxDDNum3      = $surv3.MaxDD
        NbrNum3        = [double]::NaN
        G1_3           = "N/A"
        G2_3           = "N/A"
        G3_3           = "N/A"
        G4_3           = "N/A"
        G5_3           = "N/A"
        G6_3           = "N/A"
        Verdict3       = "N/A"
        ISWindows      = $isWinCount
        OOSWindows     = $oosWinCount
        SettleNum      = $sumSettlement
    }

    return [PSCustomObject]@{
        Result      = $resultObj
        FooterLines = $footerLines
    }
}

# ---------------------------------------------------------------------------
# Parameter Parsing & Validation
# ---------------------------------------------------------------------------
if ($Workers -lt 1) {
    throw "Workers must be >= 1 (got $Workers)"
}
if (-not [string]::IsNullOrWhiteSpace($OutputDir) -and -not [string]::IsNullOrWhiteSpace($ResumeDir)) {
    throw "Pass at most one of -OutputDir or -ResumeDir"
}
$inv = [System.Globalization.CultureInfo]::InvariantCulture
$defaultsPath = Join-Path (Split-Path -Parent $PSScriptRoot) "DEFAULTS.json"
if (-not (Test-Path -LiteralPath $defaultsPath -PathType Leaf)) {
    throw "Required DEFAULTS.json not found at $defaultsPath"
}
$rootDefaults = Get-Content -LiteralPath $defaultsPath -Raw | ConvertFrom-Json
$engDef = $rootDefaults.engine
$simDef = $rootDefaults.simulation

$defaultGammaStr = ([double]$engDef.gamma).ToString("R", $inv)
$defaultVarStr = ([bool]$engDef.include_variance_spread).ToString().ToLowerInvariant()
$defaultMinTicksStr = ([uint64]$engDef.min_spread_ticks).ToString($inv)
$defaultCrossLossStr = if ($null -eq $engDef.max_cross_tick_loss_micro) { "off" } else { ([uint64]$engDef.max_cross_tick_loss_micro).ToString($inv) }
$defaultEntryProbStr = if ($null -eq $engDef.max_entry_prob) { "off" } else { ([double]$engDef.max_entry_prob).ToString("R", $inv) }
$defaultEntryPriceStr = if ($null -eq $engDef.max_entry_price) { "off" } else { ([double]$engDef.max_entry_price).ToString("R", $inv) }
$defaultEntryPriceNum = if ($null -eq $engDef.max_entry_price) { [double]::NaN } else { [double]$engDef.max_entry_price }
$defaultLLThreshNum = [double]$engDef.lead_lag_cancel_threshold
$defaultLLThreshStr = $defaultLLThreshNum.ToString("R", $inv)
$defaultLLWinNum = [uint64]$engDef.lead_lag_window_ms
$defaultLLWinStr = $defaultLLWinNum.ToString($inv)
$defaultLLCdNum = [uint64]$engDef.lead_lag_cooldown_ms
$defaultLLCdStr = $defaultLLCdNum.ToString($inv)
$defaultLLDirStr = ([bool]$engDef.lead_lag_directional).ToString().ToLowerInvariant()
$defaultHedgeSkewNum = [double]$engDef.hedge_skew_mult
$defaultHedgeSkewStr = $defaultHedgeSkewNum.ToString("R", $inv)
$defaultCapTier1Str = ([bool]$engDef.cap_tier1_reducing_to_delta).ToString().ToLowerInvariant()
$t1TrigProp = $engDef.PSObject.Properties["tier1_hedge_trigger_price"]
$defaultT1TrigStr = if ($null -eq $t1TrigProp -or $null -eq $t1TrigProp.Value) { "off" } else { ([double]$t1TrigProp.Value).ToString("R", $inv) }
$delayProp = $engDef.PSObject.Properties["entry_open_delay_secs"]
$defaultEntryDelayNum = if ($null -eq $delayProp -or $null -eq $delayProp.Value) { [uint64]0 } else { [uint64]$delayProp.Value }
$defaultEntryDelayStr = $defaultEntryDelayNum.ToString($inv)
$cutoffProp = $engDef.PSObject.Properties["entry_cutoff_secs"]
$defaultEntryCutoffStr = if ($null -eq $cutoffProp -or $null -eq $cutoffProp.Value) { "off" } else { ([uint64]$cutoffProp.Value).ToString($inv) }

if ($Gammas.Count -eq 0) { $Gammas = @($defaultGammaStr) }
if ($VarianceSpreads.Count -eq 0) { $VarianceSpreads = @($defaultVarStr) }
if ($MinSpreadTicks.Count -eq 0) { $MinSpreadTicks = @($defaultMinTicksStr) }
if ($MaxCrossTickLossMicros.Count -eq 0) { $MaxCrossTickLossMicros = @($defaultCrossLossStr) }
if ($MaxEntryProbs.Count -eq 0) { $MaxEntryProbs = @($defaultEntryProbStr) }
if ($MaxEntryPrices.Count -eq 0) { $MaxEntryPrices = @($defaultEntryPriceStr) }
if ($LeadLagThresholds.Count -eq 0) { $LeadLagThresholds = @($defaultLLThreshStr) }
if ($LeadLagWindows.Count -eq 0) { $LeadLagWindows = @($defaultLLWinStr) }
if ($LeadLagCooldowns.Count -eq 0) { $LeadLagCooldowns = @($defaultLLCdStr) }
if ($LeadLagDirectionals.Count -eq 0) { $LeadLagDirectionals = @($defaultLLDirStr) }
if ($HedgeSkewMults.Count -eq 0) { $HedgeSkewMults = @($defaultHedgeSkewStr) }
if ($CapTier1Hedges.Count -eq 0) { $CapTier1Hedges = @($defaultCapTier1Str) }
if ($Tier1HedgeTriggers.Count -eq 0) { $Tier1HedgeTriggers = @($defaultT1TrigStr) }
if ($EntryOpenDelays.Count -eq 0) { $EntryOpenDelays = @($defaultEntryDelayStr) }
if ($EntryCutoffs.Count -eq 0) { $EntryCutoffs = @($defaultEntryCutoffStr) }
if ($null -eq $MaxCombinedBidMicro) { [uint64]$MaxCombinedBidMicro = [uint64]$engDef.max_combined_bid_micro } else { [uint64]$MaxCombinedBidMicro = [uint64]$MaxCombinedBidMicro }
if ([string]::IsNullOrWhiteSpace($QueueDecay)) { $QueueDecay = ([string]$simDef.queue_decay).ToLowerInvariant() }
if ($QueueDecay -notin @("conservative", "optimistic")) { throw "Invalid QueueDecay '$QueueDecay' (expected conservative or optimistic)" }
if ($null -eq $VMax) { [uint64]$VMax = [uint64]([Math]::Floor([double]$engDef.max_notional_per_leg_micro / 1e6)) } else { [uint64]$VMax = [uint64]$VMax }
if ($null -eq $BaseSize) { [uint64]$BaseSize = [uint64]([Math]::Floor([double]$engDef.base_order_size_shares / 1e6)) } else { [uint64]$BaseSize = [uint64]$BaseSize }
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
$parsedT1Triggers = @($Tier1HedgeTriggers | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    if ($_.Equals("off", [System.StringComparison]::OrdinalIgnoreCase) -or $_.Equals("none", [System.StringComparison]::OrdinalIgnoreCase)) {
        "off"
    } else {
        $p = [double]::Parse($_, $inv)
        if ([double]::IsNaN($p) -or [double]::IsInfinity($p) -or $p -le 0.0 -or $p -ge 1.0) {
            throw "Invalid Tier1HedgeTriggers entry '$_' (expected 'off' or finite number in (0.0, 1.0))"
        }
        $p.ToString("R", $inv)
    }
})
$parsedEntryDelays = @($EntryOpenDelays | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    $d = [uint64]::Parse($_, $inv)
    if ($d -gt 240) {
        throw "Invalid EntryOpenDelays entry '$_' (expected 0..=240)"
    }
    $d
})
$parsedEntryCutoffs = @($EntryCutoffs | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' } | ForEach-Object {
    if ($_.Equals("off", [System.StringComparison]::OrdinalIgnoreCase) -or $_.Equals("none", [System.StringComparison]::OrdinalIgnoreCase)) {
        "off"
    } else {
        $c = [uint64]::Parse($_, $inv)
        if ($c -eq 0 -or $c -gt 300) {
            throw "Invalid EntryCutoffs entry '$_' (expected 'off' or 1..=300)"
        }
        $c.ToString($inv)
    }
})
if ($parsedT1Triggers.Count -eq 0 -or $parsedEntryDelays.Count -eq 0 -or $parsedEntryCutoffs.Count -eq 0) {
    throw "Tier1HedgeTriggers, EntryOpenDelays, and EntryCutoffs must each contain at least one value"
}
if ((@($parsedT1Triggers | Where-Object { $_ -ne "off" }).Count -gt 0) -and (@($parsedCapTier1 | Where-Object { $_ -eq "false" }).Count -gt 0)) {
    throw "Non-'off' Tier1HedgeTriggers requires CapTier1Hedges=true"
}
foreach ($d in $parsedEntryDelays) {
    foreach ($c in $parsedEntryCutoffs) {
        if ($c -ne "off" -and $d -ge [uint64]$c) {
            throw "EntryOpenDelays ($d) must be strictly less than EntryCutoffs ($c)"
        }
    }
}

$showCrossLoss = @($parsedCrossLosses | Where-Object { $_ -ne "off" }).Count -gt 0
$showEntryProb = @($parsedMaxEntryProbs | Where-Object { $_ -ne "off" }).Count -gt 0
$showEntryPrice = if ([double]::IsNaN($defaultEntryPriceNum)) {
    @($parsedMaxEntryPrices | Where-Object { $_ -ne "off" }).Count -gt 0
} else {
    Test-EntryPriceSwept -Prices $parsedMaxEntryPrices -Culture $inv -DefaultEntryPrice $defaultEntryPriceNum
}
$showLLThresh = ($parsedLLThresholds.Count -gt 1) -or (@($parsedLLThresholds | Where-Object { [double]::Parse($_, $inv) -ne $defaultLLThreshNum }).Count -gt 0)
$showLLWin = ($parsedLLWindows.Count -gt 1) -or (@($parsedLLWindows | Where-Object { $_ -ne $defaultLLWinNum }).Count -gt 0)
$showLLCd = ($parsedLLCooldowns.Count -gt 1) -or (@($parsedLLCooldowns | Where-Object { $_ -ne $defaultLLCdNum }).Count -gt 0)
$showLLDir = ($parsedLLDirectionals.Count -gt 1) -or (@($parsedLLDirectionals | Where-Object { $_ -ne $defaultLLDirStr }).Count -gt 0)
$showHedgeSkew = ($parsedHedgeSkews.Count -gt 1) -or (@($parsedHedgeSkews | Where-Object { $_ -ne $defaultHedgeSkewNum }).Count -gt 0)
$showCapTier1 = ($parsedCapTier1.Count -gt 1) -or (@($parsedCapTier1 | Where-Object { $_ -ne $defaultCapTier1Str }).Count -gt 0)
$showT1Trigger = ($parsedT1Triggers.Count -gt 1) -or (@($parsedT1Triggers | Where-Object { $_ -ne $defaultT1TrigStr }).Count -gt 0)
$showEntryDelay = ($parsedEntryDelays.Count -gt 1) -or (@($parsedEntryDelays | Where-Object { $_ -ne $defaultEntryDelayNum }).Count -gt 0)
$showEntryCutoff = ($parsedEntryCutoffs.Count -gt 1) -or (@($parsedEntryCutoffs | Where-Object { $_ -ne $defaultEntryCutoffStr }).Count -gt 0)

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

# -InputPath, -Latest, -AllCaptures and -Quick all choose the input set: at most one may be given.
$inputSelectors = @()
if ($parsedInputPaths.Count -gt 0) { $inputSelectors += "-InputPath" }
if ($Latest) { $inputSelectors += "-Latest" }
if ($AllCaptures) { $inputSelectors += "-AllCaptures" }
if ($Quick) { $inputSelectors += "-Quick" }
if ($inputSelectors.Count -gt 1) {
    throw "Pass at most one input selector (got $($inputSelectors -join ' + '))"
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
$origPwd = (Get-Location).Path
Push-Location $repoRoot
# logs/ is gitignored and never shared by worktrees: when this script is run from another
# checkout (e.g. a worktree script against the main repo's captures), fall back to the
# caller's logs directory so -AllCaptures / -Latest / sweep output resolve where the data is.
$logsRoot = Join-Path $repoRoot "logs"
if (-not (Test-Path -LiteralPath (Join-Path $logsRoot "captures"))) {
    $callerLogs = Join-Path $origPwd "logs"
    if (Test-Path -LiteralPath (Join-Path $callerLogs "captures")) { $logsRoot = $callerLogs }
}
try {
    if ($resolvedInputs.Count -eq 0) {
        $capturesDir = Join-Path $logsRoot "captures"
        if (-not (Test-Path -LiteralPath $capturesDir)) {
            throw "Captures directory not found: $capturesDir (pass -InputPath or run poly-as feeds --discover)"
        }
        if ($Quick -or $AllCaptures) {
            # -Quick reuses the -AllCaptures pool but keeps only the two shortest captures,
            # floored at 5 windows so the IS/OOS split stays meaningful (a trailing partial
            # window emits no per-window result); -MinWindows still wins if it is stricter.
            $selectMinWindows = if ($Quick) { [Math]::Max($MinWindows, 5) } else { $MinWindows }
            $matchedFiles = @(Get-ChildItem -LiteralPath $capturesDir -Filter "*.jsonl" -File |
                Where-Object {
                    -not $_.Name.EndsWith(".inprogress.jsonl", [System.StringComparison]::OrdinalIgnoreCase) -and
                    $_.Name -match '_(\d+)w\.jsonl$' -and
                    [int]$Matches[1] -ge $selectMinWindows
                } |
                Sort-Object -Property LastWriteTimeUtc)
            if ($Quick) {
                # Re-sort the shortest pair into time order so the IS/OOS walk-forward stays chronological.
                $matchedFiles = @($matchedFiles |
                    Sort-Object -Property @{ Expression = { [int]($_.Name -replace '^.*_(\d+)w\.jsonl$', '$1') } }, LastWriteTimeUtc |
                    Select-Object -First 2 |
                    Sort-Object -Property LastWriteTimeUtc)
                Write-Host ("⚡ [QUICK] Captures: {0}" -f (($matchedFiles | ForEach-Object { $_.Name }) -join ", "))
            }
            if ($matchedFiles.Count -eq 0) {
                throw "No finalized .jsonl captures matching _<N>w.jsonl with N >= $selectMinWindows found in $capturesDir"
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
        if (-not [System.IO.Path]::IsPathRooted($binPath)) { $binPath = Join-Path $origPwd $binPath }
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

    $totalRuns = $parsedGammas.Count * $parsedVarSpreads.Count * $parsedMinTicks.Count * $parsedCrossLosses.Count * $parsedMaxEntryProbs.Count * $parsedMaxEntryPrices.Count * $parsedLLThresholds.Count * $parsedLLWindows.Count * $parsedLLCooldowns.Count * $parsedLLDirectionals.Count * $parsedHedgeSkews.Count * $parsedCapTier1.Count * $parsedT1Triggers.Count * $parsedEntryDelays.Count * $parsedEntryCutoffs.Count

    if (-not [string]::IsNullOrWhiteSpace($ResumeDir)) {
        $candidateResume = if ([System.IO.Path]::IsPathRooted($ResumeDir)) { $ResumeDir } else { Join-Path $origPwd $ResumeDir }
        if (-not (Test-Path -LiteralPath $candidateResume -PathType Container)) {
            throw "Resume directory not found: $candidateResume"
        }
        $sweepDir = (Resolve-Path -LiteralPath $candidateResume).Path
    } elseif (-not [string]::IsNullOrWhiteSpace($OutputDir)) {
        $sweepDir = Resolve-NewSweepOutputDir -OutputDir $OutputDir -OrigPwd $origPwd
    } else {
        $stamp = [DateTime]::UtcNow.ToString("dd-MM_HH-mm-ssZ", $inv)
        $sweepBase = Join-Path $logsRoot "sweeps"
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
    # Flatten Parameter Grid Into Deterministic Config Specs
    # -----------------------------------------------------------------------
    $maxPairFormatted = ($MaxCombinedBidMicro / 1e6).ToString("0.00", $inv)
    $configSpecs = [System.Collections.Generic.List[PSCustomObject]]::new()
    $seenConfigStems = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
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
                                                foreach ($t1Trig in $parsedT1Triggers) {
                                                    foreach ($entryDelay in $parsedEntryDelays) {
                                                        foreach ($entryCutoff in $parsedEntryCutoffs) {
                                                $runIdx++
                                                $crossLossDisplay  = Format-DollarParam -RawVal $crossLoss -Scale 1e6 -MinDecimals 2 -Culture $inv
                                                $entryPriceDisplay = Format-DollarParam -RawVal $entryPrice -Scale 1.0 -MinDecimals 2 -Culture $inv
                                                $llThreshDisplay   = Format-DollarParam -RawVal $llThresh -Scale 1.0 -MinDecimals 2 -Culture $inv
                                                $t1TrigDisplay     = Format-DollarParam -RawVal $t1Trig -Scale 1.0 -MinDecimals 2 -Culture $inv
                                                $entryDelayDisplay = ("{0}s" -f $entryDelay)
                                                $entryCutDisplay   = if ($entryCutoff -eq "off") { "off" } else { ("{0}s" -f $entryCutoff) }

                                                $currParamMap = @{
                                                    "V Max"       = "`$$VMax"
                                                    "Base Size"   = "${BaseSize}sh"
                                                    "Max Pair"    = "`$$maxPairFormatted"
                                                    "Queue Decay" = $QueueDecay
                                                    "Gamma"       = $gammaStr
                                                    "Variance"    = $varStr
                                                    "Min Ticks"   = $minTicks.ToString($inv)
                                                    "Cross Loss"  = $crossLossDisplay
                                                    "Entry Prob"  = $entryProb
                                                    "Entry Price" = $entryPriceDisplay
                                                    "Threshold"   = $llThreshDisplay
                                                    "Window Ms"   = $llWin.ToString($inv)
                                                    "Cooldown Ms" = $llCd.ToString($inv)
                                                    "Direction"   = $llDir
                                                    "Hedge Skew"  = $hsStr
                                                    "Tier 1 Cap"  = $capT1
                                                    "T1 Trigger"  = $t1TrigDisplay
                                                    "Entry Delay" = $entryDelayDisplay
                                                    "Entry Cut"   = $entryCutDisplay
                                                }

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
                                                if ($showT1Trigger)  { $stem += "_t1t-$($t1Trig.Replace('.', 'p'))" }
                                                if ($showEntryDelay) { $stem += "_ed-${entryDelay}" }
                                                if ($showEntryCutoff){ $stem += "_ec-${entryCutoff}" }

                                                if (-not $seenConfigStems.Add($stem)) {
                                                    throw "Duplicate config stem generated: $stem"
                                                }

                                                $envOverrides = @{
                                                    "MAX_CROSS_TICK_LOSS_MICRO"   = $crossLoss
                                                    "MAX_ENTRY_PROB"              = $entryProb
                                                    "MAX_ENTRY_PRICE"             = $entryPrice
                                                    "LEAD_LAG_CANCEL_THRESHOLD"   = $llThresh
                                                    "LEAD_LAG_WINDOW_MS"          = $llWin.ToString($inv)
                                                    "LEAD_LAG_COOLDOWN_MS"        = $llCd.ToString($inv)
                                                    "LEAD_LAG_DIRECTIONAL"        = $llDir
                                                    "HEDGE_SKEW_MULT"             = $hsStr
                                                    "CAP_TIER1_REDUCING_TO_DELTA" = $capT1
                                                    "TIER1_HEDGE_TRIGGER_PRICE"   = $t1Trig
                                                    "ENTRY_OPEN_DELAY_SECS"       = $entryDelay.ToString($inv)
                                                    "ENTRY_CUTOFF_SECS"           = $entryCutoff
                                                }

                                                $configSpecs.Add([PSCustomObject]@{
                                                    Index             = $runIdx
                                                    Total             = $totalRuns
                                                    Stem              = $stem
                                                    GammaStr          = $gammaStr
                                                    VarStr            = $varStr
                                                    MinTicks          = $minTicks
                                                    CrossLoss         = $crossLoss
                                                    CrossLossDisplay  = $crossLossDisplay
                                                    EntryProb         = $entryProb
                                                    EntryPrice        = $entryPrice
                                                    EntryPriceDisplay = $entryPriceDisplay
                                                    LLThresh          = $llThresh
                                                    LLThreshDisplay   = $llThreshDisplay
                                                    LLWin             = $llWin
                                                    LLCd              = $llCd
                                                    LLDir             = $llDir
                                                    HedgeSkewStr      = $hsStr
                                                    CapT1             = $capT1
                                                    T1Trig            = $t1Trig
                                                    T1TrigDisplay     = $t1TrigDisplay
                                                    EntryDelay        = $entryDelay
                                                    EntryDelayDisplay = $entryDelayDisplay
                                                    EntryCutoff       = $entryCutoff
                                                    EntryCutDisplay   = $entryCutDisplay
                                                    CurrParamMap      = $currParamMap
                                                    PrevParamMap      = $prevParamMap
                                                    EnvOverrides      = $envOverrides
                                                })
                                                $prevParamMap = $currParamMap
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
            }
        }
    }

    # -----------------------------------------------------------------------
    # Sweep Input Manifest (guards -ResumeDir against mismatched inputs)
    # -----------------------------------------------------------------------
    $axisNames = @(
        "gamma", "var_spreads", "min_ticks", "cross_losses", "max_entry_probs", "max_entry_prices",
        "ll_thresholds", "ll_windows", "ll_cooldowns", "ll_directionals", "hedge_skews",
        "cap_tier1", "t1_triggers", "entry_delays", "entry_cutoffs"
    )
    $axisValues = @(
        $parsedGammas, $parsedVarSpreads, $parsedMinTicks, $parsedCrossLosses, $parsedMaxEntryProbs,
        $parsedMaxEntryPrices, $parsedLLThresholds, $parsedLLWindows, $parsedLLCooldowns,
        $parsedLLDirectionals, $parsedHedgeSkews, $parsedCapTier1, $parsedT1Triggers,
        $parsedEntryDelays, $parsedEntryCutoffs
    )
    $axisMap = [ordered]@{}
    for ($ai = 0; $ai -lt $axisNames.Count; $ai++) {
        $axisMap[$axisNames[$ai]] = @($axisValues[$ai] | ForEach-Object { [string]$_ })
    }
    $manifest = [ordered]@{
        base_size     = [int]$BaseSize
        v_max         = [double]$VMax
        queue_decay   = [string]$QueueDecay
        capture_stems = @($captures | ForEach-Object { [string]$_.Stem })
        axes          = $axisMap
    }
    $manifestPath = Join-Path $sweepDir "sweep-manifest.json"
    if (Test-Path -LiteralPath $manifestPath -PathType Leaf) {
        if (-not [string]::IsNullOrWhiteSpace($ResumeDir)) {
            $priorManifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
            $inputMismatch = [System.Collections.Generic.List[string]]::new()
            if ([int]$priorManifest.base_size -ne [int]$BaseSize) { $inputMismatch.Add(("- base_size: prior={0} now={1}" -f $priorManifest.base_size, $BaseSize)) }
            if ([Math]::Abs([double]$priorManifest.v_max - [double]$VMax) -gt 1e-9) { $inputMismatch.Add(("- v_max: prior={0} now={1}" -f $priorManifest.v_max, $VMax)) }
            if ([string]$priorManifest.queue_decay -ne [string]$QueueDecay) { $inputMismatch.Add(("- queue_decay: prior={0} now={1}" -f $priorManifest.queue_decay, $QueueDecay)) }
            if ((@($priorManifest.capture_stems) -join ",") -ne (@($manifest.capture_stems) -join ",")) {
                $inputMismatch.Add(("- captures: prior={0} now={1}" -f (@($priorManifest.capture_stems) -join ","), (@($manifest.capture_stems) -join ",")))
            }
            foreach ($an in $axisNames) {
                $priorProp = $priorManifest.axes.PSObject.Properties[$an]
                $priorVals = if ($null -ne $priorProp) { @($priorProp.Value | ForEach-Object { [string]$_ }) -join "," } else { "" }
                $nowVals = @($manifest.axes[$an] | ForEach-Object { [string]$_ }) -join ","
                if ($priorVals -ne $nowVals) { $inputMismatch.Add(("- axis {0}: prior={1} now={2}" -f $an, $priorVals, $nowVals)) }
            }
            if ($inputMismatch.Count -gt 0) {
                throw ("Resume safety abort — sweep inputs differ from the original run (write to a new directory instead):`n" + ($inputMismatch -join "`n"))
            }
        }
    } else {
        ($manifest | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $manifestPath -Encoding UTF8
    }

    # -----------------------------------------------------------------------
    # Inspect Resume State (Before Launching Any Worker)
    # -----------------------------------------------------------------------
    $resumedSpecs = [System.Collections.Generic.List[PSCustomObject]]::new()
    $pendingSpecs = [System.Collections.Generic.List[PSCustomObject]]::new()
    $partialErrors = [System.Collections.Generic.List[string]]::new()
    $earliestRunTime = $null

    if (-not [string]::IsNullOrWhiteSpace($ResumeDir)) {
        foreach ($spec in $configSpecs) {
            $disk = Get-ConfigDiskStatus -SweepDir $sweepDir -Captures $captures -IsMultiCapture $isMultiCapture -Stem $spec.Stem
            if ($disk.Status -eq "Complete") {
                $cfgStart = $null
                foreach ($lp in $disk.ExistingLogs) {
                    $ct = (Get-Item -LiteralPath $lp).CreationTime
                    if ($null -eq $cfgStart -or $ct -lt $cfgStart) { $cfgStart = $ct }
                    if ($null -eq $earliestRunTime -or $ct -lt $earliestRunTime) { $earliestRunTime = $ct }
                }
                $resumedSpecs.Add([PSCustomObject]@{
                    Spec          = $spec
                    StartTimeText = if ($null -ne $cfgStart) { $cfgStart.ToString("HH:mm", $inv) } else { [DateTime]::Now.ToString("HH:mm", $inv) }
                })
            } elseif ($disk.Status -eq "Pending") {
                $pendingSpecs.Add($spec)
            } else {
                $partialErrors.Add(("Config #{0}/{1} ({2}): valid={3}/{4}, jsonl={5}, log={6}, missing/incomplete={7}" -f `
                    $spec.Index, $spec.Total, $spec.Stem, $disk.ValidSummaryCount, $captures.Count, $disk.ExistingJsonlCount, $disk.ExistingLogCount, ($disk.MissingOrIncomplete -join ", ")))
            }
        }
        if ($partialErrors.Count -gt 0) {
            throw ("Resume safety abort — partial config state detected (will not overwrite or duplicate):`n" + ($partialErrors -join "`n"))
        }
    } else {
        foreach ($spec in $configSpecs) {
            $pendingSpecs.Add($spec)
        }
    }

    # -----------------------------------------------------------------------
    # Phase 2: Top-Level Run Header & Tabular Streaming / Parallel Loop
    # -----------------------------------------------------------------------
    $sweepStartedStamp = if ($null -ne $earliestRunTime) {
        $earliestRunTime.ToString("HH:mm", $inv)
    } else {
        [DateTime]::Now.ToString("HH:mm", $inv)
    }
    $majorRule = Format-ColorText -Text ("=" * 100) -Code $AnsiGray
    $sweepTitle = if ($isMultiCapture) {
        "🚀 OFFLINE MULTI-CAPTURE PARAMETER SWEEP ({0} config sets)"
    } else {
        "🚀 OFFLINE PARAMETER SWEEP ({0} config sets)"
    }
    Write-Host ""
    Write-Host $majorRule
    Write-Host (Format-ColorText -Text ($sweepTitle -f $totalRuns) -Code $AnsiBoldCyan)
    Write-Host ("Started  : {0}" -f $sweepStartedStamp)
    Write-Host ("Captures : {0} file(s) ({1} windows total)" -f $captures.Count, $totalWindows)
    Write-Host ("Output   : {0}" -f $sweepDir)
    if (-not [string]::IsNullOrWhiteSpace($ResumeDir)) {
        Write-Host ("Resumed  : {0} complete config(s) preserved | {1} pending config(s) on {2} worker(s)" -f $resumedSpecs.Count, $pendingSpecs.Count, $Workers)
    } elseif ($Workers -gt 1) {
        Write-Host ("Workers  : {0} parallel worker(s)" -f $Workers)
    }
    Write-Host $majorRule

    $winColW = [Math]::Max(12, ("SUBTOTAL {0}w" -f $totalWindows).Length)
    $tblMeta = Format-CaptureTableHeaderLines -WinColW $winColW
    $tblHdrLine     = $tblMeta[0]
    $tblDashColored = $tblMeta[1]
    $tblDash        = $tblMeta[2]
    $tblSep         = $tblMeta[3]

    $sweepStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $replayStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $resultsByIndex = @{}
    $stoppedEarly = $false

    foreach ($rItem in $resumedSpecs) {
        $spec = $rItem.Spec
        Write-ConfigBanner -Index $spec.Index -Total $spec.Total -Curr $spec.CurrParamMap -Prev $spec.PrevParamMap -StartTimeText $rItem.StartTimeText
        Write-HostHud $tblHdrLine
        Write-HostHud $tblDashColored

        $capRuns = [System.Collections.Generic.List[PSCustomObject]]::new()
        $minCt = $null
        $maxWt = $null
        for ($capIdx = 0; $capIdx -lt $captures.Count; $capIdx++) {
            $cap = $captures[$capIdx]
            $outJsonl = if ($isMultiCapture) {
                Join-Path (Join-Path $sweepDir $cap.Stem) "$($spec.Stem).jsonl"
            } else {
                Join-Path $sweepDir "$($spec.Stem).jsonl"
            }
            $fi = Get-Item -LiteralPath $outJsonl
            if ($null -eq $minCt -or $fi.CreationTime -lt $minCt) { $minCt = $fi.CreationTime }
            if ($null -eq $maxWt -or $fi.LastWriteTime -gt $maxWt) { $maxWt = $fi.LastWriteTime }

            $parsedCap = Read-CaptureJsonlMetrics -OutJsonl $outJsonl -Cap $cap -CapIdx $capIdx -BaseSize $BaseSize -MaxCombinedBidMicro $MaxCombinedBidMicro -WinColW $winColW -TblSep $tblSep -Culture $inv
            Write-HostHud $parsedCap.RowText
            $capRuns.Add($parsedCap.CapRun)
        }
        $elapsed = if ($null -ne $minCt -and $null -ne $maxWt) { [Math]::Max(0.0, ($maxWt - $minCt).TotalSeconds) } else { 0.0 }
        $agg = Complete-ConfigRunAggregation -Spec $spec -CapRuns $capRuns -CfgElapsedSec $elapsed -TotalWindows $totalWindows -IsMultiCapture $isMultiCapture -CaptureCount $captures.Count -WinColW $winColW -TblSep $tblSep -TblDash $tblDash -AllocatedCapital ([double]$VMax) -Culture $inv
        foreach ($fl in $agg.FooterLines) { Write-HostHud $fl }
        $resultsByIndex[$spec.Index] = $agg.Result
    }

    $stopSentinelPath = Join-Path $sweepDir ".stop"

    if ($pendingSpecs.Count -gt 0) {
        if ($Workers -le 1) {
            foreach ($spec in $pendingSpecs) {
                if (Test-Path -LiteralPath $stopSentinelPath -PathType Leaf) {
                    Write-HostHud (Format-ColorText -Text ("⏹️  [STOP] Sentinel detected at {0}; halting before CONFIG #{1}/{2}." -f $stopSentinelPath, $spec.Index, $spec.Total) -Code $AnsiBoldYellow)
                    $stoppedEarly = $true
                    break
                }

                $cfgStartStamp = [DateTime]::Now.ToString("HH:mm", $inv)
                Update-BottomRightHud -ConfigIdx $spec.Index -ConfigTotal $spec.Total -FileIdx 1 -FileTotal $captures.Count
                Write-ConfigBanner -Index $spec.Index -Total $spec.Total -Curr $spec.CurrParamMap -Prev $spec.PrevParamMap -StartTimeText $cfgStartStamp
                Write-HostHud $tblHdrLine
                Write-HostHud $tblDashColored

                $cfgStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
                $capRuns = [System.Collections.Generic.List[PSCustomObject]]::new()

                for ($capIdx = 0; $capIdx -lt $captures.Count; $capIdx++) {
                    Update-BottomRightHud -ConfigIdx $spec.Index -ConfigTotal $spec.Total -FileIdx ($capIdx + 1) -FileTotal $captures.Count
                    $cap = $captures[$capIdx]
                    if ($isMultiCapture) {
                        $capOutDir = Join-Path $sweepDir $cap.Stem
                        $outJsonl = Join-Path $capOutDir "$($spec.Stem).jsonl"
                        $outLog = Join-Path $capOutDir "$($spec.Stem).log"
                    } else {
                        $outJsonl = Join-Path $sweepDir "$($spec.Stem).jsonl"
                        $outLog = Join-Path $sweepDir "$($spec.Stem).log"
                    }

                    $cliArgs = @(
                        "lifecycle",
                        "--source", "replay",
                        "--input", $cap.Path,
                        "--v-max", ([uint64]$VMax).ToString($inv),
                        "--base-size", ([uint64]$BaseSize).ToString($inv),
                        "--queue-decay", $QueueDecay,
                        "--gamma", $spec.GammaStr,
                        "--include-variance-spread", $spec.VarStr,
                        "--min-spread-ticks", $spec.MinTicks.ToString($inv),
                        "--max-combined-bid-micro", ([uint64]$MaxCombinedBidMicro).ToString($inv),
                        "--lead-lag-threshold", $spec.LLThresh,
                        "--lead-lag-window-ms", $spec.LLWin.ToString($inv),
                        "--lead-lag-cooldown-ms", $spec.LLCd.ToString($inv),
                        "--lead-lag-directional", $spec.LLDir,
                        "--hedge-skew-mult", $spec.HedgeSkewStr,
                        "--cap-tier1-hedge", $spec.CapT1,
                        "--tier1-hedge-trigger-price", $spec.T1Trig,
                        "--entry-open-delay-secs", $spec.EntryDelay.ToString($inv),
                        "--entry-cutoff-secs", $spec.EntryCutoff,
                        "--jsonl", $outJsonl,
                        "--max-cross-tick-loss-micro", $spec.CrossLoss,
                        "--max-entry-prob", $spec.EntryProb,
                        "--max-entry-price", $spec.EntryPrice
                    )

                    Invoke-CaptureReplayProcess -BinPath $binPath -CliArgs $cliArgs -EnvOverrides $spec.EnvOverrides -OutLog $outLog -WorkingDirectory $repoRoot -Label ("{0}/{1}" -f $cap.Stem, $spec.Stem)
                    $parsedCap = Read-CaptureJsonlMetrics -OutJsonl $outJsonl -Cap $cap -CapIdx $capIdx -BaseSize $BaseSize -MaxCombinedBidMicro $MaxCombinedBidMicro -WinColW $winColW -TblSep $tblSep -Culture $inv
                    Write-HostHud $parsedCap.RowText
                    $capRuns.Add($parsedCap.CapRun)
                }

                $cfgStopwatch.Stop()
                $agg = Complete-ConfigRunAggregation -Spec $spec -CapRuns $capRuns -CfgElapsedSec $cfgStopwatch.Elapsed.TotalSeconds -TotalWindows $totalWindows -IsMultiCapture $isMultiCapture -CaptureCount $captures.Count -WinColW $winColW -TblSep $tblSep -TblDash $tblDash -AllocatedCapital ([double]$VMax) -Culture $inv
                foreach ($fl in $agg.FooterLines) { Write-HostHud $fl }
                $resultsByIndex[$spec.Index] = $agg.Result
            }
        } else {
            $scriptSelfText = Get-Content -LiteralPath $PSCommandPath -Raw
            $hStart = $scriptSelfText.IndexOf("# Ensure UTF-8 console rendering")
            $hEnd = $scriptSelfText.IndexOf("# Parameter Parsing & Validation")
            if ($hStart -lt 0 -or $hEnd -le $hStart) {
                throw "Failed to extract helper block for parallel workers"
            }
            $helperBlockText = $scriptSelfText.Substring($hStart, $hEnd - $hStart)
            $consoleLock = [object]::new()
            $capturesArr = @($captures.ToArray())
            $effectiveWorkers = [Math]::Min($Workers, $pendingSpecs.Count)

            $workerOutputs = @($pendingSpecs | ForEach-Object -ThrottleLimit $effectiveWorkers -Parallel {
                Set-StrictMode -Version Latest
                $ErrorActionPreference = "Stop"

                $spec = $_
                $wInv = [System.Globalization.CultureInfo]::InvariantCulture
                Invoke-Expression $using:helperBlockText
                $UseAnsi = $using:UseAnsi

                $wSweepDir = $using:sweepDir
                $wStopPath = $using:stopSentinelPath
                $wLock = $using:consoleLock

                if (Test-Path -LiteralPath $wStopPath -PathType Leaf) {
                    [System.Threading.Monitor]::Enter($wLock)
                    try {
                        Write-Host (Format-ColorText -Text ("⏹️  [STOP] Sentinel detected; skipping CONFIG #{0} / {1}." -f $spec.Index, $spec.Total) -Code $AnsiBoldYellow)
                    } finally {
                        [System.Threading.Monitor]::Exit($wLock)
                    }
                    return [PSCustomObject]@{ SkippedByStop = $true; Index = $spec.Index; Result = $null }
                }

                [System.Threading.Monitor]::Enter($wLock)
                try {
                    $cfgStartStamp = [DateTime]::Now.ToString("HH:mm", $wInv)
                    Write-Host (Format-ColorText -Text ("[{0}] ▶ Starting CONFIG #{1} / {2} ({3})" -f $cfgStartStamp, $spec.Index, $spec.Total, $spec.Stem) -Code $AnsiCyan)
                } finally {
                    [System.Threading.Monitor]::Exit($wLock)
                }

                $wCaptures = $using:capturesArr
                $wIsMulti = $using:isMultiCapture
                $wTotalWin = $using:totalWindows
                $wWinColW = $using:winColW
                $wVMax = [uint64]$using:VMax
                $wBaseSize = [uint64]$using:BaseSize
                $wQueueDecay = $using:QueueDecay
                $wMaxBidMicro = [uint64]$using:MaxCombinedBidMicro
                $wBinPath = $using:binPath
                $wRepoRoot = $using:repoRoot

                $tblMetaLocal = Format-CaptureTableHeaderLines -WinColW $wWinColW
                $tblHdrLocal = $tblMetaLocal[0]
                $tblDashColoredLocal = $tblMetaLocal[1]
                $tblDashLocal = $tblMetaLocal[2]
                $tblSepLocal = $tblMetaLocal[3]

                $buffer = [System.Collections.Generic.List[string]]::new()
                foreach ($bLine in (Format-ConfigBannerLines -Index $spec.Index -Total $spec.Total -Curr $spec.CurrParamMap -Prev $spec.PrevParamMap -StartTimeText $cfgStartStamp)) {
                    $buffer.Add($bLine)
                }
                $buffer.Add($tblHdrLocal)
                $buffer.Add($tblDashColoredLocal)

                $cfgSw = [System.Diagnostics.Stopwatch]::StartNew()
                $capRuns = [System.Collections.Generic.List[PSCustomObject]]::new()

                for ($capIdx = 0; $capIdx -lt $wCaptures.Count; $capIdx++) {
                    $cap = $wCaptures[$capIdx]
                    if ($wIsMulti) {
                        $capOutDir = Join-Path $wSweepDir $cap.Stem
                        $outJsonl = Join-Path $capOutDir "$($spec.Stem).jsonl"
                        $outLog = Join-Path $capOutDir "$($spec.Stem).log"
                    } else {
                        $outJsonl = Join-Path $wSweepDir "$($spec.Stem).jsonl"
                        $outLog = Join-Path $wSweepDir "$($spec.Stem).log"
                    }

                    $cliArgs = @(
                        "lifecycle",
                        "--source", "replay",
                        "--input", $cap.Path,
                        "--v-max", $wVMax.ToString($wInv),
                        "--base-size", $wBaseSize.ToString($wInv),
                        "--queue-decay", $wQueueDecay,
                        "--gamma", $spec.GammaStr,
                        "--include-variance-spread", $spec.VarStr,
                        "--min-spread-ticks", $spec.MinTicks.ToString($wInv),
                        "--max-combined-bid-micro", $wMaxBidMicro.ToString($wInv),
                        "--lead-lag-threshold", $spec.LLThresh,
                        "--lead-lag-window-ms", $spec.LLWin.ToString($wInv),
                        "--lead-lag-cooldown-ms", $spec.LLCd.ToString($wInv),
                        "--lead-lag-directional", $spec.LLDir,
                        "--hedge-skew-mult", $spec.HedgeSkewStr,
                        "--cap-tier1-hedge", $spec.CapT1,
                        "--tier1-hedge-trigger-price", $spec.T1Trig,
                        "--entry-open-delay-secs", $spec.EntryDelay.ToString($wInv),
                        "--entry-cutoff-secs", $spec.EntryCutoff,
                        "--jsonl", $outJsonl,
                        "--max-cross-tick-loss-micro", $spec.CrossLoss,
                        "--max-entry-prob", $spec.EntryProb,
                        "--max-entry-price", $spec.EntryPrice
                    )

                    Invoke-CaptureReplayProcess -BinPath $wBinPath -CliArgs $cliArgs -EnvOverrides $spec.EnvOverrides -OutLog $outLog -WorkingDirectory $wRepoRoot -Label ("{0}/{1}" -f $cap.Stem, $spec.Stem)
                    $parsedCap = Read-CaptureJsonlMetrics -OutJsonl $outJsonl -Cap $cap -CapIdx $capIdx -BaseSize $wBaseSize -MaxCombinedBidMicro $wMaxBidMicro -WinColW $wWinColW -TblSep $tblSepLocal -Culture $wInv
                    $buffer.Add($parsedCap.RowText)
                    $capRuns.Add($parsedCap.CapRun)
                }

                $cfgSw.Stop()
                $agg = Complete-ConfigRunAggregation -Spec $spec -CapRuns $capRuns -CfgElapsedSec $cfgSw.Elapsed.TotalSeconds -TotalWindows $wTotalWin -IsMultiCapture $wIsMulti -CaptureCount $wCaptures.Count -WinColW $wWinColW -TblSep $tblSepLocal -TblDash $tblDashLocal -AllocatedCapital ([double]$wVMax) -Culture $wInv
                foreach ($fl in $agg.FooterLines) { $buffer.Add($fl) }

                [System.Threading.Monitor]::Enter($wLock)
                try {
                    foreach ($line in $buffer) { Write-Host $line }
                } finally {
                    [System.Threading.Monitor]::Exit($wLock)
                }

                return [PSCustomObject]@{
                    SkippedByStop = $false
                    Index         = $spec.Index
                    Result        = $agg.Result
                }
            })

            foreach ($wo in $workerOutputs) {
                if ($wo.SkippedByStop) {
                    $stoppedEarly = $true
                } elseif ($null -ne $wo.Result) {
                    $resultsByIndex[$wo.Index] = $wo.Result
                }
            }
        }
    }

    $replayStopwatch.Stop()
    Clear-BottomRightHud
    $sweepStopwatch.Stop()

    if ($stoppedEarly) {
        Write-Host ""
        Write-Host (Format-ColorText -Text ("⏹️  Sweep stopped at config boundary ({0}/{1} configs complete). Remove `"{2}\.stop`" and re-run with the original capture/sweep options plus -ResumeDir `"{2}`" to finish." -f $resultsByIndex.Count, $totalRuns, $sweepDir) -Code $AnsiBoldYellow)
        return
    }

    $results = [System.Collections.Generic.List[PSCustomObject]]::new()
    foreach ($spec in $configSpecs) {
        if (-not $resultsByIndex.ContainsKey($spec.Index)) {
            throw "Missing aggregated result for Config #$($spec.Index)/$($spec.Total) ($($spec.Stem))"
        }
        $results.Add($resultsByIndex[$spec.Index])
    }

    $totalElapsedSec = [Math]::Max(0.001, $sweepStopwatch.Elapsed.TotalSeconds)
    $avgConfigSec = $totalElapsedSec / [Math]::Max(1, $results.Count)
    # Throughput covers replay work this invocation actually ran, not re-reading saved results.
    $replayedConfigCount = [Math]::Max(0, $results.Count - $resumedSpecs.Count)
    $replayElapsedSec = [Math]::Max(0.0, $replayStopwatch.Elapsed.TotalSeconds)
    $throughputText = if ($totalWindows -le 0) { "n/a (windows unknown)" }
        elseif ($replayedConfigCount -eq 0) { "n/a (no replays; resumed results only)" }
        elseif ($replayElapsedSec -le 0.001) { "n/a (elapsed unmeasurable)" }
        else { "{0:F2} win/s" -f (($replayedConfigCount * $totalWindows) / $replayElapsedSec) }

    # -----------------------------------------------------------------------
    # Phase 3: Final Aggregation & Split Tables (Parameters vs. Performance Metrics)
    # -----------------------------------------------------------------------
    $sorted = @($results | Sort-Object -Property NetPnLNum -Descending)

    # -----------------------------------------------------------------------
    # Phase 3b: G1-G6 survival-gate evaluation (dual 0% floor & 3% Tier-1 paths)
    # -----------------------------------------------------------------------
    Update-SurvivalVerdicts -Sorted $sorted

    $cfgIdWidth = [Math]::Max(6, ("#{0}/{1}" -f $totalRuns, $totalRuns).Length)

    # --- Table A: Config Parameters Reference (All Parameters Shown) ---
    $paramCols = [System.Collections.Generic.List[hashtable]]::new()
    $paramCols.Add(@{ Name = "Rank";        Width = 4;           Prop = "Rank";     Kind = "rank" })
    $paramCols.Add(@{ Name = "Config";      Width = $cfgIdWidth; Prop = "ConfigId"; Kind = "cfg_id" })
    $paramCols.Add(@{ Name = "Gamma";       Width = -6;          Prop = "Gamma";    Kind = "param" })
    $paramCols.Add(@{ Name = "Variance";    Width = -8;          Prop = "VarSpr";   Kind = "param" })
    $paramCols.Add(@{ Name = "Min Ticks";   Width = 9;           Prop = "MinT";     Kind = "param" })
    $paramCols.Add(@{ Name = "Cross Loss";  Width = 10;          Prop = "XTick";    Kind = "param" })
    $paramCols.Add(@{ Name = "Entry Prob";  Width = 10;          Prop = "MaxP";     Kind = "param" })
    $paramCols.Add(@{ Name = "Entry Price"; Width = 11;          Prop = "MaxPx";    Kind = "param" })
    $paramCols.Add(@{ Name = "Threshold";   Width = 9;           Prop = "LLTh";     Kind = "param" })
    $paramCols.Add(@{ Name = "Window Ms";   Width = 9;           Prop = "LLWin";    Kind = "param" })
    $paramCols.Add(@{ Name = "Cooldown Ms"; Width = 11;          Prop = "LLCd";     Kind = "param" })
    $paramCols.Add(@{ Name = "Direction";   Width = 9;           Prop = "LLDir";    Kind = "param" })
    $paramCols.Add(@{ Name = "Hedge Skew";  Width = 10;          Prop = "HSkew";      Kind = "param" })
    $paramCols.Add(@{ Name = "Tier 1 Cap";  Width = 10;          Prop = "T1Cap";      Kind = "param" })
    $paramCols.Add(@{ Name = "T1 Trigger";  Width = 10;          Prop = "T1Trig";     Kind = "param" })
    $paramCols.Add(@{ Name = "Entry Delay"; Width = 11;          Prop = "EntryDelay"; Kind = "param" })
    $paramCols.Add(@{ Name = "Entry Cut";   Width = 9;           Prop = "EntryCut";   Kind = "param" })

    # --- Table B: Config Leaderboard (Logical Flow: Deployed -> Pairs -> Returned -> Win -> Gross ROI / P&L -> Fees -> Net @ 0% Rebate -> Rebate @ 3% -> Net @ 3% Rebate) ---
    $metricCols = [System.Collections.Generic.List[hashtable]]::new()
    $metricCols.Add(@{ Name = "Rank";            Width = 4;           Prop = "Rank";       Kind = "rank" })
    $metricCols.Add(@{ Name = "Config";          Width = $cfgIdWidth; Prop = "ConfigId";   Kind = "cfg_id" })
    $metricCols.Add(@{ Name = "Deployed";        Width = 9;           Prop = "VolumeIn";   NumProp = "VolumeInNum";    Kind = "money" })
    $metricCols.Add(@{ Name = "Pairs";           Width = 7;           Prop = "Pairs";      NumProp = "PairsNum";       Kind = "float" })
    $metricCols.Add(@{ Name = "Returned";        Width = 9;           Prop = "CloseVol";   NumProp = "CloseVolumeNum"; Kind = "money" })
    $metricCols.Add(@{ Name = "Win";             Width = 5;           Prop = "WinRate";    NumProp = "WinRatePct";     Kind = "winrate" })
    $metricCols.Add(@{ Name = "Gross ROI / P&L"; Width = 18;          Prop = "GrossPnl";   NumProp = "GrossPnlNum";    PctProp = "GrossPctNum"; Kind = "pnl_pct" })
    $metricCols.Add(@{ Name = "Fees";            Width = 8;           Prop = "Fees";       NumProp = "FeesNum";        Kind = "fee" })
    $metricCols.Add(@{ Name = "Net @ 0% Rebate"; Width = 18;          Prop = "Net0Plain";  NumProp = "NetPnLNum";      PctProp = "Net0PctNum";  Kind = "pnl_pct_bold" })
    $metricCols.Add(@{ Name = "Rebate @ 3%";     Width = 11;          Prop = "Rebate";     NumProp = "RebateNum";      Kind = "rebate_dollar" })
    $metricCols.Add(@{ Name = "Net @ 3% Rebate"; Width = 18;          Prop = "NetPlusReb"; NumProp = "NetPlusRebNum";  PctProp = "NetPctNum";   Kind = "pnl_pct_bold" })

    function Format-TableHeaderLine {
        param(
            [System.Collections.Generic.List[hashtable]]$Columns,
            [bool]$Colorize
        )
        $sep = if ($Colorize) { Format-ColorText -Text " │ " -Code $AnsiGray } else { " | " }
        $cells = foreach ($col in $Columns) {
            $w = [Math]::Abs([int]$col.Width)
            $leftAlign = ([int]$col.Width -lt 0)
            $cell = if ($leftAlign) { $col.Name.PadRight($w) } else { $col.Name.PadLeft($w) }
            if ($Colorize) { Format-ColorText -Text $cell -Code $AnsiBoldWhite } else { $cell }
        }
        return ("  " + ($cells -join $sep))
    }

    function Format-TableDashLine {
        param(
            [System.Collections.Generic.List[hashtable]]$Columns,
            [bool]$Colorize
        )
        $sep = if ($Colorize) { "─┼─" } else { "-+-" }
        $parts = foreach ($col in $Columns) {
            $w = [Math]::Abs([int]$col.Width)
            ("-" * $w)
        }
        $line = "  " + ($parts -join $sep)
        if ($Colorize) { return (Format-ColorText -Text $line -Code $AnsiGray) }
        return $line
    }

    function Format-TableDataRow {
        param(
            [System.Collections.Generic.List[hashtable]]$Columns,
            [int]$RankNum,
            [PSCustomObject]$Row,
            [bool]$Colorize
        )
        $sep = if ($Colorize) { Format-ColorText -Text " │ " -Code $AnsiGray } else { " | " }
        $cells = [System.Collections.Generic.List[string]]::new()
        foreach ($col in $Columns) {
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
                "cfg_id" {
                    $raw = ([string]$Row.($col.Prop)).PadLeft($w)
                    if ($Colorize) {
                        $cells.Add((Format-ColorText -Text $raw -Code $AnsiCyan))
                    } else {
                        $cells.Add($raw)
                    }
                }
                "param" {
                    $valStr = [string]$Row.($col.Prop)
                    $padded = if ($leftAlign) { $valStr.PadRight($w) } else { $valStr.PadLeft($w) }
                    if ($Colorize) {
                        $pColor = Get-ParamStateColor -Val $valStr -DefaultColor $AnsiBoldWhite
                        $cells.Add((Format-ColorText -Text $padded -Code $pColor))
                    } else {
                        $cells.Add($padded)
                    }
                }
                { $_ -in @("int", "float", "qty", "money") } {
                    $valStr = [string]$Row.($col.Prop)
                    $cells.Add($(if ($leftAlign) { $valStr.PadRight($w) } else { $valStr.PadLeft($w) }))
                }
                { $_ -in @("pnl_pct", "pnl_pct_bold") } {
                    if ($Colorize) {
                        $isBold = ($col.Kind -eq "pnl_pct_bold")
                        $cells.Add((Format-SignedPnlWithPctCell -Value ([double]$Row.($col.NumProp)) -Pct ([double]$Row.($col.PctProp)) -Width $w -Bold $isBold -IncludeDollar $true -Decimals 2 -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                { $_ -in @("rebate", "rebate_dollar") } {
                    if ($Colorize) {
                        $withDollar = ($col.Kind -eq "rebate_dollar")
                        $cells.Add((Format-RebateCell -Value ([double]$Row.($col.NumProp)) -Width $w -Bold $false -IncludeDollar $withDollar -Decimals 2 -Culture $inv))
                    } else {
                        $cells.Add(([string]$Row.($col.Prop)).PadLeft($w))
                    }
                }
                "fee" {
                    if ($Colorize) {
                        $cells.Add((Format-FeeCell -FeePaidPositive ([double]$Row.($col.NumProp)) -Width $w -Bold $false -IncludeDollar $true -Decimals 2 -Culture $inv))
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
        return ("  " + ($cells -join $sep))
    }

    $summaryPath = Join-Path $sweepDir "summary.txt"

    # -----------------------------------------------------------------------
    # Build Plain-Text Summary Lines (for summary.txt) & Color Console Output
    # -----------------------------------------------------------------------
    $plainSummary = [System.Collections.Generic.List[string]]::new()
    $plainSummary.Add("=" * 100)
    $plainSummary.Add(("SWEEP COMPLETE | Started: {0} | Total Elapsed: {1:F2}s | Avg/Config: {2:F2}s | Throughput: {3}" -f $sweepStartedStamp, $totalElapsedSec, $avgConfigSec, $throughputText))
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
    $plainSummary.Add("CONFIG PARAMETERS (ranked by Net @ 0% Rebate desc)")
    $plainSummary.Add((Format-TableHeaderLine -Columns $paramCols -Colorize $false))
    $plainSummary.Add((Format-TableDashLine -Columns $paramCols -Colorize $false))
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        $plainSummary.Add((Format-TableDataRow -Columns $paramCols -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $false))
    }
    $plainSummary.Add((Format-TableDashLine -Columns $paramCols -Colorize $false))
    $plainSummary.Add("")
    $plainSummary.Add("CONFIG LEADERBOARD - PERFORMANCE METRICS (sorted by Net @ 0% Rebate desc)")
    $plainSummary.Add((Format-TableHeaderLine -Columns $metricCols -Colorize $false))
    $plainSummary.Add((Format-TableDashLine -Columns $metricCols -Colorize $false))
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        $plainSummary.Add((Format-TableDataRow -Columns $metricCols -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $false))
    }
    $plainSummary.Add((Format-TableDashLine -Columns $metricCols -Colorize $false))
    $plainSummary.Add("  Win = windows won / windows with results at 0% floor; n/a = no window results")
    $plainSummary.Add("")
    $plainSummary.Add("  STRATEGY VERIFICATION - G1-G6 SURVIVAL GATES (Dual 0% Floor vs. 3% Tier-1)")
    $plainSummary.Add("  G1 In Sample>0 | G2 Out Sample>0 | G3 Retention>=50% | G4 Max Drawdown<=25% | G5 Drag<=30%/50% | G6 Neighbors>=60% | QueueDecay=$QueueDecay (promotion requires conservative)")
    $plainSummary.Add("  Filter 4 (minimum sample size): a config needs >= $($script:MinValidationFills) fills across the sample; below that its Verdict reads UNDERPOWERED and is neither a pass nor a failure.")
    if (-not $isMultiCapture) {
        $plainSummary.Add("  NOTE: single-capture run - In Sample / Out Sample + Max Drawdown report N/A. Re-run with -AllCaptures (or multi -InputPath) for full walk-forward verdicts.")
    }
    $vWidths = @(4, 5, 10, 10, 9, 12, 11, 9, 12)
    $vFmtParts = for ($vi = 0; $vi -lt $vWidths.Count; $vi++) { "{$vi,$($vWidths[$vi])}" }
    $vFmt = $vFmtParts -join " "
    $vHeaders = @("Rank", "Tier", "In Sample", "Out Sample", "Retention", "Max Drawdown", "Drag / Tail", "Neighbors", "Verdict")
    $vDashes = @($vWidths | ForEach-Object { "-" * $_ })
    $plainSummary.Add(("  " + ($vFmt -f $vHeaders)))
    $plainSummary.Add(("  " + ($vFmt -f $vDashes)))
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        $r = $sorted[$ri]
        $dragPlainText = if ([double]::IsNaN([double]$r.TailDragNum)) { "{0:F0}%" -f [double]$r.DragNum } else { "{0:F0}%/{1:F0}%" -f [double]$r.DragNum, [double]$r.TailDragNum }
        foreach ($tierSpec in @(
            @{ Tag = "0%"; IS = $r.ISNum;  OOS = $r.OOSNum;  Ret = $r.RetainNum;  DD = $r.MaxDDNum;  Nbr = $r.NbrNum;  Ver = $r.Verdict  },
            @{ Tag = "3%"; IS = $r.ISNum3; OOS = $r.OOSNum3; Ret = $r.RetainNum3; DD = $r.MaxDDNum3; Nbr = $r.NbrNum3; Ver = $r.Verdict3 }
        )) {
            $isS = if ([double]::IsNaN($tierSpec.IS)) { 'n/a'.PadLeft(10) } else { (Format-GateMoneyText -Value ([double]$tierSpec.IS) -Culture $inv).PadLeft(10) }
            $oosS = if ([double]::IsNaN($tierSpec.OOS)) { 'n/a'.PadLeft(10) } else { (Format-GateMoneyText -Value ([double]$tierSpec.OOS) -Culture $inv).PadLeft(10) }
            $retS = if ([double]::IsNaN($tierSpec.Ret) -or [double]::IsInfinity($tierSpec.Ret)) { 'n/a'.PadLeft(9) } else { (("{0:F0}%" -f $tierSpec.Ret).PadLeft(9)) }
            $ddS = if ([double]::IsNaN($tierSpec.DD)) { 'n/a'.PadLeft(12) } else { (("{0:F0}%" -f $tierSpec.DD).PadLeft(12)) }
            $dragS = $dragPlainText.PadLeft(11)
            $nbrS = if ([double]::IsNaN($tierSpec.Nbr)) { 'n/a'.PadLeft(9) } else { (("{0:F0}%" -f $tierSpec.Nbr).PadLeft(9)) }
            $plainSummary.Add(("  " + ($vFmt -f @(($ri + 1).ToString($inv).PadLeft(4), $tierSpec.Tag.PadLeft(5), $isS, $oosS, $retS, $ddS, $dragS, $nbrS, ([string]$tierSpec.Ver).PadLeft(10)))))
        }
    }
    $plainSummary.Add(("  " + ($vFmt -f $vDashes)))
    $approved0Count = @($sorted | Where-Object { $_.Verdict -eq "APPROVED" }).Count
    $approved3Count = @($sorted | Where-Object { $_.Verdict3 -eq "APPROVED" }).Count
    $underpoweredCount = @($sorted | Where-Object { $_.Verdict -eq "UNDERPOWERED" -or $_.Verdict3 -eq "UNDERPOWERED" }).Count
    $plainSummary.Add(("  Verdicts: @0% {0}/{3} APPROVED | @3% {1}/{3} APPROVED | {2} UNDERPOWERED (<{4} fills) | FAIL(Gx) = first failed gate | NEEDS-OOS = re-run multi-capture | N/A = inputs incomplete" -f $approved0Count, $approved3Count, $underpoweredCount, $sorted.Count, $script:MinValidationFills))
    # Write the leaderboard before printing, so an interrupted console dump still leaves the ranked artifact.
    $plainSummary | Set-Content -LiteralPath $summaryPath -Encoding utf8

    # Print Completion Banner to Console
    Write-Host ""
    Write-Host $majorRule
    Write-Host (Format-ColorText -Text ("🏁 SWEEP COMPLETE  |  Total Elapsed: {0:F2}s  |  Avg/Config: {1:F2}s  |  Throughput: {2}" -f $totalElapsedSec, $avgConfigSec, $throughputText) -Code $AnsiBoldCyan)
    Write-Host ("Summary File : {0}" -f $summaryPath)
    Write-Host $majorRule

    # Table 1: Config Parameters Reference
    Write-Host ""
    Write-Host (Format-ColorText -Text "⚙️  CONFIG PARAMETERS (ranked by Net @ 0% Rebate desc)" -Code $AnsiBoldCyan)
    Write-Host (Format-TableHeaderLine -Columns $paramCols -Colorize $true)
    Write-Host (Format-TableDashLine -Columns $paramCols -Colorize $true)
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        Write-Host (Format-TableDataRow -Columns $paramCols -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $true)
    }
    Write-Host (Format-TableDashLine -Columns $paramCols -Colorize $true)

    # Table 2: Config Leaderboard (Performance Metrics matching per-config subtotals)
    Write-Host ""
    Write-Host (Format-ColorText -Text "🏆 CONFIG LEADERBOARD (sorted by Net @ 0% Rebate desc)" -Code $AnsiBoldCyan)
    Write-Host (Format-TableHeaderLine -Columns $metricCols -Colorize $true)
    Write-Host (Format-TableDashLine -Columns $metricCols -Colorize $true)
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        Write-Host (Format-TableDataRow -Columns $metricCols -RankNum ($ri + 1) -Row $sorted[$ri] -Colorize $true)
    }
    Write-Host (Format-TableDashLine -Columns $metricCols -Colorize $true)
    Write-Host (Format-ColorText -Text "  Win = windows won / windows with results at 0% floor; n/a = no window results" -Code $AnsiGray)

    # Print Strategy Verification table (same fixed-width contract: static columns, padded badges)
    Write-Host ""
    Write-Host (Format-ColorText -Text "🛡️  STRATEGY VERIFICATION - G1-G6 SURVIVAL GATES (Dual 0% Floor vs. 3% Tier-1)" -Code $AnsiBoldCyan)
    Write-Host (Format-ColorText -Text "  G1 In Sample>0 | G2 Out Sample>0 | G3 Retention>=50% | G4 Max Drawdown<=25% | G5 Drag<=30%/50% | G6 Neighbors>=60%" -Code $AnsiGray)
    Write-Host (Format-ColorText -Text ("  Filter 4 minimum sample size: under {0} fills the Verdict reads UNDERPOWERED, not PASS or FAIL (gate cells stay measured)." -f $script:MinValidationFills) -Code $AnsiGray)
    if ($QueueDecay -ne "conservative") {
        Write-Host (Format-ColorText -Text ("  NOTE: QueueDecay={0} (exploratory) - promotion requires -QueueDecay conservative." -f $QueueDecay) -Code $AnsiBoldYellow)
    }
    if (-not $isMultiCapture) {
        Write-Host (Format-ColorText -Text "  NOTE: single-capture run - In Sample / Out Sample + Max Drawdown report N/A. Re-run with -AllCaptures for full walk-forward verdicts." -Code $AnsiYellow)
    }
    Write-Host (Format-ColorText -Text ("  " + ($vFmt -f $vHeaders)) -Code $AnsiBoldWhite)
    Write-Host (Format-ColorText -Text ("  " + ($vFmt -f $vDashes)) -Code $AnsiGray)
    for ($ri = 0; $ri -lt $sorted.Count; $ri++) {
        $r = $sorted[$ri]
        $dragConsoleText = if ([double]::IsNaN([double]$r.TailDragNum)) { "{0:F0}%" -f [double]$r.DragNum } else { "{0:F0}%/{1:F0}%" -f [double]$r.DragNum, [double]$r.TailDragNum }
        foreach ($tierSpec in @(
            @{ Tag = "0%"; IS = $r.ISNum;  OOS = $r.OOSNum;  Ret = $r.RetainNum;  DD = $r.MaxDDNum;  Nbr = $r.NbrNum;  G1 = $r.G1;   G2 = $r.G2;   G3 = $r.G3;   G4 = $r.G4;   G5 = $r.G5;   G6 = $r.G6;   Ver = $r.Verdict  },
            @{ Tag = "3%"; IS = $r.ISNum3; OOS = $r.OOSNum3; Ret = $r.RetainNum3; DD = $r.MaxDDNum3; Nbr = $r.NbrNum3; G1 = $r.G1_3; G2 = $r.G2_3; G3 = $r.G3_3; G4 = $r.G4_3; G5 = $r.G5_3; G6 = $r.G6_3; Ver = $r.Verdict3 }
        )) {
            $rkRaw = (($ri + 1).ToString($inv)).PadLeft(4)
            $tierCell = $tierSpec.Tag.PadLeft(5)
            $isCell = Format-GateCell -RawText (Format-GateMoneyText -Value ([double]$tierSpec.IS) -Culture $inv) -Pass ($tierSpec.G1 -eq "PASS") -IsNA ($tierSpec.G1 -eq "N/A") -Width 10
            $oosCell = Format-GateCell -RawText (Format-GateMoneyText -Value ([double]$tierSpec.OOS) -Culture $inv) -Pass ($tierSpec.G2 -eq "PASS") -IsNA ($tierSpec.G2 -eq "N/A") -Width 10
            $retCell = Format-GateCell -RawText ("{0:F0}%" -f ([double]$tierSpec.Ret)) -Pass ($tierSpec.G3 -eq "PASS") -IsNA (($tierSpec.G3 -eq "N/A")) -Width 9
            $ddCell = Format-GateCell -RawText ("{0:F0}%" -f ([double]$tierSpec.DD)) -Pass ($tierSpec.G4 -eq "PASS") -IsNA ($tierSpec.G4 -eq "N/A") -Width 12
            $dragCell = Format-GateCell -RawText $dragConsoleText -Pass ($tierSpec.G5 -eq "PASS") -IsNA $false -Width 11
            $nbrCell = Format-GateCell -RawText ("{0:F0}%" -f ([double]$tierSpec.Nbr)) -Pass ($tierSpec.G6 -eq "PASS") -IsNA ($tierSpec.G6 -eq "N/A") -Width 9
            $verCell = Format-VerdictCell -Verdict ([string]$tierSpec.Ver) -Width 12
            Write-Host ("  {0} {1} {2} {3} {4} {5} {6} {7} {8}" -f $rkRaw, $tierCell, $isCell, $oosCell, $retCell, $ddCell, $dragCell, $nbrCell, $verCell)
        }
    }
    Write-Host (Format-ColorText -Text ("  " + ($vFmt -f $vDashes)) -Code $AnsiGray)
    Write-Host ("  Verdicts: @0% {0}/{3} APPROVED  |  @3% {1}/{3} APPROVED  |  {2} UNDERPOWERED  |  FAIL(Gx) = first failed gate  |  NEEDS-OOS = re-run multi-capture" -f $approved0Count, $approved3Count, $underpoweredCount, $sorted.Count)

    Write-Host ""
}
finally {
    Clear-BottomRightHud
    Pop-Location
}
