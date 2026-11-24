# Tests for the ANSI helpers embedded in scripts/sweep-params.ps1.
# Run: pwsh -NoProfile -File scripts/sweep-style.tests.ps1
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:Failures = 0
$esc = [char]27

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if ($Condition) {
        Write-Host "PASS: $Message"
    } else {
        Write-Host "FAIL: $Message" -ForegroundColor Red
        $script:Failures++
    }
}

function Get-VisibleText {
    param([string]$Text)
    return ($Text -replace "$esc\[[0-9;]*m", "")
}

function Get-DisplayWidth {
    param([string]$Text)
    # '⚡' is one UTF-16 unit but two terminal cells.
    $visible = Get-VisibleText $Text
    return ($visible.Length + ([regex]::Matches($visible, "⚡").Count))
}

# Load only the helper block so the script body (which needs captures and a binary) does not run.
$scriptText = Get-Content -LiteralPath (Join-Path $PSScriptRoot "sweep-params.ps1") -Raw
$startMarker = "# Ensure UTF-8 console rendering"
$endMarker = "# Parameter Parsing & Validation"
$start = $scriptText.IndexOf($startMarker)
$end = $scriptText.IndexOf($endMarker)
if ($start -lt 0 -or $end -le $start) {
    throw "Could not locate the helper block in sweep-params.ps1"
}
$helperBlock = $scriptText.Substring($start, $end - $start)

$previousNoColor = $env:NO_COLOR
try {
    Remove-Item Env:NO_COLOR -ErrorAction SilentlyContinue
    Invoke-Expression $helperBlock
    # Test stdout is redirected, so force color on to exercise the escape sequences.
    $script:UseAnsi = $true

    # --- PnL cells -------------------------------------------------------
    foreach ($value in @(1.2345, -1.2345, 0.0)) {
        $pnl = Format-SignedPnlCell -Value $value -Width 9
        $markout = Format-SignedMarkoutCell -Value $value -Width 6
        Assert-True ((Get-VisibleText $pnl).Length -eq 9) "PnL cell width is 9 for $value"
        Assert-True ((Get-VisibleText $markout).Length -eq 6) "markout cell width is 6 for $value"
    }
    Assert-True ((Get-VisibleText (Format-SignedPnlCell -Value 1.2345)) -eq "  +1.2345") "positive PnL pads before coloring"
    Assert-True ((Get-VisibleText (Format-SignedPnlCell -Value -1.2345)) -eq "  -1.2345") "negative PnL pads before coloring"
    Assert-True ((Get-VisibleText (Format-SignedPnlCell -Value 0.0)) -eq "  +0.0000") "zero PnL pads before coloring"
    Assert-True ((Format-SignedPnlCell -Value 1.2345).Contains("$esc[32m")) "positive PnL is green"
    Assert-True ((Format-SignedPnlCell -Value -1.2345).Contains("$esc[31m")) "negative PnL is red"
    Assert-True ((Format-SignedPnlCell -Value 0.0).Contains("$esc[90m")) "zero PnL is muted"
    Assert-True ((Format-SignedPnlCell -Value 1.2345 -Bold $true).Contains("$esc[1;32m")) "bold positive PnL is bold green"

    # --- Rebate cells ----------------------------------------------------
    Assert-True ((Get-VisibleText (Format-RebateCell -Value 0.42 -Width 7)).Length -eq 7) "rebate width is 7"
    Assert-True ((Format-RebateCell -Value 0.42 -Width 7).Contains("$esc[36m")) "positive rebate is cyan"
    Assert-True ((Format-RebateCell -Value 0.0 -Width 7).Contains("$esc[90m")) "zero rebate is muted"
    Assert-True ((Format-RebateCell -Value -0.42 -Width 7).Contains("$esc[31m")) "negative rebate is red"

    # --- Fee cells & Dollar formatting ----------------------------------
    $zeroFee = Format-FeeCell -FeePaidPositive 0.0 -Width 9 -IncludeDollar $true
    Assert-True ((Get-VisibleText $zeroFee) -eq " -`$0.0000") "zero fee has single minus and dollar sign"
    Assert-True (-not (Get-VisibleText $zeroFee).Contains("--")) "zero fee has no double minus"
    $posFee = Format-FeeCell -FeePaidPositive 0.1234 -Width 9 -IncludeDollar $true
    Assert-True ((Get-VisibleText $posFee) -eq " -`$0.1234") 'positive fee paid renders as -$0.1234'
    Assert-True (-not (Get-VisibleText $posFee).Contains("--")) "positive fee has no double minus"

    $pnlPct = Format-SignedPnlWithPctCell -Value -3.8439 -Pct -0.0145 -Width 20 -IncludeDollar $true
    Assert-True ((Get-VisibleText $pnlPct).Contains("-`$3.8439")) 'negative PnL includes -$ sign'
    Assert-True (-not (Get-VisibleText $pnlPct).Contains("--")) "negative PnL has no double minus"

    # --- Parameter diff cells -------------------------------------------
    $changed = Format-ParamCell -Label "MaxPx" -CurrVal "0.7" -PrevVal "off" -IsLeftColumn $false
    Assert-True ($changed.Contains("$esc[1;33m")) "changed parameter is bold yellow"
    Assert-True ($changed.Contains("⚡ (was: off)")) "changed parameter shows the previous value"

    $unchanged = Format-ParamCell -Label "Gamma" -CurrVal "0.12" -PrevVal "0.12" -IsLeftColumn $false
    Assert-True (-not $unchanged.Contains("⚡")) "unchanged parameter has no diff badge"

    $firstConfig = Format-ParamCell -Label "Gamma" -CurrVal "0.12" -PrevVal "" -IsLeftColumn $false
    Assert-True (-not $firstConfig.Contains("⚡")) "first config has no diff badge"

    $leftChanged = Format-ParamCell -Label "Gamma" -CurrVal "0.15" -PrevVal "0.12" -IsLeftColumn $true
    $leftUnchanged = Format-ParamCell -Label "Gamma" -CurrVal "0.15" -PrevVal "0.15" -IsLeftColumn $true
    Assert-True ((Get-DisplayWidth $leftChanged) -eq 37) "left column keeps a fixed display width when changed"
    Assert-True ((Get-DisplayWidth $leftChanged) -eq (Get-DisplayWidth $leftUnchanged)) "diff badge does not shift the left column separator"
    Assert-True ((Get-DisplayWidth $leftUnchanged) -eq ((Get-VisibleText $leftUnchanged).Length)) "unchanged left cell has no double-width glyphs"

    # --- Entry-price sweep detection -------------------------------------
    Assert-True (-not (Test-EntryPriceSwept -Prices @("off"))) "single 'off' entry price hides the MaxPx column"
    Assert-True (-not (Test-EntryPriceSwept -Prices @("0.6"))) "default 0.6 entry price hides the MaxPx column"
    Assert-True (Test-EntryPriceSwept -Prices @("0.7")) "non-default entry price shows the MaxPx column"
    Assert-True (Test-EntryPriceSwept -Prices @("off", "0.6")) "two entry prices show the MaxPx column"

    # --- Plain mode ------------------------------------------------------
    $script:UseAnsi = $false
    $plainPnl = Format-SignedPnlCell -Value 1.2345 -Width 9
    $plainCell = Format-ParamCell -Label "MaxPx" -CurrVal "0.7" -PrevVal "off" -IsLeftColumn $false
    Assert-True (-not $plainPnl.Contains($esc)) "NO_COLOR PnL contains no escape sequences"
    Assert-True (-not $plainCell.Contains($esc)) "NO_COLOR diff cell contains no escape sequences"
    Assert-True ($plainCell.Contains("⚡ (was: off)")) "diff badge stays readable without color"
    Assert-True ((Get-VisibleText $plainPnl) -eq "  +1.2345") "plain PnL keeps padding"

    # --- NO_COLOR presence semantics ------------------------------------
    # The gate is ($null -eq $env:NO_COLOR) -and (not redirected); test the NO_COLOR half.
    $env:NO_COLOR = ""
    Assert-True (-not ($null -eq $env:NO_COLOR)) "empty NO_COLOR counts as set (disables color)"
    Remove-Item Env:NO_COLOR
    Assert-True ($null -eq $env:NO_COLOR) "unset NO_COLOR allows color"
}
finally {
    if ($null -eq $previousNoColor) {
        Remove-Item Env:NO_COLOR -ErrorAction SilentlyContinue
    } else {
        $env:NO_COLOR = $previousNoColor
    }
}

if ($script:Failures -gt 0) {
    Write-Host "$($script:Failures) test(s) failed." -ForegroundColor Red
    exit 1
}
Write-Host "All sweep-style tests passed."
