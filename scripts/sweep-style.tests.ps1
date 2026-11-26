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

    # --- Signed percent cells (ROI) ----------------------------------------
    $pctPos = Format-SignedPctCell -Ratio 0.0155 -Width 8
    Assert-True ((Get-VisibleText $pctPos) -eq '  +1.55%') "positive pct shows +1.55%"
    Assert-True ((Get-VisibleText $pctPos).Length -eq 8) "pct cell width is 8"
    Assert-True ($pctPos.Contains("$esc[32m")) "positive pct is green"
    $pctNeg = Format-SignedPctCell -Ratio -0.0044 -Width 8
    Assert-True ((Get-VisibleText $pctNeg) -eq '  -0.44%') "negative pct shows -0.44%"
    Assert-True ($pctNeg.Contains("$esc[31m")) "negative pct is red"
    $pctZero = Format-SignedPctCell -Ratio 0.0 -Width 8
    Assert-True ($pctZero.Contains("$esc[90m")) "zero pct is muted"
    $pctNaN = Format-SignedPctCell -Ratio ([double]::NaN) -Width 8
    Assert-True ((Get-VisibleText $pctNaN) -eq '     n/a') "NaN pct renders n/a"
    Assert-True ($pctNaN.Contains("$esc[90m")) "NaN pct is muted"

    # --- Trading ROI ratio (single definition) -----------------------------
    $roiBasic = Get-TradingRoiRatio -Deployed 1000.0 -Returned 1020.0 -FeesPaid 5.0
    Assert-True ($roiBasic -eq 0.015) "ROI deducts fees: (1020-1000-5)/1000"
    $roiNeg = Get-TradingRoiRatio -Deployed 1000.0 -Returned 990.0 -FeesPaid 5.0
    Assert-True ($roiNeg -eq -0.015) "ROI goes negative when trading loses"
    $roiEmpty = Get-TradingRoiRatio -Deployed 0.0 -Returned 0.0 -FeesPaid 0.0
    Assert-True ([double]::IsNaN($roiEmpty)) "ROI is NaN when nothing deployed"
    $roiSample = Get-TradingRoiRatio -Deployed 523.10 -Returned 531.25 -FeesPaid 0.41
    Assert-True ([Math]::Abs($roiSample - 0.014797) -lt 1e-6) "ROI matches the leaderboard sample figure"

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
    Assert-True (-not (Test-EntryPriceSwept -Prices @("0.65"))) "default 0.65 entry price hides the MaxPx column"
    Assert-True (Test-EntryPriceSwept -Prices @("0.60")) "non-default 0.60 entry price shows the MaxPx column"
    Assert-True (Test-EntryPriceSwept -Prices @("off", "0.65")) "two entry prices show the MaxPx column"

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

    # --- Window-level survival inputs (IS/OOS split, MaxDD) ---------------
    $survSmall = Get-SurvivalInputs -CapRuns @() -TotalWindows 3 -IsMultiCapture $false -CaptureCount 1 `
        -SumVolIn 100.0 -SumGross 0.0 -SumSettle 0.0 -SumUnreal 0.0 -WindowNets @(10.0, -20.0, 10.0)
    Assert-True ([Math]::Abs($survSmall.MaxDD - 20.0) -lt 1e-9) "3-window series reports 20% MaxDD"
    Assert-True ([double]::IsNaN($survSmall.IS) -and [double]::IsNaN($survSmall.OOS)) "under 4 windows skips the IS/OOS split"

    $survSplit = Get-SurvivalInputs -CapRuns @() -TotalWindows 10 -IsMultiCapture $true -CaptureCount 2 `
        -SumVolIn 100.0 -SumGross 0.0 -SumSettle 0.0 -SumUnreal 0.0 -WindowNets @(-1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0, 1.0)
    Assert-True ($survSplit.ISWin -eq 7 -and $survSplit.OOSWin -eq 3) "10 windows split 7 IS / 3 OOS"
    Assert-True ([Math]::Abs($survSplit.IS - 10.0) -lt 1e-9) "IS sums the first 7 window nets"
    Assert-True ([Math]::Abs($survSplit.OOS - 3.0) -lt 1e-9) "OOS sums the final 3 window nets"
    Assert-True ([Math]::Abs($survSplit.Retain - 70.0) -lt 1e-9) "OOS per-window retention is 70% of IS (1.0/win vs 1.4286/win)"

    # --- G4 AllocatedCapital & G5 MergePos / TailDrag ----------------------
    $survG45 = Get-SurvivalInputs -CapRuns @() -TotalWindows 4 -IsMultiCapture $true -CaptureCount 2 `
        -SumVolIn 1000.0 -AllocatedCapital 50.0 -SumGross 55.0 -SumSettle -23.62 -SumUnreal 0.0 `
        -SumMergeNet 78.61 -SumMergePos 89.03 -SumMergeNegLoss 10.42 -WindowNets @(15.0, -9.71, 25.0, 26.30)
    Assert-True ([Math]::Abs($survG45.MaxDD - 19.42) -lt 1e-6) 'MaxDD uses AllocatedCapital ($9.71 / $50 = 19.42%), not SumVolIn'
    Assert-True ([Math]::Abs($survG45.Drag - (23.62 / 89.03 * 100.0)) -lt 1e-6) "Expiry drag divides settlement loss by positive merge gains"
    Assert-True ([Math]::Abs($survG45.TailDrag - (34.04 / 89.03 * 100.0)) -lt 1e-6) "Tail drag adds negative merge losses to settlement losses"
    $survPartialWindows = Get-SurvivalInputs -CapRuns @() -TotalWindows 6 -IsMultiCapture $false -CaptureCount 1 `
        -SumVolIn 1000.0 -SumGross 10.0 -SumSettle 0.0 -SumUnreal 0.0 -WindowNets @(5.0, 5.0, 5.0, 5.0)
    Assert-True ([double]::IsNaN($survPartialWindows.IS)) 'Partial window series (4 of 6 windows) is not scored as the full sample'

    $evalPass = Evaluate-SurvivalGateRow -ISVal 10.0 -OOSVal 10.0 -RetainVal 70.0 -MaxDDVal 19.4 -DragVal 26.5 -NbrVal 100.0 -TailDragVal 38.2
    Assert-True ($evalPass.Verdict -eq "APPROVED") "Config passing G1-G6 and TailDrag<=50% is APPROVED"
    $evalRelabelFail = Evaluate-SurvivalGateRow -ISVal 10.0 -OOSVal 10.0 -RetainVal 70.0 -MaxDDVal 20.9 -DragVal 12.1 -NbrVal 100.0 -TailDragVal 57.3
    Assert-True ($evalRelabelFail.G5 -eq "FAIL" -and $evalRelabelFail.Verdict -eq "FAIL(G5)") "High negative-merge TailDrag>50% fails G5 despite low settlement drag"
    $evalUnknownTail = Evaluate-SurvivalGateRow -ISVal 10.0 -OOSVal 10.0 -RetainVal 70.0 -MaxDDVal 19.4 -DragVal 12.1 -NbrVal 100.0
    Assert-True ($evalUnknownTail.G5 -eq "N/A" -and $evalUnknownTail.Verdict -eq "N/A") "Unknown TailDrag reports G5 N/A and is ineligible for APPROVED"
    $evalZeroTail = Evaluate-SurvivalGateRow -ISVal 10.0 -OOSVal 10.0 -RetainVal 70.0 -MaxDDVal 19.4 -DragVal 12.1 -NbrVal 100.0 -TailDragVal 0.0
    Assert-True ($evalZeroTail.G5 -eq "PASS" -and $evalZeroTail.Verdict -eq "APPROVED") "Measured zero TailDrag still passes G5"

    # --- Filter 4: minimum sample size (UNDERPOWERED verdict) --------------
    $evalUnder = Evaluate-SurvivalGateRow -ISVal 31.68 -OOSVal 1.61 -RetainVal 11.66 -MaxDDVal 24.49 -DragVal 39.11 -NbrVal 100.0 -TailDragVal 63.08 -TotalFills 351
    Assert-True ($evalUnder.Verdict -eq "UNDERPOWERED") "351 fills reports UNDERPOWERED instead of FAIL/APPROVED"
    Assert-True ($evalUnder.G3 -eq "FAIL" -and $evalUnder.G5 -eq "FAIL") "UNDERPOWERED still shows the measured gate cells"
    $evalUnderPass = Evaluate-SurvivalGateRow -ISVal 10.0 -OOSVal 10.0 -RetainVal 70.0 -MaxDDVal 19.4 -DragVal 26.5 -NbrVal 100.0 -TailDragVal 38.2 -TotalFills 399
    Assert-True ($evalUnderPass.Verdict -eq "UNDERPOWERED") "399 fills is still below the 400-fill bar"
    $evalAtBar = Evaluate-SurvivalGateRow -ISVal 10.0 -OOSVal 10.0 -RetainVal 70.0 -MaxDDVal 19.4 -DragVal 26.5 -NbrVal 100.0 -TailDragVal 38.2 -TotalFills 400
    Assert-True ($evalAtBar.Verdict -eq "APPROVED") "400 fills meets the bar and can be APPROVED"
    Assert-True ((Get-VisibleText (Format-VerdictCell -Verdict "UNDERPOWERED" -Width 12)).Length -eq 12) "UNDERPOWERED fits the widened 12-column verdict field"

    # --- G6 Manhattan-1 adjacency + OOS neighbor survival -----------------
    $mkRow = {
        param([string]$px, [string]$t1, [double]$isv, [double]$oosv, [double]$netv)
        [PSCustomObject]@{
            Gamma = "0.12"; VarSpr = "true"; MinT = "1"; XTick = '$0.08'; MaxP = "0.8"
            MaxPx = $px; LLTh = '$0.015'; LLWin = "1000"; LLCd = "1000"; LLDir = "true"
            HSkew = "1"; T1Cap = "true"; T1Trig = $t1; EntryDelay = "15s"; EntryCut = "120s"
            NetPnLNum = $netv; ISNum = $isv; OOSNum = $oosv
            RetainNum = 100.0; MaxDDNum = 10.0; DragNum = 20.0; TailDragNum = 25.0
            NbrNum = [double]::NaN; G1 = ""; G2 = ""; G3 = ""; G4 = ""; G5 = ""; G6 = ""; Verdict = ""
        }
    }
    $gridRows = @(
        (& $mkRow '$0.50' "off" 10.0 10.0 20.0),
        (& $mkRow '$0.55' "off" 10.0 -2.0 8.0),
        (& $mkRow '$0.60' "off" 10.0 10.0 20.0)
    )
    Update-SurvivalVerdicts -Sorted $gridRows
    Assert-True ([Math]::Abs($gridRows[0].NbrNum - 0.0) -lt 1e-9) 'G6 Manhattan-1 skips non-adjacent $0.60 and rejects OOS-negative $0.55'
    Assert-True ([Math]::Abs($gridRows[1].NbrNum - 100.0) -lt 1e-9) "G6 interior cell sees both immediate neighbors"

    # A row's fill count must reach the verdict, or a sweep would still print APPROVED.
    $gridRows[0] | Add-Member -NotePropertyName FillsNum -NotePropertyValue 351
    Update-SurvivalVerdicts -Sorted @($gridRows[0], $gridRows[1])
    Assert-True ($gridRows[0].Verdict -eq "UNDERPOWERED") "Update-SurvivalVerdicts forwards the row's FillsNum into the verdict"
    Assert-True ($gridRows[1].Verdict -ne "UNDERPOWERED") "A row without a fill count keeps the legacy verdict path"

    $capRuns = @(
        [PSCustomObject]@{ Windows = 2; NetPlusRebNum = 5.0 },
        [PSCustomObject]@{ Windows = 2; NetPlusRebNum = -10.0 }
    )
    $survFallback = Get-SurvivalInputs -CapRuns $capRuns -TotalWindows 4 -IsMultiCapture $true -CaptureCount 2 `
        -SumVolIn 100.0 -SumGross 0.0 -SumSettle 0.0 -SumUnreal 0.0 -WindowNets @()
    Assert-True ([Math]::Abs($survFallback.MaxDD - 10.0) -lt 1e-9) "no window nets falls back to capture-level MaxDD"
    $survSingle = Get-SurvivalInputs -CapRuns @($capRuns[0]) -TotalWindows 2 -IsMultiCapture $false -CaptureCount 1 `
        -SumVolIn 100.0 -SumGross 0.0 -SumSettle 0.0 -SumUnreal 0.0 -WindowNets @()
    Assert-True ([double]::IsNaN($survSingle.MaxDD)) "single capture without window nets reports MaxDD n/a"

    # --- Read-CaptureJsonlMetrics: summary telemetry & legacy fallbacks ---
    $tmpMetricDir = Join-Path ([System.IO.Path]::GetTempPath()) ("sweep-metrics-test-" + [Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force -Path $tmpMetricDir | Out-Null
    try {
        $fakeCap = [PSCustomObject]@{ Name = "cap.jsonl"; Stem = "cap"; Windows = 1 }
        $fullPath = Join-Path $tmpMetricDir "full.jsonl"
        @(
            '{"event":"summary","payload":{"realized_pnl_micro":5000000,"unrealized_pnl_micro":0,"net_pnl_micro":5000000,"estimated_rebate_micro":100000,"total_pairs_merged_micro":20000000,"fills_count":2,"merges_count":1,"rpm_60s":2,"markout_1s_bps":0,"markout_5s_bps":0,"total_shares_bought_micro":40000000,"volume_in_micro":19500000,"total_merge_gain_micro":5000000,"positive_merge_gain_micro":5500000,"negative_merge_loss_micro":500000}}'
        ) | Set-Content -LiteralPath $fullPath -Encoding utf8
        $mFull = (Read-CaptureJsonlMetrics -OutJsonl $fullPath -Cap $fakeCap -CapIdx 0 -BaseSize 20 -MaxCombinedBidMicro 980000 -WinColW 12 -TblSep " | ").CapRun
        Assert-True ([Math]::Abs($mFull.QuantityNum - 40.0) -lt 1e-6) "summary telemetry supplies total_shares_bought_micro"
        Assert-True ([Math]::Abs($mFull.VolumeInNum - 19.5) -lt 1e-6) "summary telemetry supplies volume_in_micro"
        Assert-True ([Math]::Abs($mFull.MergePosNum - 5.5) -lt 1e-6 -and [Math]::Abs($mFull.MergeNegLossNum - 0.5) -lt 1e-6) "summary telemetry supplies merge gains and losses"

        $legacyPath = Join-Path $tmpMetricDir "legacy.jsonl"
        @(
            '{"event":"fill","payload":{"side":"BUY","filled_shares_micro":20000000,"price_micro":510000}}',
            '{"event":"fill","payload":{"side":"BUY","filled_shares_micro":10000000,"price_micro":450000}}',
            '{"event":"flatten","payload":{"side":"BUY","size_micro":10000000,"price_micro":10000}}',
            '{"event":"merge","payload":{"realized_gain_micro":1200000}}',
            '{"event":"merge","payload":{"realized_gain_micro":-300000}}',
            '{"event":"window_end","payload":{"slug":"w1","net_pnl_micro":900000}}',
            '{"event":"summary","payload":{"realized_pnl_micro":900000,"unrealized_pnl_micro":0,"net_pnl_micro":900000,"estimated_rebate_micro":0,"total_pairs_merged_micro":20000000,"fills_count":2,"flattens_executed":1,"merges_count":2,"rpm_60s":1,"markout_1s_bps":0,"markout_5s_bps":0,"settlement_pnl_micro":0}}'
        ) | Set-Content -LiteralPath $legacyPath -Encoding utf8
        $mLeg = (Read-CaptureJsonlMetrics -OutJsonl $legacyPath -Cap $fakeCap -CapIdx 0 -BaseSize 20 -MaxCombinedBidMicro 980000 -WinColW 12 -TblSep " | ").CapRun
        Assert-True ([Math]::Abs($mLeg.QuantityNum - 40.0) -lt 1e-6) "legacy scan sums BUY fill + BUY flatten shares (40sh)"
        Assert-True ([Math]::Abs($mLeg.VolumeInNum - 14.8) -lt 1e-6) 'legacy scan reconstructs exact BUY notional ($10.20 + $4.50 + $0.10 = $14.80)'
        Assert-True ([Math]::Abs($mLeg.MergePosNum - 1.2) -lt 1e-6 -and [Math]::Abs($mLeg.MergeNegLossNum - 0.3) -lt 1e-6) "legacy scan splits positive and negative merge events"

        $summaryOnlyPath = Join-Path $tmpMetricDir "summary_only.jsonl"
        @(
            '{"event":"summary","payload":{"realized_pnl_micro":2000000,"unrealized_pnl_micro":0,"net_pnl_micro":2000000,"estimated_rebate_micro":0,"total_pairs_merged_micro":10000000,"fills_count":5,"merges_count":2,"rpm_60s":1,"markout_1s_bps":0,"markout_5s_bps":0,"settlement_pnl_micro":0}}'
        ) | Set-Content -LiteralPath $summaryOnlyPath -Encoding utf8
        $mSummOnly = (Read-CaptureJsonlMetrics -OutJsonl $summaryOnlyPath -Cap $fakeCap -CapIdx 0 -BaseSize 20 -MaxCombinedBidMicro 980000 -WinColW 12 -TblSep " | ").CapRun
        Assert-True ([double]::IsNaN($mSummOnly.MergePosNum) -and [double]::IsNaN($mSummOnly.MergeNegLossNum)) "summary-only legacy JSONL with merges>0 does not mistake missing merge events for 0.0"
        Assert-True ($mSummOnly.VolumeInNum -gt 0.0) "summary-only legacy JSONL with fills>0 falls back to synthetic volume instead of 0.0"
    } finally {
        if (Test-Path -LiteralPath $tmpMetricDir) { Remove-Item -LiteralPath $tmpMetricDir -Recurse -Force -ErrorAction SilentlyContinue }
    }

    # --- Config banner timestamp & format ---------------------------------
    $sampleMap = @{
        "V Max" = "`$50"; "Base Size" = "20sh"; "Max Pair" = "`$0.98"; "Queue Decay" = "conservative"
        "Gamma" = "0.12"; "Variance" = "true"; "Min Ticks" = "1"; "Cross Loss" = "`$0.08"
        "Entry Prob" = "0.8"; "Entry Price" = "`$0.65"; "Threshold" = "`$0.015"; "Window Ms" = "1000"
        "Cooldown Ms" = "1000"; "Direction" = "true"; "Hedge Skew" = "1"; "Tier 1 Cap" = "true"
        "T1 Trigger" = "off"; "Entry Delay" = "15s"; "Entry Cut" = "120s"
    }
    $script:UseAnsi = $false
    $plainBanner = Format-ConfigBannerLines -Index 4 -Total 18 -Curr $sampleMap -Prev $null -StartTimeText "19:05"
    Assert-True ($plainBanner[1].StartsWith("⚙️  [CONFIG #4 / 18] - [19:05] ---")) "plain config banner includes #idx/total and HH:MM timestamp"
    Assert-True ($plainBanner[1].Length -ge 100) "plain config banner pads dashes to full rule width"

    $script:UseAnsi = $true
    $ansiBanner = Format-ConfigBannerLines -Index 4 -Total 18 -Curr $sampleMap -Prev $null -StartTimeText "19:05"
    Assert-True ((Get-VisibleText $ansiBanner[1]).StartsWith("⚙️  [CONFIG #4 / 18] - [19:05] ---")) "ANSI config banner keeps visible header format"

    # --- Resume state detection (Complete vs Partial vs Pending) ----------
    $tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("sweep-resume-test-" + [Guid]::NewGuid().ToString("N"))
    try {
        $capA = Join-Path $tmpRoot "cap_a"
        $capB = Join-Path $tmpRoot "cap_b"
        New-Item -ItemType Directory -Force -Path $capA, $capB | Out-Null
        $fakeCaptures = @(
            [PSCustomObject]@{ Stem = "cap_a"; Name = "cap_a.jsonl"; Windows = 5 },
            [PSCustomObject]@{ Stem = "cap_b"; Name = "cap_b.jsonl"; Windows = 5 }
        )

        # Pending
        $stPending = Get-ConfigDiskStatus -SweepDir $tmpRoot -Captures $fakeCaptures -IsMultiCapture $true -Stem "cfg_pending"
        Assert-True ($stPending.Status -eq "Pending") "missing files classify as Pending"

        # Complete
        '{"event":"summary","payload":{"realized_pnl_micro":0}}' | Set-Content -LiteralPath (Join-Path $capA "cfg_done.jsonl") -Encoding utf8
        '{"event":"summary","payload":{"realized_pnl_micro":0}}' | Set-Content -LiteralPath (Join-Path $capB "cfg_done.jsonl") -Encoding utf8
        $stDone = Get-ConfigDiskStatus -SweepDir $tmpRoot -Captures $fakeCaptures -IsMultiCapture $true -Stem "cfg_done"
        Assert-True ($stDone.Status -eq "Complete" -and $stDone.ValidSummaryCount -eq 2) "readable summary events classify as Complete"

        # A summary the metrics reader cannot parse is not resumable as Complete.
        '{"event":"summary","payload":{}}' | Set-Content -LiteralPath (Join-Path $capA "cfg_empty.jsonl") -Encoding utf8
        '{"event":"summary","payload":{}}' | Set-Content -LiteralPath (Join-Path $capB "cfg_empty.jsonl") -Encoding utf8
        $stEmpty = Get-ConfigDiskStatus -SweepDir $tmpRoot -Captures $fakeCaptures -IsMultiCapture $true -Stem "cfg_empty"
        Assert-True ($stEmpty.Status -eq "Partial") "summary without readable fields classifies as Partial"

        # Partial (missing second capture)
        '{"event":"summary","payload":{}}' | Set-Content -LiteralPath (Join-Path $capA "cfg_part1.jsonl") -Encoding utf8
        $stPart1 = Get-ConfigDiskStatus -SweepDir $tmpRoot -Captures $fakeCaptures -IsMultiCapture $true -Stem "cfg_part1"
        Assert-True ($stPart1.Status -eq "Partial") "single capture file classifies as Partial"

        # Partial (missing summary line in second capture)
        '{"event":"summary","payload":{}}' | Set-Content -LiteralPath (Join-Path $capA "cfg_part2.jsonl") -Encoding utf8
        '{"event":"tick","payload":{}}' | Set-Content -LiteralPath (Join-Path $capB "cfg_part2.jsonl") -Encoding utf8
        $stPart2 = Get-ConfigDiskStatus -SweepDir $tmpRoot -Captures $fakeCaptures -IsMultiCapture $true -Stem "cfg_part2"
        Assert-True ($stPart2.Status -eq "Partial") "jsonl without final summary classifies as Partial"
    } finally {
        if (Test-Path -LiteralPath $tmpRoot) { Remove-Item -LiteralPath $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue }
    }

    # --- Explicit -OutputDir safety (create new, refuse existing) ---------
    $tmpOutParent = Join-Path ([System.IO.Path]::GetTempPath()) ("sweep-outdir-test-" + [Guid]::NewGuid().ToString("N"))
    try {
        $newOutDir = Join-Path $tmpOutParent "fresh_9runs"
        $resolvedOut = Resolve-NewSweepOutputDir -OutputDir $newOutDir -OrigPwd $tmpOutParent
        Assert-True (Test-Path -LiteralPath $resolvedOut -PathType Container) "Resolve-NewSweepOutputDir creates brand-new unique directory"

        $threwOnExisting = $false
        try {
            $null = Resolve-NewSweepOutputDir -OutputDir $resolvedOut -OrigPwd $tmpOutParent
        } catch {
            $threwOnExisting = $true
        }
        Assert-True $threwOnExisting "Resolve-NewSweepOutputDir refuses to overwrite or reuse an existing directory"
    } finally {
        if (Test-Path -LiteralPath $tmpOutParent) { Remove-Item -LiteralPath $tmpOutParent -Recurse -Force -ErrorAction SilentlyContinue }
    }

    # --- Syntax check of sweep-params.ps1 --------------------------------
    $parseErrs = $null
    $parseTokens = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot "sweep-params.ps1"), [ref]$parseTokens, [ref]$parseErrs)
    Assert-True ($parseErrs.Count -eq 0) "sweep-params.ps1 parses with zero syntax errors"
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
