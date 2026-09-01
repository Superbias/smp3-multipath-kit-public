[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
$Installer = Join-Path $Root "scripts\install-mihomo-smp3.ps1"

$tokens = $null
$parseErrors = $null
[System.Management.Automation.Language.Parser]::ParseFile($Installer, [ref]$tokens, [ref]$parseErrors) | Out-Null
if ($parseErrors.Count -gt 0) {
    throw ("PowerShell parse failed: " + (($parseErrors | ForEach-Object { $_.Message }) -join "; "))
}

if (-not [Environment]::Is64BitOperatingSystem) {
    throw "installer test requires Windows amd64"
}

$TestRoot = Join-Path $env:TEMP ("smp3-mihomo-installer-test-" + [Guid]::NewGuid().ToString("N"))
$Assets = Join-Path $TestRoot "assets"
$Profile = Join-Path $TestRoot "profile"
New-Item -ItemType Directory -Path $Assets, $Profile -Force | Out-Null

function Get-TestSha256 {
    param([string]$Path)
    $stream = [IO.File]::OpenRead($Path)
    try {
        $algorithm = [Security.Cryptography.SHA256]::Create()
        try {
            return ([BitConverter]::ToString($algorithm.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
        } finally {
            $algorithm.Dispose()
        }
    } finally {
        $stream.Dispose()
    }
}

function Write-ReleaseFixture {
    param([string]$Version)
    $asset = Join-Path $Assets "mihomo-smp3-windows-amd64.exe"
    Set-Content -LiteralPath $asset -Value ("fixture-" + $Version) -NoNewline
    $hash = Get-TestSha256 $asset
    Set-Content -LiteralPath (Join-Path $Assets "SHA256SUMS") -Value ("$hash  mihomo-smp3-windows-amd64.exe") -NoNewline
    $json = '{"tag_name":"v' + $Version + '","draft":false,"prerelease":false,"assets":[{"name":"mihomo-smp3-windows-amd64.exe","browser_download_url":"file:///fixture/mihomo-smp3-windows-amd64.exe"},{"name":"SHA256SUMS","browser_download_url":"file:///fixture/SHA256SUMS"}]}'
    Set-Content -LiteralPath (Join-Path $TestRoot "release.json") -Value $json -NoNewline
}

function Invoke-Installer {
    param([string[]]$Arguments)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $Installer @Arguments 2>&1 | Out-String
        $success = $LASTEXITCODE -eq 0
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    return [PSCustomObject]@{ Output = $output; Success = $success }
}

try {
    $env:SMP3_INSTALLER_TEST_RELEASE_JSON = Join-Path $TestRoot "release.json"
    $env:SMP3_INSTALLER_TEST_ASSET_DIR = $Assets
    $oldCore = Join-Path $TestRoot "mihomo.exe"
    Set-Content -LiteralPath $oldCore -Value "original-core" -NoNewline

    Write-ReleaseFixture "2.0.1"
    $result = Invoke-Installer @("-CorePath", $oldCore, "-Update", "-Version", "2.0.1", "-NonInteractive")
    if (-not $result.Success) { Write-Host $result.Output; throw "single-core install failed" }
    if ((Get-Content -LiteralPath $oldCore -Raw) -ne "fixture-2.0.1") { throw "installed core content mismatch" }
    $state = Join-Path $TestRoot "smp3-install-state.json"
    if (-not (Test-Path -LiteralPath $state)) { throw "install state was not created" }
    if ((Get-Content -LiteralPath $state -Raw) -match 'password|psk|token') { throw "install state contains secret-shaped data" }

    $result = Invoke-Installer @("-CorePath", $oldCore, "-Check", "-Version", "2.0.1")
    if (-not $result.Success -or $result.Output -notmatch "SMP3 MIHOMO CORE: INSTALLED" -or $result.Output -notmatch "Backup available: YES") {
        throw "check output failed: $($result.Output)"
    }

    $result = Invoke-Installer @("-CorePath", $oldCore, "-Update", "-Version", "2.0.1", "-NonInteractive")
    if (-not $result.Success -or $result.Output -notmatch "ALREADY UP TO DATE") { throw "same-version update was not a no-op" }

    $result = Invoke-Installer @("-CorePath", $oldCore, "-Restore", "-NonInteractive")
    if (-not $result.Success -or (Get-Content -LiteralPath $oldCore -Raw) -ne "original-core") { throw "restore did not recover exact original" }

    $knownHash = Get-TestSha256 (Join-Path $Assets "mihomo-smp3-windows-amd64.exe")
    Set-Content -LiteralPath (Join-Path $Assets "mihomo-smp3-windows-amd64.exe") -Value "corrupt" -NoNewline
    $result = Invoke-Installer @("-CorePath", $oldCore, "-Update", "-Version", "2.0.1", "-NonInteractive")
    if ($result.Success -or $result.Output -notmatch "CHECKSUM_MISMATCH") { throw "checksum mismatch was not fail-closed" }
    if ((Get-Content -LiteralPath $oldCore -Raw) -ne "original-core") { throw "checksum failure changed target" }

    $currentProfile = $env:USERPROFILE
    $env:USERPROFILE = $Profile
    Set-Content -LiteralPath (Join-Path $TestRoot "mihomo.exe") -Value "one" -NoNewline
    Set-Content -LiteralPath (Join-Path $Profile "mihomo.exe") -Value "two" -NoNewline
    Push-Location $TestRoot
    try {
        $result = Invoke-Installer @("-Check")
    } finally {
        Pop-Location
        $env:USERPROFILE = $currentProfile
    }
    if ($result.Success -or $result.Output -notmatch "STOP: MULTIPLE_MIHOMO_CORES") { throw "multiple-core detection was not fail-safe" }

    if ((Get-Content -LiteralPath $Installer -Raw) -notmatch "CORE_SUPERVISOR_AUTO_RESTART") { throw "supervisor restart guard missing" }
    if ((Get-Content -LiteralPath $Installer -Raw) -notmatch "Get-CimInstance Win32_Process") { throw "CIM process discovery missing" }
    Write-Output "POWERSHELL_INSTALLER_TESTS=PASS"
} finally {
    Remove-Item -LiteralPath $TestRoot -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item Env:SMP3_INSTALLER_TEST_RELEASE_JSON -ErrorAction SilentlyContinue
    Remove-Item Env:SMP3_INSTALLER_TEST_ASSET_DIR -ErrorAction SilentlyContinue
}
