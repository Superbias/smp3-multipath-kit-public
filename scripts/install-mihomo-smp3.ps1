[CmdletBinding()]
param(
    [string]$CorePath,
    [switch]$Check,
    [switch]$Update,
    [switch]$Restore,
    [string]$Version,
    [switch]$NonInteractive,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$Repository = "Superbias/smp3-multipath-kit-public"
$AssetName = "mihomo-smp3-windows-amd64.exe"
$ApiBase = if ($env:SMP3_RELEASE_API_BASE) { $env:SMP3_RELEASE_API_BASE.TrimEnd('/') } else { "https://api.github.com/repos/$Repository" }
$KnownReleaseSha256 = @{
    "2.0.0" = "df364855c899648b6e2d4d785a702514f1fc87a64a662fab28489be5b234ffd3"
    "2.0.1" = "df364855c899648b6e2d4d785a702514f1fc87a64a662fab28489be5b234ffd3"
}

function Stop-WithCode {
    param([string]$Code, [string]$Message)
    Write-Error "$Code`n$Message"
    throw "$Code"
}

function Assert-Amd64 {
    if (-not [Environment]::Is64BitOperatingSystem) {
        Stop-WithCode "STOP: UNSUPPORTED_ARCH" "This installer supports Windows amd64 only."
    }
}

function Get-FullPath {
    param([string]$Path)
    return [IO.Path]::GetFullPath($Path)
}

function Get-Sha256 {
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

function Get-ProcessSnapshot {
    try {
        return @(Get-CimInstance Win32_Process | Where-Object {
            $_.Name -ieq "mihomo.exe" -and $_.ExecutablePath
        } | ForEach-Object {
            [PSCustomObject]@{
                ProcessId = [int]$_.ProcessId
                ExecutablePath = Get-FullPath $_.ExecutablePath
                CommandLine = [string]$_.CommandLine
            }
        })
    } catch {
        Stop-WithCode "STOP: PROCESS_DISCOVERY_FAILED" $_.Exception.Message
    }
}

function Get-CommonCoreCandidates {
    $paths = New-Object System.Collections.Generic.List[string]
    $paths.Add((Join-Path (Get-Location).Path "mihomo.exe"))
    if ($env:LOCALAPPDATA) { $paths.Add((Join-Path $env:LOCALAPPDATA "Mihomo\mihomo.exe")) }
    if ($env:ProgramFiles) { $paths.Add((Join-Path $env:ProgramFiles "Mihomo\mihomo.exe")) }
    if ($env:ProgramData) { $paths.Add((Join-Path $env:ProgramData "Mihomo\mihomo.exe")) }
    if ($env:USERPROFILE) { $paths.Add((Join-Path $env:USERPROFILE "mihomo.exe")) }
    return @($paths | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | ForEach-Object { Get-FullPath $_ } | Sort-Object -Unique)
}

function Resolve-CorePath {
    if ($CorePath) {
        return Get-FullPath $CorePath
    }

    $running = @(Get-ProcessSnapshot | Select-Object -ExpandProperty ExecutablePath -Unique)
    $common = @(Get-CommonCoreCandidates)
    $all = @($running + $common | Sort-Object -Unique)
    if ($all.Count -gt 1) {
        Write-Output "Multiple Mihomo cores detected:"
        for ($index = 0; $index -lt $all.Count; $index++) {
            Write-Output ("[{0}] {1}" -f ($index + 1), $all[$index])
        }
        Stop-WithCode "STOP: MULTIPLE_MIHOMO_CORES" "Please specify -CorePath with the exact executable path."
    }
    if ($all.Count -eq 1) {
        return $all[0]
    }
    return $null
}

function Get-ProcessesForPath {
    param([string]$Path)
    $full = Get-FullPath $Path
    return @(Get-ProcessSnapshot | Where-Object { $_.ExecutablePath -ieq $full })
}

function Stop-SelectedCore {
    param([string]$Path)
    $processes = @(Get-ProcessesForPath $Path)
    foreach ($process in $processes) {
        Write-Host ("Stopping selected Mihomo PID={0} Path={1}" -f $process.ProcessId, $process.ExecutablePath)
        Write-Host ("CommandLine: {0}" -f $process.CommandLine)
        Stop-Process -Id $process.ProcessId -Force
    }
    if ($processes.Count -gt 0) {
        Start-Sleep -Milliseconds 1200
        $restarted = @(Get-ProcessesForPath $Path)
        if ($restarted.Count -gt 0) {
            Stop-WithCode "STOP: CORE_SUPERVISOR_AUTO_RESTART" "The selected Mihomo path was relaunched. Close or stop its parent GUI/service and retry."
        }
    }
    return $processes
}

function Get-VersionText {
    param([string]$Path)
    $stamp = [Guid]::NewGuid().ToString('N')
    $stdout = Join-Path $env:TEMP ("smp3-mihomo-version-$stamp.out")
    $stderr = Join-Path $env:TEMP ("smp3-mihomo-version-$stamp.err")
    try {
        $process = Start-Process -FilePath $Path -ArgumentList "-v" -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden -PassThru
        if (-not $process.WaitForExit(5000)) {
            try { $process.Kill() } catch {}
            return ""
        }
        $out = if (Test-Path -LiteralPath $stdout) { Get-Content -LiteralPath $stdout -Raw } else { "" }
        $err = if (Test-Path -LiteralPath $stderr) { Get-Content -LiteralPath $stderr -Raw } else { "" }
        return (($out + "`n" + $err).Trim())
    } catch {
        return ""
    } finally {
        Remove-Item -LiteralPath $stdout, $stderr -Force -ErrorAction SilentlyContinue
    }
}

function Get-Release {
    param([string]$RequestedVersion)
    try {
        if ($env:SMP3_INSTALLER_TEST_RELEASE_JSON) {
            $json = Get-Content -LiteralPath $env:SMP3_INSTALLER_TEST_RELEASE_JSON -Raw | ConvertFrom-Json
        } elseif ($RequestedVersion) {
            $tag = if ($RequestedVersion.StartsWith('v')) { $RequestedVersion } else { "v$RequestedVersion" }
            $json = Invoke-RestMethod -Uri "$ApiBase/releases/tags/$tag" -Headers @{ 'User-Agent' = 'smp3-installer' }
        } else {
            $json = Invoke-RestMethod -Uri "$ApiBase/releases/latest" -Headers @{ 'User-Agent' = 'smp3-installer' }
        }
    } catch {
        Stop-WithCode "STOP: RELEASE_FETCH_FAILED" $_.Exception.Message
    }
    if ($json.draft -or $json.prerelease) {
        Stop-WithCode "STOP: RELEASE_NOT_STABLE" "The selected GitHub Release is draft or prerelease."
    }
    if (-not $json.tag_name) {
        Stop-WithCode "STOP: RELEASE_FETCH_FAILED" "GitHub Release has no tag_name."
    }
    return $json
}

function Find-ReleaseAsset {
    param([object]$Release, [string]$Name)
    foreach ($asset in @($Release.assets)) {
        if ([string]$asset.name -ceq $Name) {
            return $asset
        }
    }
    Stop-WithCode "STOP: RELEASE_ASSET_NOT_FOUND" "Missing exact stable Release asset: $Name"
}

function Download-ReleaseAsset {
    param([object]$Asset, [string]$Destination)
    if ($env:SMP3_INSTALLER_TEST_ASSET_DIR) {
        $fixture = Join-Path $env:SMP3_INSTALLER_TEST_ASSET_DIR ([IO.Path]::GetFileName($Asset.name))
        if (-not (Test-Path -LiteralPath $fixture -PathType Leaf)) {
            Stop-WithCode "STOP: RELEASE_ASSET_NOT_FOUND" "Test fixture is missing: $fixture"
        }
        Copy-Item -LiteralPath $fixture -Destination $Destination -Force
        return
    }
    try {
        Invoke-WebRequest -Uri $Asset.browser_download_url -Headers @{ 'User-Agent' = 'smp3-installer' } -OutFile $Destination -UseBasicParsing
    } catch {
        Stop-WithCode "STOP: RELEASE_DOWNLOAD_FAILED" $_.Exception.Message
    }
}

function Get-ManifestSha {
    param([string]$ManifestPath, [string]$AssetName)
    foreach ($line in Get-Content -LiteralPath $ManifestPath) {
        if ($line -match '^\s*([0-9A-Fa-f]{64})\s+\*?(.+?)\s*$') {
            if ($matches[2] -ceq $AssetName) {
                return $matches[1].ToLowerInvariant()
            }
        }
    }
    Stop-WithCode "STOP: CHECKSUM_ENTRY_NOT_FOUND" "SHA256SUMS has no exact entry for $AssetName"
}

function Read-State {
    param([string]$Path)
    $statePath = Join-Path (Split-Path -Parent $Path) "smp3-install-state.json"
    if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) { return $null }
    try { return Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json } catch { return $null }
}

function Write-State {
    param([string]$CorePath, [hashtable]$State)
    $statePath = Join-Path (Split-Path -Parent $CorePath) "smp3-install-state.json"
    $temporary = "$statePath.tmp"
    ($State | ConvertTo-Json -Depth 4) | Set-Content -LiteralPath $temporary -Encoding UTF8
    Move-Item -LiteralPath $temporary -Destination $statePath -Force
}

function Assert-ValidCoreFile {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        Stop-WithCode "STOP: CORE_PATH_REQUIRED" "Mihomo executable was not found: $Path"
    }
    $item = Get-Item -LiteralPath $Path
    if (-not $item.PSIsContainer -and $item.Length -gt 0) { return }
    Stop-WithCode "STOP: INVALID_CORE" "CorePath must be a non-empty regular file."
}

function Replace-Core {
    param([string]$Path, [string]$Downloaded, [string]$ReleaseVersion, [string]$InstalledSha)
    $full = Get-FullPath $Path
    $directory = Split-Path -Parent $full
    $backupDirectory = Join-Path $directory "smp3-backup"
    New-Item -ItemType Directory -Path $backupDirectory -Force | Out-Null
    $timestamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
    $leaf = Split-Path -Leaf $full
    $backup = Join-Path $backupDirectory "$leaf.$timestamp.bak"
    $oldSha = Get-Sha256 $full
    $moved = $false
    try {
        $processes = @(Stop-SelectedCore $full)
        Move-Item -LiteralPath $full -Destination $backup
        $moved = $true
        if ((Get-Sha256 $backup) -ne $oldSha) {
            throw "backup SHA256 verification failed"
        }
        Move-Item -LiteralPath $Downloaded -Destination $full
        if ((Get-Sha256 $full) -ne $InstalledSha) {
            throw "installed SHA256 verification failed"
        }
        Write-State $full @{
            original_path = $full
            original_sha256 = $oldSha
            backup_path = $backup
            installed_version = $ReleaseVersion
            installed_sha256 = $InstalledSha
            installed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
            last_operation = 'install-or-update'
        }
        Write-Output "Installed SHA256: $InstalledSha"
        if ($processes.Count -gt 0) {
            Write-Output "Selected Mihomo was stopped safely; start its owning GUI/service manually."
        }
    } catch {
        if ($moved) {
            if (Test-Path -LiteralPath $full) { Remove-Item -LiteralPath $full -Force }
            if (Test-Path -LiteralPath $backup) { Move-Item -LiteralPath $backup -Destination $full -Force }
        }
        throw
    } finally {
        if (Test-Path -LiteralPath $Downloaded) { Remove-Item -LiteralPath $Downloaded -Force -ErrorAction SilentlyContinue }
    }
}

function Restore-Core {
    param([string]$Path)
    $full = Get-FullPath $Path
    Assert-ValidCoreFile $full
    $state = Read-State $full
    if (-not $state -or -not $state.backup_path -or -not (Test-Path -LiteralPath $state.backup_path -PathType Leaf)) {
        Stop-WithCode "STOP: BACKUP_NOT_FOUND" "No recorded verified Mihomo backup exists for $full"
    }
    $backupSha = Get-Sha256 $state.backup_path
    if ($backupSha -ne ([string]$state.original_sha256).ToLowerInvariant()) {
        Stop-WithCode "STOP: BACKUP_HASH_MISMATCH" "Recorded original backup SHA256 does not match."
    }
    $directory = Split-Path -Parent $full
    $backupDirectory = Join-Path $directory "smp3-backup"
    $timestamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
    $currentBackup = Join-Path $backupDirectory ((Split-Path -Leaf $full) + ".current.$timestamp.bak")
    $temporary = Join-Path $directory ((Split-Path -Leaf $full) + ".restore.$timestamp.tmp")
    try {
        Stop-SelectedCore $full | Out-Null
        Copy-Item -LiteralPath $state.backup_path -Destination $temporary
        if ((Get-Sha256 $temporary) -ne $state.original_sha256) { throw "restore temporary SHA256 verification failed" }
        Move-Item -LiteralPath $full -Destination $currentBackup
        Move-Item -LiteralPath $temporary -Destination $full
        if ((Get-Sha256 $full) -ne ([string]$state.original_sha256).ToLowerInvariant()) { throw "restored SHA256 verification failed" }
        Write-State $full @{
            original_path = $full
            original_sha256 = $state.original_sha256
            backup_path = $state.backup_path
            installed_version = 'original-restored'
            installed_sha256 = $state.original_sha256
            restored_at_utc = (Get-Date).ToUniversalTime().ToString('o')
            current_smp3_backup = $currentBackup
            last_operation = 'restore'
        }
        Write-Output "Restored original SHA256: $($state.original_sha256)"
    } catch {
        if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue }
        if (-not (Test-Path -LiteralPath $full) -and (Test-Path -LiteralPath $currentBackup)) {
            Move-Item -LiteralPath $currentBackup -Destination $full -Force
        }
        throw
    }
}

function Check-Core {
    param([string]$Path)
    if (-not $Path) {
        Write-Output "SMP3 MIHOMO CORE: NOT DETECTED"
        return
    }
    Assert-ValidCoreFile $Path
    $full = Get-FullPath $Path
    $sha = Get-Sha256 $full
    $processes = @(Get-ProcessesForPath $full)
    $state = Read-State $full
    $knownMatch = $false
    $knownVersion = ''
    foreach ($key in $KnownReleaseSha256.Keys) {
        if ($KnownReleaseSha256[$key] -eq $sha) { $knownMatch = $true; $knownVersion = $key }
    }
    try {
        $release = Get-Release $Version
        $asset = Find-ReleaseAsset $release $AssetName
        $manifestAsset = Find-ReleaseAsset $release "SHA256SUMS"
        $manifestPath = Join-Path $env:TEMP (([Guid]::NewGuid().ToString('N')) + '.sums')
        try {
            Download-ReleaseAsset $manifestAsset $manifestPath
            if ((Get-ManifestSha $manifestPath $AssetName) -eq $sha) {
                $knownMatch = $true
                $knownVersion = [string]$release.tag_name
            }
        } finally {
            Remove-Item -LiteralPath $manifestPath -Force -ErrorAction SilentlyContinue
        }
    } catch {
        Write-Output "Release metadata: unavailable (check is read-only)"
    }
    $versionText = Get-VersionText $full
    Write-Output "Core path: $full"
    Write-Output ("Running: {0}" -f ($(if ($processes.Count -gt 0) { 'YES' } else { 'NO' })))
    Write-Output "Current SHA256: $sha"
    Write-Output ("Known SMP3 release match: {0}" -f ($(if ($knownMatch) { "YES ($knownVersion)" } else { 'NO' })))
    Write-Output ("Backup available: {0}" -f ($(if ($state -and $state.backup_path -and (Test-Path -LiteralPath $state.backup_path -PathType Leaf)) { 'YES' } else { 'NO' })))
    Write-Output ("Installed version: {0}" -f ($(if ($state) { $state.installed_version } elseif ($versionText) { $versionText.Split("`n")[0] } else { 'UNKNOWN' })))
    Write-Output "SMP3 MIHOMO CORE: INSTALLED"
}

function Install-Or-Update {
    param([string]$Path)
    Assert-ValidCoreFile $Path
    $full = Get-FullPath $Path
    $release = Get-Release $Version
    $targetVersion = ([string]$release.tag_name).TrimStart('v')
    $asset = Find-ReleaseAsset $release $AssetName
    $manifestAsset = Find-ReleaseAsset $release "SHA256SUMS"
    $directory = Split-Path -Parent $full
    $downloaded = Join-Path $directory ("." + [IO.Path]::GetRandomFileName() + ".download")
    $manifestPath = Join-Path $env:TEMP (([Guid]::NewGuid().ToString('N')) + '.sums')
    try {
        Download-ReleaseAsset $manifestAsset $manifestPath
        $expected = Get-ManifestSha $manifestPath $AssetName
        Download-ReleaseAsset $asset $downloaded
        $actual = Get-Sha256 $downloaded
        if ($actual -ne $expected) {
            Stop-WithCode "STOP: CHECKSUM_MISMATCH" "Downloaded Mihomo SHA256 does not match exact SHA256SUMS entry."
        }
        $current = Get-Sha256 $full
        if ($current -eq $expected) {
            Write-Output "ALREADY UP TO DATE: $targetVersion"
            return
        }
        $versionText = Get-VersionText $downloaded
        if ($versionText) { Write-Output ("Downloaded core version: {0}" -f $versionText.Split("`n")[0]) }
        Replace-Core $full $downloaded $targetVersion $expected
        Write-Output "SMP3 MIHOMO CORE: INSTALLED"
    } finally {
        Remove-Item -LiteralPath $downloaded, $manifestPath -Force -ErrorAction SilentlyContinue
    }
}

Assert-Amd64
$selectedPath = Resolve-CorePath

if (($Check -and ($Update -or $Restore)) -or ($Update -and $Restore)) {
    Stop-WithCode "STOP: INVALID_ARGUMENTS" "Choose only one of -Check, -Update, or -Restore."
}

if ($Check) {
    Check-Core $selectedPath
    exit 0
}

if (-not $selectedPath) {
    Stop-WithCode "STOP: CORE_PATH_REQUIRED" "No Mihomo core was found. Specify -CorePath with the exact mihomo.exe path."
}

if ($Restore) {
    Restore-Core $selectedPath
    Write-Output "SMP3 MIHOMO CORE: RESTORED"
    exit 0
}

Install-Or-Update $selectedPath
