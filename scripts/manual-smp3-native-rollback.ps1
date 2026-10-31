param(
    [ValidateSet('Plan','Backup','Restore')][string]$Action = 'Plan',
    [string]$BackupDirectory,
    [string]$ExpectedCandidateSHA256
)
$ErrorActionPreference = 'Stop'
$installed = 'C:\Program Files\Clash Party\resources\sidecar\mihomo.exe'
$oldSHA256 = '97b5ec71e54efa02f01d4e87ad1f13aaf3224cde2346eab102f89e78e608db46'
if ($Action -eq 'Plan') {
    [pscustomobject]@{
        productionActions = 'NONE'
        binary = $installed
        oldSHA256 = $oldSHA256
        prerequisites = 'Explicit human approval; manually exit Clash Party; preserve active profile separately; verify candidate and old backup hashes.'
        sessionImpact = 'Existing Native sessions are interrupted. No automatic route migration or profile rollback.'
        steps = 'Backup before installing a candidate; Restore only after manually quitting Clash Party and reviewing the expected candidate SHA256.'
    } | ConvertTo-Json
    exit 0
}
if ([string]::IsNullOrWhiteSpace($env:SMP3_OPERATOR_APPROVAL)) { throw 'Human authorization record SMP3_OPERATOR_APPROVAL is required.' }
if ([string]::IsNullOrWhiteSpace($BackupDirectory)) { throw 'A reviewed backup directory is required.' }
$targetDirectory = [System.IO.Path]::GetFullPath($BackupDirectory)
if ($targetDirectory -eq [System.IO.Path]::GetPathRoot($targetDirectory)) { throw 'A drive root cannot be a backup directory.' }
$running = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'mihomo.exe' -and $_.ExecutablePath -eq $installed })
if ($Action -eq 'Restore' -and $running.Count -gt 0) { throw 'Manually exit Clash Party first; this script does not kill or restart it.' }
$backupFile = Join-Path $targetDirectory 'mihomo-old.exe'
if ($Action -eq 'Backup') {
    if (Test-Path -LiteralPath $targetDirectory) { throw 'Refusing to overwrite an existing backup directory.' }
    if ((Get-FileHash -LiteralPath $installed -Algorithm SHA256).Hash.ToLowerInvariant() -ne $oldSHA256) { throw 'Installed binary is not the inspected old Native.' }
    New-Item -ItemType Directory -Path $targetDirectory | Out-Null
    Copy-Item -LiteralPath $installed -Destination $backupFile
    if ((Get-FileHash -LiteralPath $backupFile -Algorithm SHA256).Hash.ToLowerInvariant() -ne $oldSHA256) { throw 'Backup hash mismatch.' }
    [pscustomobject]@{action='Backup';path=$backupFile;sha256=$oldSHA256} | ConvertTo-Json
    exit 0
}
if ($ExpectedCandidateSHA256 -notmatch '^[a-fA-F0-9]{64}$') { throw 'A reviewed candidate SHA256 is required for Restore.' }
if ((Get-FileHash -LiteralPath $installed -Algorithm SHA256).Hash.ToLowerInvariant() -ne $ExpectedCandidateSHA256.ToLowerInvariant()) { throw 'Installed candidate hash mismatch.' }
if ((Get-FileHash -LiteralPath $backupFile -Algorithm SHA256).Hash.ToLowerInvariant() -ne $oldSHA256) { throw 'Old backup hash mismatch.' }
$temporaryFile = $installed + '.rollback-reviewed.tmp'
if (Test-Path -LiteralPath $temporaryFile) { throw 'Refusing to overwrite an existing temporary file.' }
Copy-Item -LiteralPath $backupFile -Destination $temporaryFile
if ((Get-FileHash -LiteralPath $temporaryFile -Algorithm SHA256).Hash.ToLowerInvariant() -ne $oldSHA256) { throw 'Temporary restore hash mismatch.' }
Move-Item -LiteralPath $temporaryFile -Destination $installed -Force
if ((Get-FileHash -LiteralPath $installed -Algorithm SHA256).Hash.ToLowerInvariant() -ne $oldSHA256) { throw 'Restored binary hash mismatch.' }
Write-Output 'Old Native restored. Manually review the profile and restart Clash Party; existing sessions are not preserved.'
