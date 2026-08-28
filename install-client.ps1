param([string]$Config = "$PSScriptRoot\config\client.json")
$ErrorActionPreference = "Stop"
$bin = "$PSScriptRoot\dist\smp3-proxy-windows-amd64.exe"
if (!(Test-Path $bin)) { throw "Build first with build.sh in WSL/Linux." }
if (!(Test-Path $Config)) { throw "Create config\client.json from client.example.json first." }

$dst = "$env:ProgramData\smp3-proxy"
$taskName = "smp3-multipath"
New-Item -ItemType Directory -Force $dst | Out-Null

# Validate the new config before replacing a running installation.
& $bin check -c $Config
if ($LASTEXITCODE -ne 0) { throw "New multipath config check failed." }

try { Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue } catch {}
try { Stop-ScheduledTask -TaskName "sing-box-multipath" -ErrorAction SilentlyContinue } catch {}
try { Unregister-ScheduledTask -TaskName "sing-box-multipath" -Confirm:$false -ErrorAction SilentlyContinue } catch {}
Get-Process "smp3-proxy","sing-box-mp" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 300

Copy-Item $bin "$dst\smp3-proxy.exe" -Force
Copy-Item $Config "$dst\config.json" -Force
& "$dst\smp3-proxy.exe" check -c "$dst\config.json"
if ($LASTEXITCODE -ne 0) { throw "Installed config check failed." }

$action = New-ScheduledTaskAction -Execute "$dst\smp3-proxy.exe" -Argument "run -c `"$dst\config.json`""
$trigger = New-ScheduledTaskTrigger -AtLogOn
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Force | Out-Null
Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 1

$p = Get-Process "smp3-proxy" -ErrorAction SilentlyContinue
if (!$p) { throw "smp3-proxy did not stay running; start it manually to inspect logs." }
Write-Host "Installed/updated. Mihomo can use SOCKS5 127.0.0.1:2080"
Write-Host "Config: $dst\config.json"
