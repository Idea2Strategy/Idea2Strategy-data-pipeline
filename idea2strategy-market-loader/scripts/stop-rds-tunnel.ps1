param(
    [Parameter(Mandatory = $true)]
    [int]$ProcessId
)

$ErrorActionPreference = "Stop"
$process = Get-Process -Id $ProcessId -ErrorAction Stop
if ($process.ProcessName -notin @("aws", "session-manager-plugin")) {
    throw "PID $ProcessId is not an AWS SSM tunnel process."
}
Stop-Process -Id $ProcessId
