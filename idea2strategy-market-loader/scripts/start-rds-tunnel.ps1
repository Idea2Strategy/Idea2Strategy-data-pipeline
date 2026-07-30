param(
    [Parameter(Mandatory = $true)]
    [string]$TargetInstanceId,
    [Parameter(Mandatory = $true)]
    [string]$RdsEndpoint,
    [string]$Profile = "idea2strategy-dev",
    [string]$Region = "ap-northeast-2",
    [int]$LocalPort = 15432
)

$ErrorActionPreference = "Stop"
aws ssm start-session `
    --target $TargetInstanceId `
    --document-name AWS-StartPortForwardingSessionToRemoteHost `
    --parameters "host=$RdsEndpoint,portNumber=5432,localPortNumber=$LocalPort" `
    --profile $Profile `
    --region $Region
