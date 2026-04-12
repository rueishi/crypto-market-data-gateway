# Runs the gateway against Coinbase Exchange Direct Feed L2 using the sample
# YAML config. Invoke from the repository root:
#
#   .\scripts\run-coinbase-l2-direct.ps1
#
# Required environment variables:
#   COINBASE_EXCHANGE_API_KEY
#   COINBASE_EXCHANGE_API_SECRET
#   COINBASE_EXCHANGE_API_PASSPHRASE
#
# Optional:
#   -ConfigPath <path>  Override the YAML config path.
#
# The script builds target/classes and a Maven dependency classpath, then starts
# GatewayBootstrap with -Dgateway.config. It leaves the process running until
# Ctrl+C/SIGTERM, at which point the gateway shutdown hook should drain.

param(
    [string] $ConfigPath = "config/coinbase-l2-direct.yaml"
)

$ErrorActionPreference = "Stop"

function Require-Env {
    param([string] $Name)

    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name))) {
        throw "Missing required environment variable: $Name"
    }
}

Require-Env "COINBASE_EXCHANGE_API_KEY"
Require-Env "COINBASE_EXCHANGE_API_SECRET"
Require-Env "COINBASE_EXCHANGE_API_PASSPHRASE"

if (-not (Test-Path -LiteralPath $ConfigPath)) {
    throw "Config file not found: $ConfigPath"
}

New-Item -ItemType Directory -Force -Path "target", "var/run/coinbase-l2-direct" | Out-Null

mvn -q -DskipTests package dependency:build-classpath "-Dmdep.outputFile=target/runtime-classpath.txt"

$dependencyClasspath = (Get-Content -LiteralPath "target/runtime-classpath.txt" -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($dependencyClasspath)) {
    throw "Maven did not write target/runtime-classpath.txt"
}

$runtimeClasspath = "target/classes;$dependencyClasspath"

java "-Dgateway.config=$ConfigPath" `
    -cp $runtimeClasspath `
    io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap
