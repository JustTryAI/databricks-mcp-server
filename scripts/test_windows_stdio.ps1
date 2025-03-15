#!/usr/bin/env pwsh
# Test script for Windows stdio handling in the Databricks MCP server

# Get the directory of this script
$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = Split-Path $scriptPath -Parent
$rootDir = Split-Path $scriptDir -Parent

# Change to the root directory
Set-Location $rootDir

# Check if the virtual environment exists
if (-not (Test-Path -Path ".venv")) {
    Write-Host "Virtual environment not found. Please create it first:"
    Write-Host "uv venv"
    exit 1
}

# Activate virtual environment
. .\.venv\Scripts\Activate.ps1

# Run the test
Write-Host "Running Windows stdio test at $(Get-Date)"
python -m tests.test_stdio_windows

# Output test result
if ($LASTEXITCODE -eq 0) {
    Write-Host "Test passed successfully!" -ForegroundColor Green
} else {
    Write-Host "Test failed with exit code $LASTEXITCODE" -ForegroundColor Red
} 