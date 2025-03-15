#!/usr/bin/env pwsh
# Wrapper script to run the Windows stdio test

# Get the directory of this script
$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = Split-Path $scriptPath -Parent

# Change to the script directory
Set-Location $scriptDir

# Run the actual test script
& "$scriptDir\scripts\test_windows_stdio.ps1" 