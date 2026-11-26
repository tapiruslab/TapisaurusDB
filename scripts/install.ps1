# ==============================================================================
# TapisaurusDB Pre-Built Binary Installer for Windows (x86_64 PowerShell)
#
# Usage:
#   irm https://raw.githubusercontent.com/tapiruslab/TapisaurusDB/main/scripts/install.ps1 | iex
#
# Optional environment variables:
#   $env:TAPISAURUS_VERSION = "v0.1.0"        # Pin release tag (default: latest)
#   $env:TAPISAURUS_INSTALL_DIR = "C:\bin"    # Override install directory
#   $env:TAPISAURUS_SKIP_VERIFY = "1"         # Skip SHA-256 check (not recommended)
#
# Copyright (c) 2026 Ahmad Faiz <faiz@tapirusdb.com>
# ==============================================================================

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$repo = "tapiruslab/TapisaurusDB"
$version = if ($env:TAPISAURUS_VERSION) { $env:TAPISAURUS_VERSION } else { "latest" }
$asset = "tapisaurus-windows-x86_64.zip"

$baseUrl = if ($version -eq "latest") {
    "https://github.com/$repo/releases/latest/download"
} else {
    "https://github.com/$repo/releases/download/$version"
}

$archiveUrl = "$baseUrl/$asset"
$shaUrl = "$archiveUrl.sha256"

$installDir = if ($env:TAPISAURUS_INSTALL_DIR) {
    $env:TAPISAURUS_INSTALL_DIR
} else {
    Join-Path $HOME ".tapisaurus\bin"
}

if (!(Test-Path $installDir)) {
    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
}

$tempDir = Join-Path $env:TEMP ("tapisaurus_install_" + [Guid]::NewGuid().ToString("N").Substring(0, 8))
New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

function Download-File {
    param([string]$Url, [string]$OutFile)
    if (Get-Command curl.exe -ErrorAction SilentlyContinue) {
        & curl.exe -fsSL --retry 3 --retry-delay 1 -o $OutFile $Url
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to download $Url (curl exit code $LASTEXITCODE)"
        }
    } else {
        Invoke-WebRequest -Uri $Url -OutFile $OutFile -UseBasicParsing
    }
}

try {
    $archivePath = Join-Path $tempDir $asset
    $shaPath = Join-Path $tempDir "$asset.sha256"

    Write-Host "[tapisaurus] Downloading $asset ($version)..." -ForegroundColor Cyan
    Download-File -Url $archiveUrl -OutFile $archivePath

    if ($env:TAPISAURUS_SKIP_VERIFY -ne "1") {
        Write-Host "[tapisaurus] Verifying SHA-256 checksum..." -ForegroundColor Cyan
        Download-File -Url $shaUrl -OutFile $shaPath
        $expected = ((Get-Content $shaPath -Raw).Trim() -split '\s+')[0].ToLowerInvariant()
        $actual = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ([string]::IsNullOrWhiteSpace($expected) -or $expected -ne $actual) {
            throw "SHA-256 verification failed! Expected $expected, got $actual"
        }
        Write-Host "[tapisaurus] SHA-256 verified ($actual)." -ForegroundColor Green
    }

    $extractDir = Join-Path $tempDir "extracted"
    Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force

    $binaryPath = Join-Path $extractDir "tapisaurus.exe"
    if (!(Test-Path $binaryPath)) {
        throw "Downloaded archive did not contain 'tapisaurus.exe'."
    }

    $destBinary = Join-Path $installDir "tapisaurus.exe"
    Copy-Item -Path $binaryPath -Destination $destBinary -Force

    if ($env:TAPISAURUS_NO_MODIFY_PATH -ne "1") {
        $userPath = [Environment]::GetEnvironmentVariable("Path", [EnvironmentVariableTarget]::User)
        if ([string]::IsNullOrEmpty($userPath)) {
            [Environment]::SetEnvironmentVariable("Path", $installDir, [EnvironmentVariableTarget]::User)
            $env:Path = "$env:Path;$installDir"
            Write-Host "[tapisaurus] Added $installDir to User PATH." -ForegroundColor Green
        } elseif ($userPath -notlike "*$installDir*") {
            [Environment]::SetEnvironmentVariable("Path", "$userPath;$installDir", [EnvironmentVariableTarget]::User)
            $env:Path = "$env:Path;$installDir"
            Write-Host "[tapisaurus] Added $installDir to User PATH." -ForegroundColor Green
        }
    }

    $verOut = & $destBinary --version 2>$null
    if (-not $verOut) { $verOut = $version }

    Write-Host ""
    Write-Host "[tapisaurus] Installed successfully: $verOut -> $destBinary" -ForegroundColor Green
    Write-Host ""
    Write-Host "Quick start (open a new terminal if 'tapisaurus' is not yet recognized):" -ForegroundColor Yellow
    Write-Host "  tapisaurus serve     # Launch all 5 wire gateways (PG 5432, MySQL 3306, Mongo 27017, gRPC 50051, REST 27018)"
    Write-Host "  tapisaurus shell     # Interactive SQL / Mongo / openCypher / TapisaurusQL REPL"
    Write-Host "  tapisaurus doctor    # Probe active gateways and storage health"
    Write-Host ""
} finally {
    if (Test-Path $tempDir) {
        Remove-Item -Recurse -Force $tempDir
    }
}
