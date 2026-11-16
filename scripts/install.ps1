# ==============================================================================
# 🔥 TapisaurusDB Universal Installer for Windows (PowerShell)
#
# Usage:
#   iwr -useb https://raw.githubusercontent.com/tapiruslab/TapisaurusDB/main/scripts/install.ps1 | iex
#
# Architected by Ahmad Faiz <faiz@tapirusdb.com>
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host "╔══════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  🔥 TapisaurusDB — The AI-Native NoSQL Database Engine Installer   ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$installDir = "$HOME\.tapisaurus\bin"
if (!(Test-Path $installDir)) {
    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
}

Write-Host "ℹ️  Installation Directory: $installDir" -ForegroundColor Blue

# Check if Rust / Cargo is available
if (Get-Command cargo -ErrorAction SilentlyContinue) {
    Write-Host "✓ Rust toolchain detected. Building optimized binary..." -ForegroundColor Green
    $tempDir = Join-Path $env:TEMP ("tapisaurus_build_" + [Guid]::NewGuid().ToString().Substring(0,8))
    git clone --depth 1 https://github.com/tapiruslab/TapisaurusDB.git $tempDir
    Push-Location $tempDir
    cargo build --release
    Copy-Item "target\release\tapisaurus.exe" (Join-Path $installDir "tapisaurus.exe") -Force
    Pop-Location
    Remove-Item -Recurse -Force $tempDir
} else {
    Write-Host "⚡ Downloading pre-built TapisaurusDB Windows binary..." -ForegroundColor Yellow
    # Clone and build via rustup if needed
    $tempDir = Join-Path $env:TEMP ("tapisaurus_build_" + [Guid]::NewGuid().ToString().Substring(0,8))
    git clone --depth 1 https://github.com/tapiruslab/TapisaurusDB.git $tempDir
    Push-Location $tempDir
    # Fallback to local copy if available in workspace
    if (Test-Path "tapisaurus.exe") {
        Copy-Item "tapisaurus.exe" (Join-Path $installDir "tapisaurus.exe") -Force
    } else {
        cargo build --release
        Copy-Item "target\release\tapisaurus.exe" (Join-Path $installDir "tapisaurus.exe") -Force
    }
    Pop-Location
    Remove-Item -Recurse -Force $tempDir
}

# Add to User PATH if not present
$userPath = [Environment]::GetEnvironmentVariable("Path", [EnvironmentVariableTarget]::User)
if ($userPath -notlike "*$installDir*") {
    $newPath = "$userPath;$installDir"
    [Environment]::SetEnvironmentVariable("Path", $newPath, [EnvironmentVariableTarget]::User)
    $env:Path = "$env:Path;$installDir"
    Write-Host "✓ Added $installDir to User PATH permanently!" -ForegroundColor Green
}

Write-Host ""
Write-Host "🎉 TapisaurusDB was installed successfully on Windows!" -ForegroundColor Green
Write-Host ""
Write-Host "To get started, open a new Terminal or PowerShell and run:" -ForegroundColor Yellow
Write-Host "  tapisaurus shell                         # Interactive Multi-Dialect REPL" -ForegroundColor White
Write-Host "  tapisaurus serve --wire-port 27017       # Start MongoDB Wire & HTTP Server" -ForegroundColor White
Write-Host "  tapisaurus backup --output backup.json   # Create Consistent Snapshot" -ForegroundColor White
Write-Host ""
