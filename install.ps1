$ErrorActionPreference = "Stop"

Write-Host "🚀 Installing ByteORM..." -ForegroundColor Cyan

$cargoPath = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargoPath) {
    Write-Host "❌ Cargo is not installed. Please install Rust first:" -ForegroundColor Red
    Write-Host "   https://www.rust-lang.org/tools/install" -ForegroundColor Yellow
    exit 1
}

Write-Host "📦 Installing from GitHub..." -ForegroundColor Yellow
cargo install --git https://github.com/bytematebot/byteorm --force

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Installation failed!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ ByteORM installed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Usage:" -ForegroundColor Cyan
Write-Host "  byteorm push        - Push schema and generate client"
Write-Host "  byteorm reset       - Reset database"
Write-Host "  byteorm studio      - Launch ByteORM Studio"
Write-Host "  byteorm self-update - Update to latest version"
