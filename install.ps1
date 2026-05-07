$ErrorActionPreference = "Stop"

function Install-ByteOrmCompletions {
    $byteorm = Get-Command byteorm -ErrorAction SilentlyContinue
    if (-not $byteorm) {
        $candidate = Join-Path $HOME ".cargo\bin\byteorm.exe"
        if (Test-Path $candidate) {
            $byteormPath = $candidate
        } else {
            Write-Host "Autocomplete skipped: byteorm is not available on PATH yet." -ForegroundColor Yellow
            return
        }
    } else {
        $byteormPath = $byteorm.Source
    }

    $detected = "PowerShell $($PSVersionTable.PSVersion)"
    Write-Host "Detected shell: $detected" -ForegroundColor Cyan

    $shouldInstall = $false
    if ($env:BYTEORM_INSTALL_COMPLETIONS -eq "1") {
        $shouldInstall = $true
    } elseif ($env:BYTEORM_INSTALL_COMPLETIONS -eq "0") {
        $shouldInstall = $false
    } else {
        $answer = Read-Host "Install ByteORM autocomplete for PowerShell? [Y/n]"
        $shouldInstall = ($answer -eq "" -or $answer -match "^[Yy]")
    }

    if (-not $shouldInstall) {
        Write-Host "Autocomplete skipped. You can install it later with:" -ForegroundColor Yellow
        Write-Host "  byteorm completions powershell > `$HOME\.byteorm\completions\byteorm.ps1"
        return
    }

    $completionDir = Join-Path $HOME ".byteorm\completions"
    $completionPath = Join-Path $completionDir "byteorm.ps1"
    New-Item -ItemType Directory -Force -Path $completionDir | Out-Null

    & $byteormPath completions powershell | Out-File -Encoding utf8 -FilePath $completionPath

    $profilePath = $PROFILE.CurrentUserCurrentHost
    if (-not $profilePath) {
        $profilePath = $PROFILE
    }
    $profileDir = Split-Path -Parent $profilePath
    if ($profileDir) {
        New-Item -ItemType Directory -Force -Path $profileDir | Out-Null
    }
    if (-not (Test-Path $profilePath)) {
        New-Item -ItemType File -Force -Path $profilePath | Out-Null
    }

    $sourceLine = ". `"$completionPath`""
    $profileContent = Get-Content $profilePath -Raw -ErrorAction SilentlyContinue
    if ($profileContent -notlike "*$completionPath*") {
        Add-Content -Path $profilePath -Value ""
        Add-Content -Path $profilePath -Value "# ByteORM autocomplete"
        Add-Content -Path $profilePath -Value $sourceLine
    }

    try {
        . $completionPath
    } catch {
        Write-Host "Autocomplete installed. Restart PowerShell if Tab completion is not active in this session." -ForegroundColor Yellow
        return
    }

    Write-Host "Autocomplete installed for PowerShell." -ForegroundColor Green
}

Write-Host "Installing ByteORM..." -ForegroundColor Cyan

$cargoPath = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargoPath) {
    Write-Host "Cargo is not installed. Please install Rust first:" -ForegroundColor Red
    Write-Host "   https://www.rust-lang.org/tools/install" -ForegroundColor Yellow
    exit 1
}

Write-Host "Installing ByteORM package from GitHub..." -ForegroundColor Yellow
cargo install --git https://github.com/bytematebot/byteorm --package byteorm --bin byteorm --force

if ($LASTEXITCODE -ne 0) {
    Write-Host "Installation failed!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "ByteORM installed successfully!" -ForegroundColor Green
Install-ByteOrmCompletions

Write-Host ""
Write-Host "Usage:" -ForegroundColor Cyan
Write-Host "  byteorm init        - Initialize byteorm.toml and starter schema"
Write-Host "  byteorm generate    - Generate client from schema"
Write-Host "  byteorm push        - Push schema and generate client"
Write-Host "  byteorm doctor      - Show resolved config and paths"
Write-Host "  byteorm completions - Generate shell autocomplete scripts"
Write-Host "  byteorm reset       - Reset database"
Write-Host "  byteorm self-update - Update to latest version"
