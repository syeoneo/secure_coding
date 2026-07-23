$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
    Write-Host "[1/4] 가상환경을 생성합니다..."
    try {
        py -m venv .venv
    }
    catch {
        python -m venv .venv
    }
}

Write-Host "[2/4] 필요한 패키지를 확인합니다..."
& $venvPython -c "import flask, flask_socketio, flask_wtf, dotenv, PIL" 2>$null
if ($LASTEXITCODE -ne 0) {
    & $venvPython -m pip install -r requirements.txt
}

if (-not (Test-Path ".env")) {
    Write-Host "[3/4] SECRET_KEY를 생성합니다..."
    & $venvPython -c "import secrets, pathlib; pathlib.Path('.env').write_text('SECRET_KEY='+secrets.token_hex(32)+'\n', encoding='utf-8')"
}

Write-Host "[4/4] 햇켓 서버를 실행합니다..."
Write-Host "브라우저 주소: http://127.0.0.1:5000"
& $venvPython app.py
