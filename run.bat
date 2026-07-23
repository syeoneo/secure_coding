@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [1/4] 가상환경을 생성합니다...
  py -m venv .venv
  if errorlevel 1 python -m venv .venv
)

if not exist ".venv\Scripts\python.exe" (
  echo 가상환경 생성에 실패했습니다. Python 설치 상태를 확인해주세요.
  pause
  exit /b 1
)

echo [2/4] 필요한 패키지를 확인합니다...
".venv\Scripts\python.exe" -c "import flask, flask_socketio, flask_wtf, dotenv, PIL" >nul 2>&1
if errorlevel 1 (
  echo 필요한 패키지를 설치합니다...
  ".venv\Scripts\python.exe" -m pip install -r requirements.txt
  if errorlevel 1 (
    echo 패키지 설치에 실패했습니다.
    pause
    exit /b 1
  )
)

if not exist ".env" (
  echo [3/4] SECRET_KEY를 생성합니다...
  ".venv\Scripts\python.exe" -c "import secrets, pathlib; pathlib.Path('.env').write_text('SECRET_KEY='+secrets.token_hex(32)+'\n', encoding='utf-8')"
)

echo [4/4] 햇켓 서버를 실행합니다...
echo 브라우저 주소: http://127.0.0.1:5000
".venv\Scripts\python.exe" app.py

pause
