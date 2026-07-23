# 햇켓(HATKET)

보안을 고려해 구현한 Flask 기반 중고거래 플랫폼입니다. 상품 등록과 검색, 찜, 실시간 채팅, 친구·차단, 송금, 상품·사용자 신고, 관리자 검토 기능을 제공합니다.

## 주요 기능

### 사용자
- 회원가입, 로그인, 로그아웃
- 닉네임·소개글·로그인 비밀번호 변경
- 6자리 결제 비밀번호 설정 및 변경
- 공개 프로필, 친구 요청·수락·거절·삭제
- 사용자 차단 및 차단 해제

### 상품
- 상품 등록·조회·수정·삭제
- 이미지 최대 5장 업로드
- 카테고리·상품 상태·거래 방법·거래 장소·가격 협상 설정
- 키워드 검색, 카테고리·판매 상태·정렬 필터
- 찜, 조회수, 판매 상태 변경
- 특정 상품 숨김 및 숨김 해제

### 채팅
- 전체 채팅
- 상품과 연결된 1:1 채팅
- 채팅방 접근 권한 검사
- 차단 관계 사용자 간 메시지 차단
- 읽지 않은 메시지 표시

### 송금
- 사용자명 기반 일반 송금
- 상품 가격 기반 상품 결제
- 결제 비밀번호 재인증
- 잔액 검증 및 DB 트랜잭션
- 동일 상품 중복 결제 차단
- 결제 완료 시 상품 자동 판매 완료 처리
- 사용자·관리자 송금 내역 조회

### 신고 및 관리자
- 상품 신고와 사용자 신고
- 동일 상품 또는 사용자의 여러 신고를 대상별로 묶어서 표시
- 관리자 일괄 승인·기각 및 처리 메모
- 사용자 신고 승인 시 계정 정지
- 상품 신고 승인 시 상품 숨김
- 사용자 정지·해제, 상품 숨김·복구
- 관리자 감사 로그

## 보안 구현

- Werkzeug 기반 로그인 비밀번호·결제 비밀번호 해시 저장
- 모든 HTML POST 폼에 CSRF 보호 적용
- `HttpOnly`, `SameSite=Lax` 세션 쿠키
- 2시간 세션 만료
- HTTPS 환경에서 `Secure` 쿠키를 활성화할 수 있는 환경변수
- 상품·신고·프로필·채팅 입력값 서버 측 검증
- 상품 수정·삭제 시 소유자 검증
- 채팅방 접근 권한 검증
- SQLite 파라미터 바인딩
- 서버가 결정한 상품 가격으로 결제 처리
- 이미지 확장자뿐 아니라 실제 이미지 포맷 검증
- 업로드 파일명 무작위 재생성
- 로그인 5회 실패 시 5분 잠금
- 전체 채팅 사용자별 3초, 1:1 채팅 사용자별 1초 전송 제한
- 사용자별 한 시간 최대 5건 신고 제한
- CSP, X-Frame-Options, X-Content-Type-Options 등 보안 헤더
- 400·403·404·405·413·500 사용자용 오류 페이지

## 기술 스택

- Python 3.11 이상
- Flask
- Flask-SocketIO
- Flask-WTF
- SQLite
- Pillow
- HTML, CSS, JavaScript

## 설치 및 실행

### 1. 프로젝트 폴더 이동

```powershell
cd C:\Users\user1\whs-secure-coding\secure-coding-current
```

### 2. 가상환경 생성

```powershell
python -m venv .venv
```

### 3. 의존성 설치

PowerShell 실행 정책과 관계없이 가상환경 Python을 직접 사용합니다.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 4. 환경변수 생성

`.env.example`을 복사하여 `.env` 파일을 만듭니다.

```powershell
Copy-Item .env.example .env
```

`SECRET_KEY`는 예측하기 어려운 값으로 변경해야 합니다.

```powershell
.\.venv\Scripts\python.exe -c "import secrets, pathlib; pathlib.Path('.env').write_text('SECRET_KEY='+secrets.token_hex(32)+'\nCOOKIE_SECURE=0\n', encoding='utf-8')"
```

### 5. 서버 실행

```powershell
.\.venv\Scripts\python.exe app.py
```

브라우저에서 다음 주소로 접속합니다.

```text
http://127.0.0.1:5000
```

서버 종료는 터미널에서 `Ctrl + C`를 누릅니다.

## 관리자 계정 설정

1. 웹 화면에서 일반 계정을 먼저 회원가입합니다.
2. 서버를 종료합니다.
3. 가입한 사용자명을 인자로 전달합니다.

```powershell
.\.venv\Scripts\python.exe scripts\make_admin.py 사용자명
```

4. 서버를 다시 실행하고 해당 계정으로 로그인합니다.

관리자 계정을 해제하려면 다음 명령을 사용합니다.

```powershell
.\.venv\Scripts\python.exe scripts\make_admin.py 사용자명 --remove
```

## 주요 테스트 항목

- 비로그인 사용자의 상품 등록 차단
- 다른 사용자의 상품 수정·삭제 차단
- 다른 사용자의 채팅방 접근 차단
- 본인 상품 결제 차단
- 잔액 초과 송금 차단
- 동일 상품 중복 결제 차단
- 잘못된 결제 비밀번호 차단
- 일반 사용자의 관리자 페이지 접근 차단
- 사용자 차단 후 채팅 차단
- 상품·사용자 신고 접수
- 동일 대상 신고 묶음 표시
- 관리자 승인·기각과 실제 제재 반영

상세 점검 절차는 [`docs/FINAL_MANUAL_TEST_CHECKLIST.md`](docs/FINAL_MANUAL_TEST_CHECKLIST.md)를 참고합니다.

## 제출 전 정적 검사

```powershell
.\.venv\Scripts\python.exe scripts\verify_project.py
```

Python 문법, Jinja 템플릿 구문, 필수 파일 존재 여부를 확인합니다.

## 프로젝트 구조

```text
.
├─ app.py
├─ requirements.txt
├─ .env.example
├─ scripts/
│  └─ make_admin.py
├─ static/
│  ├─ style.css
│  ├─ chat.js
│  ├─ private_chat.js
│  └─ uploads/products/
├─ templates/
├─ docs/
└─ secure_coding_checklist.csv
```

## GitHub 업로드 시 제외할 파일

다음 항목은 `.gitignore`에 포함되어 있습니다.

- `.env`
- `.venv/`
- `market.db`
- `__pycache__/`
- 실제 업로드 이미지

## 운영 환경 유의사항

- 로컬 개발 환경에서는 HTTP와 일반 WebSocket을 사용합니다. 실제 배포 시 HTTPS와 WSS를 적용해야 합니다.
- `COOKIE_SECURE=1`은 HTTPS 환경에서만 설정해야 합니다.
- 현재 채팅 Rate Limiting은 단일 Flask 프로세스의 메모리를 사용합니다. 다중 서버 환경에서는 Redis 같은 중앙 저장소가 필요합니다.
- SQLite는 별도 DB 사용자 권한을 지원하지 않으므로 운영 환경에서는 DB 파일 접근 권한을 최소화해야 합니다.

## 문서

- [`GUIDELINE_AUDIT.md`](GUIDELINE_AUDIT.md): 과제 가이드라인 점검 결과
- [`secure_coding_checklist.csv`](secure_coding_checklist.csv): 제공된 보안 체크리스트
- [`docs/FINAL_MANUAL_TEST_CHECKLIST.md`](docs/FINAL_MANUAL_TEST_CHECKLIST.md): 최종 수동 테스트 목록
