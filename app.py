import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import re
import time
from collections import defaultdict, deque
from threading import Lock

from PIL import Image, ImageOps, UnidentifiedImageError

from dotenv import load_dotenv
from flask import (
    Flask,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
    abort,
)
from flask_socketio import SocketIO, emit, join_room
from werkzeug.security import check_password_hash, generate_password_hash

from flask_wtf.csrf import CSRFProtect, CSRFError



load_dotenv()

app = Flask(__name__)
csrf = CSRFProtect()

secret_key = os.environ.get("SECRET_KEY")
if not secret_key:
    raise RuntimeError("SECRET_KEY 환경변수가 설정되지 않았습니다.")

app.config["SECRET_KEY"] = secret_key
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
# 로컬 HTTP 개발 환경에서는 False, HTTPS 배포 시 COOKIE_SECURE=1로 활성화
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("COOKIE_SECURE", "0") == "1"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=2)
app.config["WTF_CSRF_TIME_LIMIT"] = 2 * 60 * 60
app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024

csrf.init_app(app)
socketio = SocketIO(app, async_mode="threading")

BASE_DIR = Path(__file__).resolve().parent
DATABASE = str(BASE_DIR / "market.db")
PRODUCT_UPLOAD_DIR = BASE_DIR / "static" / "uploads" / "products"
PRODUCT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def product_image_exists(filename):
    """DB에 파일명이 남아 있어도 실제 업로드 파일이 없으면 깨진 이미지를 표시하지 않는다."""
    if not filename or Path(filename).name != filename:
        return False
    return (PRODUCT_UPLOAD_DIR / filename).is_file()


@app.template_global()
def product_image_url(filename):
    if not product_image_exists(filename):
        return None
    return url_for("static", filename=f"uploads/products/{filename}")


MAX_PRODUCT_IMAGES = 5
MAX_IMAGE_BYTES = 5 * 1024 * 1024
ALLOWED_IMAGE_FORMATS = {"JPEG", "PNG", "WEBP"}

PRODUCT_CATEGORIES = (
    "디지털기기",
    "생활가전",
    "가구·인테리어",
    "의류",
    "잡화",
    "도서",
    "스포츠·레저",
    "취미·게임",
    "뷰티",
    "기타",
)
PRODUCT_CONDITIONS = (
    "새 상품",
    "거의 새 상품",
    "사용감 적음",
    "사용감 있음",
    "하자 있음",
)
TRADE_METHODS = ("직거래", "택배", "모두 가능")
PRODUCT_STATUSES = ("판매중", "예약중", "판매완료")

# 실시간 채팅 메시지의 발신자를 임시로 저장
# 서버가 재시작되면 초기화되는 실시간 상태임
unread_chat_messages = {}

# 실시간 메시지 도배 방지용 메모리 버킷
# 단일 프로세스 로컬 실행 환경을 기준으로 하며, 운영 환경에서는 Redis 등
# 중앙 저장소를 사용해야 여러 서버 인스턴스에서도 동일하게 적용된다.
_realtime_rate_buckets = defaultdict(deque)
_realtime_rate_lock = Lock()

LOGIN_FAILURE_LIMIT = 5
LOGIN_LOCK_MINUTES = 5
GLOBAL_CHAT_WINDOW_SECONDS = 3
PRIVATE_CHAT_WINDOW_SECONDS = 1
REPORT_HOURLY_LIMIT = 5


def _client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()[:64]
    return (request.remote_addr or "unknown")[:64]


def _login_attempt_key(username):
    normalized = username.strip().lower()[:30]
    return f"{_client_ip()}|{normalized}"


def _parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def get_login_lock_remaining(cursor, attempt_key):
    cursor.execute(
        """
        SELECT failure_count, locked_until
        FROM login_attempt
        WHERE attempt_key = ?
        """,
        (attempt_key,),
    )
    attempt = cursor.fetchone()

    if attempt is None:
        return 0

    locked_until = _parse_datetime(attempt["locked_until"])
    now = datetime.now().astimezone()

    if locked_until is None or locked_until <= now:
        if locked_until is not None:
            cursor.execute(
                "DELETE FROM login_attempt WHERE attempt_key = ?",
                (attempt_key,),
            )
        return 0

    return max(1, int((locked_until - now).total_seconds()))


def record_login_failure(cursor, attempt_key):
    now = datetime.now().astimezone()
    cursor.execute(
        """
        SELECT failure_count
        FROM login_attempt
        WHERE attempt_key = ?
        """,
        (attempt_key,),
    )
    row = cursor.fetchone()
    failure_count = (row["failure_count"] if row else 0) + 1
    locked_until = None

    if failure_count >= LOGIN_FAILURE_LIMIT:
        locked_until = (now + timedelta(minutes=LOGIN_LOCK_MINUTES)).isoformat()

    cursor.execute(
        """
        INSERT INTO login_attempt (
            attempt_key, failure_count, locked_until, last_failed_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(attempt_key) DO UPDATE SET
            failure_count = excluded.failure_count,
            locked_until = excluded.locked_until,
            last_failed_at = excluded.last_failed_at
        """,
        (attempt_key, failure_count, locked_until, now.isoformat()),
    )

    return locked_until is not None


def clear_login_failures(cursor, attempt_key):
    cursor.execute(
        "DELETE FROM login_attempt WHERE attempt_key = ?",
        (attempt_key,),
    )


def realtime_action_allowed(bucket_key, limit, window_seconds):
    now = time.monotonic()
    with _realtime_rate_lock:
        bucket = _realtime_rate_buckets[bucket_key]
        while bucket and now - bucket[0] >= window_seconds:
            bucket.popleft()

        if len(bucket) >= limit:
            return False

        bucket.append(now)
        return True

# 데이터베이스 연결
def get_db():
    db = getattr(g, "_database", None)

    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")

    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)

    if db is not None:
        db.close()


@app.after_request
def add_security_headers(response):
    """브라우저가 콘텐츠를 더 안전하게 처리하도록 기본 보안 헤더를 추가한다."""
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=()",
    )
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "img-src 'self' data: blob:; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; "
        "connect-src 'self' ws: wss:; "
        "font-src 'self' data:; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'",
    )
    return response


@app.errorhandler(CSRFError)
def handle_csrf_error(error):
    return (
        render_template(
            "error.html",
            status_code=400,
            title="요청을 확인할 수 없습니다",
            message=(
                "보안 토큰이 없거나 만료되었습니다. "
                "페이지를 새로고침한 뒤 다시 시도해주세요."
            ),
        ),
        400,
    )


@app.errorhandler(403)
def forbidden_error(error):
    return (
        render_template(
            "error.html",
            status_code=403,
            title="접근 권한이 없습니다",
            message="이 페이지 또는 기능을 사용할 권한이 없습니다.",
        ),
        403,
    )


@app.errorhandler(404)
def not_found_error(error):
    return (
        render_template(
            "error.html",
            status_code=404,
            title="페이지를 찾을 수 없습니다",
            message="주소가 잘못되었거나 삭제된 페이지입니다.",
        ),
        404,
    )


@app.errorhandler(405)
def method_not_allowed_error(error):
    return (
        render_template(
            "error.html",
            status_code=405,
            title="허용되지 않은 요청입니다",
            message="올바른 화면의 버튼을 통해 다시 시도해주세요.",
        ),
        405,
    )


@app.errorhandler(413)
def upload_too_large_error(error):
    return (
        render_template(
            "error.html",
            status_code=413,
            title="업로드 용량을 초과했습니다",
            message="전체 업로드 용량은 30MB를 넘을 수 없습니다.",
        ),
        413,
    )


@app.errorhandler(500)
def internal_server_error(error):
    db = getattr(g, "_database", None)
    if db is not None:
        db.rollback()

    return (
        render_template(
            "error.html",
            status_code=500,
            title="서버에서 오류가 발생했습니다",
            message="잠시 후 다시 시도해주세요.",
        ),
        500,
    )



# 상품 입력값 검증
def parse_product_form(form):
    title = form.get("title", "").strip()
    description = form.get("description", "").strip()
    price_text = form.get("price", "").strip()
    category = form.get("category", "기타").strip()
    condition = form.get("condition", "사용감 있음").strip()
    trade_method = form.get("trade_method", "직거래").strip()
    location = form.get("location", "").strip()
    status = form.get("status", "판매중").strip()
    negotiable = 1 if form.get("negotiable") == "1" else 0

    if not title or len(title) > 100:
        raise ValueError("상품명은 1자 이상 100자 이하로 입력해주세요.")

    if not description or len(description) > 2000:
        raise ValueError("상품 설명은 1자 이상 2000자 이하로 입력해주세요.")

    try:
        price = int(price_text)
    except ValueError as exc:
        raise ValueError("가격은 숫자로 입력해주세요.") from exc

    if price <= 0 or price > 100_000_000:
        raise ValueError("가격은 1원 이상 1억 원 이하로 입력해주세요.")

    if category not in PRODUCT_CATEGORIES:
        raise ValueError("올바른 카테고리를 선택해주세요.")

    if condition not in PRODUCT_CONDITIONS:
        raise ValueError("올바른 상품 상태를 선택해주세요.")

    if trade_method not in TRADE_METHODS:
        raise ValueError("올바른 거래 방법을 선택해주세요.")

    if status not in PRODUCT_STATUSES:
        raise ValueError("올바른 판매 상태를 선택해주세요.")

    if len(location) > 100:
        raise ValueError("거래 희망 장소는 100자 이하로 입력해주세요.")

    return {
        "title": title,
        "description": description,
        "price": price,
        "category": category,
        "condition": condition,
        "trade_method": trade_method,
        "location": location,
        "status": status,
        "negotiable": negotiable,
    }


def save_product_images(files, product_id, start_order=0):
    uploads = [file for file in files if file and file.filename]

    if not uploads:
        return []

    if len(uploads) + start_order > MAX_PRODUCT_IMAGES:
        raise ValueError(f"상품 사진은 최대 {MAX_PRODUCT_IMAGES}장까지 등록할 수 있습니다.")

    saved_filenames = []

    try:
        for index, upload in enumerate(uploads, start=start_order):
            upload.stream.seek(0, os.SEEK_END)
            size = upload.stream.tell()
            upload.stream.seek(0)

            if size <= 0 or size > MAX_IMAGE_BYTES:
                raise ValueError("각 상품 사진은 5MB 이하만 업로드할 수 있습니다.")

            try:
                with Image.open(upload.stream) as checking_image:
                    image_format = (checking_image.format or "").upper()
                    checking_image.verify()
            except (UnidentifiedImageError, OSError) as exc:
                raise ValueError("JPEG, PNG, WebP 형식의 정상적인 이미지만 업로드할 수 있습니다.") from exc

            if image_format not in ALLOWED_IMAGE_FORMATS:
                raise ValueError("JPEG, PNG, WebP 형식의 이미지만 업로드할 수 있습니다.")

            upload.stream.seek(0)
            with Image.open(upload.stream) as source_image:
                source_image = ImageOps.exif_transpose(source_image)
                source_image.thumbnail((1600, 1600))

                if source_image.mode in ("RGBA", "LA"):
                    clean_image = Image.new("RGB", source_image.size, "white")
                    alpha = source_image.getchannel("A")
                    clean_image.paste(source_image.convert("RGB"), mask=alpha)
                else:
                    clean_image = source_image.convert("RGB")

                filename = f"{product_id}_{uuid.uuid4().hex}.webp"
                destination = PRODUCT_UPLOAD_DIR / filename
                clean_image.save(
                    destination,
                    format="WEBP",
                    quality=86,
                    method=6,
                )

            saved_filenames.append((filename, index))

    except Exception:
        for filename, _ in saved_filenames:
            (PRODUCT_UPLOAD_DIR / filename).unlink(missing_ok=True)
        raise

    return saved_filenames


def remove_product_image_file(filename):
    if not filename:
        return

    safe_name = Path(filename).name
    (PRODUCT_UPLOAD_DIR / safe_name).unlink(missing_ok=True)


def normalize_user_pair(first_user_id, second_user_id):
    """친구 관계와 1:1 채팅방에서 동일한 사용자 쌍을 일정한 순서로 저장한다."""
    if first_user_id == second_user_id:
        raise ValueError("같은 사용자를 사용자 쌍으로 만들 수 없습니다.")

    return tuple(sorted((first_user_id, second_user_id)))


def get_friendship(cursor, first_user_id, second_user_id):
    user1_id, user2_id = normalize_user_pair(first_user_id, second_user_id)
    cursor.execute(
        """
        SELECT id, user1_id, user2_id, created_at
        FROM friendship
        WHERE user1_id = ? AND user2_id = ?
        """,
        (user1_id, user2_id),
    )
    return cursor.fetchone()


def get_private_room_for_pair(cursor, first_user_id, second_user_id):
    user1_id, user2_id = normalize_user_pair(first_user_id, second_user_id)
    cursor.execute(
        """
        SELECT id, user1_id, user2_id, product_id, created_at
        FROM private_chat_room
        WHERE user1_id = ? AND user2_id = ?
        """,
        (user1_id, user2_id),
    )
    return cursor.fetchone()


def user_can_access_private_room(cursor, room_id, user_id):
    cursor.execute(
        """
        SELECT id, user1_id, user2_id, product_id, created_at
        FROM private_chat_room
        WHERE id = ?
          AND (user1_id = ? OR user2_id = ?)
        """,
        (room_id, user_id, user_id),
    )
    return cursor.fetchone()

def users_have_block_relation(cursor, user_a_id, user_b_id):
    cursor.execute(
        """
        SELECT 1
        FROM user_block
        WHERE (
            blocker_id = ?
            AND blocked_id = ?
        )
        OR (
            blocker_id = ?
            AND blocked_id = ?
        )
        LIMIT 1
        """,
        (
            user_a_id,
            user_b_id,
            user_b_id,
            user_a_id,
        ),
    )

    return cursor.fetchone() is not None


def record_admin_action(
    cursor,
    admin_id,
    admin_username,
    action_type,
    target_type,
    target_id,
    target_label,
    details=None,
):
    """관리자 조치를 감사 로그에 남긴다.

    실제 상태 변경과 같은 데이터베이스 트랜잭션에서 호출해야
    조치만 반영되고 로그는 누락되는 상황을 막을 수 있다.
    """
    cursor.execute(
        """
        INSERT INTO admin_action_log (
            id,
            admin_id,
            admin_username,
            action_type,
            target_type,
            target_id,
            target_label,
            details
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            admin_id,
            admin_username,
            action_type,
            target_type,
            target_id,
            target_label,
            details,
        ),
    )

# 테이블 생성 및 기존 DB 마이그레이션
def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                bio TEXT
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS product (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                price INTEGER NOT NULL,
                seller_id TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '기타',
                condition TEXT NOT NULL DEFAULT '사용감 있음',
                trade_method TEXT NOT NULL DEFAULT '직거래',
                location TEXT,
                negotiable INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT '판매중',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                view_count INTEGER NOT NULL DEFAULT 0,
                report_count INTEGER NOT NULL DEFAULT 0,
                is_hidden INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (seller_id) REFERENCES user(id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS report (
                id TEXT PRIMARY KEY,
                reporter_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                target_type TEXT NOT NULL DEFAULT 'product',
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                admin_note TEXT,
                reviewed_by TEXT,
                reviewed_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (reporter_id, target_id),
                FOREIGN KEY (reporter_id) REFERENCES user(id),
                FOREIGN KEY (reviewed_by) REFERENCES user(id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS login_attempt (
                attempt_key TEXT PRIMARY KEY,
                failure_count INTEGER NOT NULL DEFAULT 0,
                locked_until TEXT,
                last_failed_at TEXT NOT NULL
            )
            """
        )

        cursor.execute("PRAGMA table_info(report)")
        report_columns = {column["name"] for column in cursor.fetchall()}
        report_migrations = {
            "target_type": "TEXT",
            "status": "TEXT NOT NULL DEFAULT 'pending'",
            "admin_note": "TEXT",
            "reviewed_by": "TEXT",
            "reviewed_at": "TEXT",
            "created_at": "TEXT",
        }
        for column_name, definition in report_migrations.items():
            if column_name not in report_columns:
                cursor.execute(
                    f"ALTER TABLE report ADD COLUMN {column_name} {definition}"
                )

        cursor.execute(
            """
            UPDATE report
            SET created_at = CURRENT_TIMESTAMP
            WHERE created_at IS NULL OR created_at = ''
            """
        )
        cursor.execute(
            """
            UPDATE report
            SET target_type = CASE
                WHEN EXISTS (
                    SELECT 1 FROM product WHERE product.id = report.target_id
                ) THEN 'product'
                ELSE 'user'
            END
            WHERE target_type IS NULL
               OR target_type NOT IN ('product', 'user')
            """
        )
        cursor.execute(
            """
            UPDATE report
            SET status = 'pending'
            WHERE status IS NULL
               OR status NOT IN ('pending', 'approved', 'rejected')
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_report_status_created
            ON report(status, created_at DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_report_target
            ON report(target_type, target_id)
            """
        )

        cursor.execute("PRAGMA table_info(user)")
        user_columns = {column["name"] for column in cursor.fetchall()}

        user_migrations = {
            "report_count": "INTEGER NOT NULL DEFAULT 0",
            "is_suspended": "INTEGER NOT NULL DEFAULT 0",
            "is_admin": "INTEGER NOT NULL DEFAULT 0",
            "balance": "INTEGER NOT NULL DEFAULT 10000",
            "nickname": "TEXT",
            "payment_password": "TEXT",
            "created_at": "TEXT",
        }
        for column_name, definition in user_migrations.items():
            if column_name not in user_columns:
                cursor.execute(
                    f"ALTER TABLE user ADD COLUMN {column_name} {definition}"
                )
            # 기존 데이터베이스에 nickname 컬럼이 없는 경우 추가
        cursor.execute(
            "PRAGMA table_info(user)"
        )

        user_columns = {
            row[1]
            for row in cursor.fetchall()
        }

        if "nickname" not in user_columns:
            cursor.execute(
                """
                ALTER TABLE user
                ADD COLUMN nickname TEXT
                """
            )

        cursor.execute(
            """
            UPDATE user
            SET nickname = username
            WHERE nickname IS NULL OR TRIM(nickname) = ''
            """
        )
        cursor.execute(
            """
            UPDATE user
            SET created_at = CURRENT_TIMESTAMP
            WHERE created_at IS NULL OR created_at = ''
            """
        )

        cursor.execute("PRAGMA table_info(product)")
        product_columns = {column["name"] for column in cursor.fetchall()}

        product_migrations = {
            "category": "TEXT NOT NULL DEFAULT '기타'",
            "condition": "TEXT NOT NULL DEFAULT '사용감 있음'",
            "trade_method": "TEXT NOT NULL DEFAULT '직거래'",
            "location": "TEXT",
            "negotiable": "INTEGER NOT NULL DEFAULT 0",
            "status": "TEXT NOT NULL DEFAULT '판매중'",
            "created_at": "TEXT",
            "view_count": "INTEGER NOT NULL DEFAULT 0",
            "report_count": "INTEGER NOT NULL DEFAULT 0",
            "is_hidden": "INTEGER NOT NULL DEFAULT 0",
        }
        for column_name, definition in product_migrations.items():
            if column_name not in product_columns:
                cursor.execute(
                    f"ALTER TABLE product ADD COLUMN {column_name} {definition}"
                )

        cursor.execute(
            """
            UPDATE product
            SET created_at = CURRENT_TIMESTAMP
            WHERE created_at IS NULL OR created_at = ''
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transfer (
                id TEXT PRIMARY KEY,
                sender_id TEXT NOT NULL,
                receiver_id TEXT NOT NULL,
                amount INTEGER NOT NULL CHECK (amount > 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sender_id) REFERENCES user(id),
                FOREIGN KEY (receiver_id) REFERENCES user(id)
            )
            """
        )

        # 상품 상세에서 상품 금액 그대로 송금할 수 있도록 연결 정보 추가
        cursor.execute("PRAGMA table_info(transfer)")
        transfer_columns = {
            column["name"]
            for column in cursor.fetchall()
        }

        if "product_id" not in transfer_columns:
            cursor.execute(
                """
                ALTER TABLE transfer
                ADD COLUMN product_id TEXT
                """
            )

        # 하나의 상품이 중복 결제되는 것을 DB 수준에서도 방지
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transfer_product_unique
            ON transfer(product_id)
            WHERE product_id IS NOT NULL
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_action_log (
                id TEXT PRIMARY KEY,
                admin_id TEXT NOT NULL,
                admin_username TEXT NOT NULL,
                action_type TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_id TEXT,
                target_label TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (admin_id) REFERENCES user(id)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_admin_action_log_created_at
            ON admin_action_log(created_at DESC)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transfer_created_at
            ON transfer(created_at DESC)
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS product_image (
                id TEXT PRIMARY KEY,
                product_id TEXT NOT NULL,
                filename TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS favorite (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, product_id),
                FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE CASCADE
            )
            """
        )
                # 사용자별 사용자 차단
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_block (
                id TEXT PRIMARY KEY,
                blocker_id TEXT NOT NULL,
                blocked_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                UNIQUE (blocker_id, blocked_id),
                CHECK (blocker_id != blocked_id),

                FOREIGN KEY (blocker_id)
                    REFERENCES user(id)
                    ON DELETE CASCADE,

                FOREIGN KEY (blocked_id)
                    REFERENCES user(id)
                    ON DELETE CASCADE
            )
            """
        )

        # 사용자별 상품 숨김
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS hidden_product (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                UNIQUE (user_id, product_id),

                FOREIGN KEY (user_id)
                    REFERENCES user(id)
                    ON DELETE CASCADE,

                FOREIGN KEY (product_id)
                    REFERENCES product(id)
                    ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS friend_request (
                id TEXT PRIMARY KEY,
                sender_id TEXT NOT NULL,
                receiver_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'accepted', 'rejected')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                responded_at TEXT,
                UNIQUE (sender_id, receiver_id),
                CHECK (sender_id != receiver_id),
                FOREIGN KEY (sender_id) REFERENCES user(id) ON DELETE CASCADE,
                FOREIGN KEY (receiver_id) REFERENCES user(id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS friendship (
                id TEXT PRIMARY KEY,
                user1_id TEXT NOT NULL,
                user2_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user1_id, user2_id),
                CHECK (user1_id < user2_id),
                FOREIGN KEY (user1_id) REFERENCES user(id) ON DELETE CASCADE,
                FOREIGN KEY (user2_id) REFERENCES user(id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS private_chat_room (
                id TEXT PRIMARY KEY,
                user1_id TEXT NOT NULL,
                user2_id TEXT NOT NULL,
                product_id TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user1_id, user2_id),
                CHECK (user1_id < user2_id),
                FOREIGN KEY (user1_id) REFERENCES user(id) ON DELETE CASCADE,
                FOREIGN KEY (user2_id) REFERENCES user(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE SET NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS private_chat_message (
                id TEXT PRIMARY KEY,
                room_id TEXT NOT NULL,
                sender_id TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                read_at TEXT,
                FOREIGN KEY (room_id) REFERENCES private_chat_room(id) ON DELETE CASCADE,
                FOREIGN KEY (sender_id) REFERENCES user(id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_product_seller ON product(seller_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_product_status ON product(status, is_hidden)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_product_image_product ON product_image(product_id, sort_order)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_favorite_product ON favorite(product_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_friend_request_receiver ON friend_request(receiver_id, status)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_friendship_user1 ON friendship(user1_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_friendship_user2 ON friendship(user2_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_private_message_room ON private_chat_message(room_id, created_at)"
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_block_blocker
            ON user_block(blocker_id, blocked_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_hidden_product_user
            ON hidden_product(user_id, product_id)
            """
        )

        db.commit()

@app.before_request
def check_suspended_user():
    user_id = session.get("user_id")

    # 로그인하지 않은 사용자는 검사하지 않음
    if not user_id:
        return None

    # CSS 등 정적 파일 요청은 검사하지 않음
    if request.endpoint == "static":
        return None

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, is_suspended
        FROM user
        WHERE id = ?
        """,
        (user_id,),
    )
    user = cursor.fetchone()

    # DB에서 사용자가 삭제된 경우 기존 세션 제거
    if user is None:
        session.clear()
        flash("사용자 정보를 찾을 수 없어 로그아웃되었습니다.")
        return redirect(url_for("login"))

    # 로그인 후 계정이 정지된 경우 강제 로그아웃
    if user["is_suspended"] == 1:
        session.clear()
        flash("신고 누적으로 정지된 계정입니다.")
        return redirect(url_for("login"))

    return None

# 기본 페이지
@app.route("/")
def index():
    if "user_id" in session:
        return redirect(url_for("dashboard"))

    return render_template("index.html")


# 회원가입
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if len(username) < 3 or len(username) > 30:
            flash("사용자명은 3자 이상 30자 이하로 입력해주세요.")
            return redirect(url_for("register"))

        if len(password) < 8 or len(password) > 128:
            flash("비밀번호는 8자 이상 128자 이하로 입력해주세요.")
            return redirect(url_for("register"))

        db = get_db()
        cursor = db.cursor()

        cursor.execute(
            "SELECT id FROM user WHERE username = ?",
            (username,),
        )

        if cursor.fetchone() is not None:
            flash("이미 존재하는 사용자명입니다.")
            return redirect(url_for("register"))

        user_id = str(uuid.uuid4())
        password_hash = generate_password_hash(password)

        try:
            cursor.execute(
                """
                INSERT INTO user (id, username, password)
                VALUES (?, ?, ?)
                """,
                (user_id, username, password_hash),
            )
            db.commit()
        except sqlite3.IntegrityError:
            db.rollback()
            flash("회원가입 처리 중 오류가 발생했습니다.")
            return redirect(url_for("register"))

        flash("회원가입이 완료되었습니다. 로그인해주세요.")
        return redirect(url_for("login"))

    return render_template("register.html")


# 로그인
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        db = get_db()
        cursor = db.cursor()
        attempt_key = _login_attempt_key(username)
        remaining_seconds = get_login_lock_remaining(cursor, attempt_key)

        if remaining_seconds > 0:
            db.commit()
            remaining_minutes = max(1, (remaining_seconds + 59) // 60)
            flash(
                f"로그인 시도가 너무 많습니다. "
                f"약 {remaining_minutes}분 후 다시 시도해주세요."
            )
            return redirect(url_for("login"))

        cursor.execute(
            "SELECT * FROM user WHERE username = ?",
            (username,),
        )
        user = cursor.fetchone()

        # 아이디 또는 비밀번호가 틀린 경우
        if (
            user is None
            or not check_password_hash(
                user["password"],
                password,
            )
        ):
            locked = record_login_failure(cursor, attempt_key)
            db.commit()
            if locked:
                flash(
                    "로그인에 5회 실패하여 5분 동안 로그인이 제한됩니다."
                )
            else:
                flash("아이디 또는 비밀번호가 올바르지 않습니다.")
            return redirect(url_for("login"))

        clear_login_failures(cursor, attempt_key)
        db.commit()

        # 비밀번호는 맞지만 운영 정책상 정지된 계정인 경우
        if user["is_suspended"] == 1:
            session.clear()
            flash("이용이 정지된 계정입니다.")
            return redirect(url_for("login"))

        # 정상 사용자 로그인 처리
        session.clear()
        session.permanent = True
        session["user_id"] = user["id"]

        flash("로그인 성공!")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


# 로그아웃: 상태를 변경하는 요청이므로 POST + CSRF로 처리
@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    flash("로그아웃되었습니다.")

    return redirect(url_for("index"))




# 메인 상품 피드
@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT * FROM user WHERE id = ?",
        (session["user_id"],),
    )
    current_user = cursor.fetchone()

    if current_user is None:
        session.clear()
        return redirect(url_for("login"))

    query = request.args.get("q", "").strip()[:50]
    category = request.args.get("category", "").strip()
    status = request.args.get("status", "판매중").strip()
    sort = request.args.get("sort", "latest").strip()

    current_user_id = session["user_id"]

    where_clauses = [
        "product.is_hidden = 0",

        """
        NOT EXISTS (
            SELECT 1
            FROM user_block
            WHERE (
                blocker_id = ?
                AND blocked_id = product.seller_id
            )
            OR (
                blocker_id = product.seller_id
                AND blocked_id = ?
            )
        )
        """,

        """
        NOT EXISTS (
            SELECT 1
            FROM hidden_product
            WHERE hidden_product.user_id = ?
              AND hidden_product.product_id = product.id
        )
        """,
    ]

    params = [
        current_user_id,
        current_user_id,
        current_user_id,
    ]

    if query:
        escaped_query = (
            query.replace("!", "!!")
            .replace("%", "!%")
            .replace("_", "!_")
        )
        search_pattern = f"%{escaped_query}%"
        where_clauses.append(
            """
            (
                product.title LIKE ? ESCAPE '!'
                OR product.description LIKE ? ESCAPE '!'
                OR product.category LIKE ? ESCAPE '!'
                OR COALESCE(product.location, '') LIKE ? ESCAPE '!'
            )
            """
        )
        params.extend([search_pattern] * 4)

    if category in PRODUCT_CATEGORIES:
        where_clauses.append("product.category = ?")
        params.append(category)
    else:
        category = ""

    if status in PRODUCT_STATUSES:
        where_clauses.append("product.status = ?")
        params.append(status)
    elif status == "전체":
        pass
    else:
        status = "판매중"
        where_clauses.append("product.status = ?")
        params.append(status)

    order_by = {
        "latest": "product.created_at DESC, product.rowid DESC",
        "price_low": "product.price ASC, product.created_at DESC",
        "price_high": "product.price DESC, product.created_at DESC",
        "popular": "favorite_count DESC, product.view_count DESC",
        "views": "product.view_count DESC, product.created_at DESC",
    }.get(sort, "product.created_at DESC, product.rowid DESC")

    sql = f"""
        SELECT
            product.*,
            user.username AS seller_username,
            (
                SELECT filename
                FROM product_image
                WHERE product_image.product_id = product.id
                ORDER BY sort_order ASC, rowid ASC
                LIMIT 1
            ) AS image_filename,
            (
                SELECT COUNT(*)
                FROM favorite
                WHERE favorite.product_id = product.id
            ) AS favorite_count
        FROM product
        LEFT JOIN user ON user.id = product.seller_id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY {order_by}
    """
    cursor.execute(sql, params)
    products = cursor.fetchall()

    return render_template(
        "dashboard.html",
        products=products,
        user=current_user,
        query=query,
        categories=PRODUCT_CATEGORIES,
        selected_category=category,
        selected_status=status,
        selected_sort=sort,
    )
# 프로필 관리
@app.route("/profile", methods=["GET", "POST"])
def profile():
    if "user_id" not in session:
        return redirect(url_for("login"))

    current_user_id = session["user_id"]

    db = get_db()
    cursor = db.cursor()

    if request.method == "POST":
        nickname = request.form.get(
            "nickname",
            "",
        ).strip()

        bio = request.form.get(
            "bio",
            "",
        ).strip()

        if len(nickname) < 2:
            flash(
                "닉네임은 2자 이상 입력해주세요."
            )
            return redirect(url_for("profile"))

        if len(nickname) > 20:
            flash(
                "닉네임은 20자 이하로 입력해주세요."
            )
            return redirect(url_for("profile"))

        # 한글, 영문, 숫자, 밑줄만 허용
        if not re.fullmatch(
            r"[가-힣A-Za-z0-9_]+",
            nickname,
        ):
            flash(
                "닉네임에는 한글, 영문, 숫자, 밑줄만 사용할 수 있습니다."
            )
            return redirect(url_for("profile"))

        if len(bio) > 500:
            flash(
                "소개글은 500자 이하로 입력해주세요."
            )
            return redirect(url_for("profile"))

        # 다른 사용자가 같은 닉네임을 사용 중인지 검사
        cursor.execute(
            """
            SELECT id
            FROM user
            WHERE LOWER(TRIM(nickname))
                  = LOWER(TRIM(?))
              AND id != ?
            LIMIT 1
            """,
            (
                nickname,
                current_user_id,
            ),
        )

        duplicate_user = cursor.fetchone()

        if duplicate_user is not None:
            flash(
                "이미 사용 중인 닉네임입니다."
            )
            return redirect(url_for("profile"))

        try:
            cursor.execute(
                """
                UPDATE user
                SET
                    nickname = ?,
                    bio = ?
                WHERE id = ?
                """,
                (
                    nickname,
                    bio,
                    current_user_id,
                ),
            )

            db.commit()

        except sqlite3.Error:
            db.rollback()

            flash(
                "프로필 변경 중 오류가 발생했습니다."
            )
            return redirect(url_for("profile"))

        flash(
            "프로필이 업데이트되었습니다."
        )

        return redirect(url_for("profile"))

    cursor.execute(
        """
        SELECT
            id,
            username,
            nickname,
            bio,
            payment_password
        FROM user
        WHERE id = ?
        """,
        (current_user_id,),
    )

    user = cursor.fetchone()

    if user is None:
        session.clear()

        flash(
            "사용자 정보를 확인할 수 없습니다."
        )

        return redirect(url_for("login"))

    return render_template(
        "profile.html",
        user=user,
        has_payment_password=bool(user["payment_password"]),
    )

# 결제 비밀번호 설정·변경
@app.route("/profile/payment-password", methods=["GET", "POST"])
def payment_password():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT id, username, payment_password
        FROM user
        WHERE id = ?
          AND is_suspended = 0
        """,
        (session["user_id"],),
    )
    user = cursor.fetchone()

    if user is None:
        session.clear()
        flash("사용자 정보를 확인할 수 없습니다.")
        return redirect(url_for("login"))

    has_payment_password = bool(user["payment_password"])

    if request.method == "POST":
        current_pin = request.form.get("current_pin", "").strip()
        new_pin = request.form.get("new_pin", "").strip()
        confirm_pin = request.form.get("confirm_pin", "").strip()

        if has_payment_password and not check_password_hash(
            user["payment_password"],
            current_pin,
        ):
            flash("현재 결제 비밀번호가 올바르지 않습니다.")
            return redirect(url_for("payment_password"))

        if not re.fullmatch(r"\d{6}", new_pin):
            flash("결제 비밀번호는 숫자 6자리로 입력해주세요.")
            return redirect(url_for("payment_password"))

        weak_pins = {
            "000000", "111111", "222222", "333333", "444444",
            "555555", "666666", "777777", "888888", "999999",
            "123456", "654321", "012345", "543210",
        }
        if new_pin in weak_pins:
            flash("연속되거나 반복되는 쉬운 번호는 사용할 수 없습니다.")
            return redirect(url_for("payment_password"))

        if new_pin != confirm_pin:
            flash("새 결제 비밀번호 확인이 일치하지 않습니다.")
            return redirect(url_for("payment_password"))

        if has_payment_password and check_password_hash(
            user["payment_password"],
            new_pin,
        ):
            flash("현재 결제 비밀번호와 다른 번호를 사용해주세요.")
            return redirect(url_for("payment_password"))

        try:
            cursor.execute(
                """
                UPDATE user
                SET payment_password = ?
                WHERE id = ?
                """,
                (
                    generate_password_hash(new_pin),
                    session["user_id"],
                ),
            )
            db.commit()
        except sqlite3.Error:
            db.rollback()
            flash("결제 비밀번호 저장 중 오류가 발생했습니다.")
            return redirect(url_for("payment_password"))

        session.pop("payment_pin_failures", None)
        session.pop("payment_pin_locked_until", None)
        flash(
            "결제 비밀번호가 변경되었습니다."
            if has_payment_password
            else "결제 비밀번호가 설정되었습니다."
        )
        return redirect(url_for("transfer"))

    return render_template(
        "payment_password.html",
        user=user,
        has_payment_password=has_payment_password,
    )

# 비밀번호 변경
@app.route("/profile/password", methods=["GET", "POST"])
def change_password():
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, password
        FROM user
        WHERE id = ?
        """,
        (session["user_id"],),
    )

    user = cursor.fetchone()

    if user is None:
        session.clear()
        flash("사용자 정보를 확인할 수 없습니다.")
        return redirect(url_for("login"))

    if request.method == "POST":
        current_password = request.form.get(
            "current_password",
            "",
        )

        new_password = request.form.get(
            "new_password",
            "",
        )

        confirm_password = request.form.get(
            "confirm_password",
            "",
        )

        if not current_password:
            flash("현재 비밀번호를 입력해주세요.")
            return redirect(
                url_for("change_password")
            )

        if not check_password_hash(
            user["password"],
            current_password,
        ):
            flash("현재 비밀번호가 올바르지 않습니다.")
            return redirect(
                url_for("change_password")
            )

        if len(new_password) < 8:
            flash(
                "새 비밀번호는 8자 이상이어야 합니다."
            )
            return redirect(
                url_for("change_password")
            )

        if len(new_password) > 128:
            flash(
                "새 비밀번호는 128자 이하로 입력해주세요."
            )
            return redirect(
                url_for("change_password")
            )

        if any(
            character.isspace()
            for character in new_password
        ):
            flash(
                "새 비밀번호에는 공백을 사용할 수 없습니다."
            )
            return redirect(
                url_for("change_password")
            )

        if not any(
            character.isalpha()
            for character in new_password
        ):
            flash(
                "새 비밀번호에는 영문자를 포함해야 합니다."
            )
            return redirect(
                url_for("change_password")
            )

        if not any(
            character.isdigit()
            for character in new_password
        ):
            flash(
                "새 비밀번호에는 숫자를 포함해야 합니다."
            )
            return redirect(
                url_for("change_password")
            )

        if new_password != confirm_password:
            flash(
                "새 비밀번호 확인이 일치하지 않습니다."
            )
            return redirect(
                url_for("change_password")
            )

        if check_password_hash(
            user["password"],
            new_password,
        ):
            flash(
                "현재 비밀번호와 다른 비밀번호를 사용해주세요."
            )
            return redirect(
                url_for("change_password")
            )

        new_password_hash = generate_password_hash(
            new_password
        )

        try:
            cursor.execute(
                """
                UPDATE user
                SET password = ?
                WHERE id = ?
                """,
                (
                    new_password_hash,
                    session["user_id"],
                ),
            )

            db.commit()

        except sqlite3.Error:
            db.rollback()

            flash(
                "비밀번호 변경 중 오류가 발생했습니다."
            )
            return redirect(
                url_for("change_password")
            )

        # 비밀번호 변경 후 기존 로그인 세션 종료
        session.clear()

        flash(
            "비밀번호가 변경되었습니다. 새 비밀번호로 다시 로그인해주세요."
        )

        return redirect(url_for("login"))

    return render_template(
        "change_password.html",
        user=user,
    )

# 상품 등록
@app.route("/product/new", methods=["GET", "POST"])
def new_product():
    if "user_id" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        try:
            product_data = parse_product_form(request.form)
            images = request.files.getlist("images")
            if not any(image and image.filename for image in images):
                raise ValueError("상품 사진을 1장 이상 첨부해주세요.")

            product_id = str(uuid.uuid4())
            saved_images = save_product_images(images, product_id)

            db = get_db()
            cursor = db.cursor()

            cursor.execute(
                """
                INSERT INTO product (
                    id, title, description, price, seller_id,
                    category, condition, trade_method, location,
                    negotiable, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product_id,
                    product_data["title"],
                    product_data["description"],
                    product_data["price"],
                    session["user_id"],
                    product_data["category"],
                    product_data["condition"],
                    product_data["trade_method"],
                    product_data["location"],
                    product_data["negotiable"],
                    product_data["status"],
                ),
            )

            for filename, sort_order in saved_images:
                cursor.execute(
                    """
                    INSERT INTO product_image (id, product_id, filename, sort_order)
                    VALUES (?, ?, ?, ?)
                    """,
                    (str(uuid.uuid4()), product_id, filename, sort_order),
                )

            db.commit()

        except ValueError as exc:
            flash(str(exc))
            return redirect(url_for("new_product"))
        except sqlite3.Error:
            if 'saved_images' in locals():
                for filename, _ in saved_images:
                    remove_product_image_file(filename)
            get_db().rollback()
            flash("상품 등록 중 오류가 발생했습니다.")
            return redirect(url_for("new_product"))

        flash("상품이 등록되었습니다.")
        return redirect(url_for("view_product", product_id=product_id))

    return render_template(
        "new_product.html",
        categories=PRODUCT_CATEGORIES,
        conditions=PRODUCT_CONDITIONS,
        trade_methods=TRADE_METHODS,
    )


# 상품 수정
@app.route("/product/<product_id>/edit", methods=["GET", "POST"])
def edit_product(product_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute("SELECT * FROM product WHERE id = ?", (product_id,))
    product = cursor.fetchone()

    if product is None:
        abort(404)
    if product["seller_id"] != session["user_id"]:
        abort(403)

    cursor.execute(
        """
        SELECT id, filename, sort_order
        FROM product_image
        WHERE product_id = ?
        ORDER BY sort_order, rowid
        """,
        (product_id,),
    )
    existing_images = [
        image for image in cursor.fetchall()
        if product_image_exists(image["filename"])
    ]

    if request.method == "POST":
        try:
            product_data = parse_product_form(request.form)
            new_files = request.files.getlist("images")
            saved_images = save_product_images(
                new_files,
                product_id,
                start_order=len(existing_images),
            )

            cursor.execute(
                """
                UPDATE product
                SET title = ?, description = ?, price = ?, category = ?,
                    condition = ?, trade_method = ?, location = ?,
                    negotiable = ?, status = ?
                WHERE id = ? AND seller_id = ?
                """,
                (
                    product_data["title"],
                    product_data["description"],
                    product_data["price"],
                    product_data["category"],
                    product_data["condition"],
                    product_data["trade_method"],
                    product_data["location"],
                    product_data["negotiable"],
                    product_data["status"],
                    product_id,
                    session["user_id"],
                ),
            )

            for filename, sort_order in saved_images:
                cursor.execute(
                    """
                    INSERT INTO product_image (id, product_id, filename, sort_order)
                    VALUES (?, ?, ?, ?)
                    """,
                    (str(uuid.uuid4()), product_id, filename, sort_order),
                )

            db.commit()

        except ValueError as exc:
            flash(str(exc))
            return redirect(url_for("edit_product", product_id=product_id))
        except sqlite3.Error:
            if 'saved_images' in locals():
                for filename, _ in saved_images:
                    remove_product_image_file(filename)
            db.rollback()
            flash("상품 수정 중 오류가 발생했습니다.")
            return redirect(url_for("edit_product", product_id=product_id))

        flash("상품 정보가 수정되었습니다.")
        return redirect(url_for("view_product", product_id=product_id))

    return render_template(
        "edit_product.html",
        product=product,
        images=existing_images,
        categories=PRODUCT_CATEGORIES,
        conditions=PRODUCT_CONDITIONS,
        trade_methods=TRADE_METHODS,
        statuses=PRODUCT_STATUSES,
    )


@app.route("/product/<product_id>/image/<image_id>/delete", methods=["POST"])
def delete_product_image(product_id, image_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT seller_id FROM product WHERE id = ?",
        (product_id,),
    )
    product = cursor.fetchone()
    if product is None:
        abort(404)
    if product["seller_id"] != session["user_id"]:
        abort(403)

    cursor.execute(
        "SELECT COUNT(*) AS count FROM product_image WHERE product_id = ?",
        (product_id,),
    )
    if cursor.fetchone()["count"] <= 1:
        flash("상품에는 최소 1장의 사진이 필요합니다.")
        return redirect(url_for("edit_product", product_id=product_id))

    cursor.execute(
        "SELECT filename FROM product_image WHERE id = ? AND product_id = ?",
        (image_id, product_id),
    )
    image = cursor.fetchone()
    if image is None:
        abort(404)

    cursor.execute("DELETE FROM product_image WHERE id = ?", (image_id,))
    db.commit()
    remove_product_image_file(image["filename"])
    flash("상품 사진을 삭제했습니다.")
    return redirect(url_for("edit_product", product_id=product_id))


# 상품 삭제
@app.route("/product/<product_id>/delete", methods=["POST"])
def delete_product(product_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, title, seller_id FROM product WHERE id = ?",
        (product_id,),
    )
    product = cursor.fetchone()
    if product is None:
        abort(404)

    cursor.execute(
        "SELECT id, is_admin FROM user WHERE id = ?",
        (session["user_id"],),
    )
    current_user = cursor.fetchone()
    if current_user is None:
        session.clear()
        return redirect(url_for("login"))

    if product["seller_id"] != session["user_id"] and current_user["is_admin"] != 1:
        abort(403)

    cursor.execute(
        "SELECT filename FROM product_image WHERE product_id = ?",
        (product_id,),
    )
    image_filenames = [row["filename"] for row in cursor.fetchall()]

    try:
        cursor.execute("DELETE FROM report WHERE target_id = ?", (product_id,))
        cursor.execute("DELETE FROM favorite WHERE product_id = ?", (product_id,))
        cursor.execute("DELETE FROM product_image WHERE product_id = ?", (product_id,))
        cursor.execute("DELETE FROM product WHERE id = ?", (product_id,))
        db.commit()
    except sqlite3.Error:
        db.rollback()
        flash("상품 삭제 중 오류가 발생했습니다.")
        return redirect(url_for("view_product", product_id=product_id))

    for filename in image_filenames:
        remove_product_image_file(filename)

    flash(f"{product['title']} 상품이 삭제되었습니다.")
    return redirect(url_for("admin" if current_user["is_admin"] == 1 else "my_products"))


# 상품 상세
@app.route("/product/<product_id>")
def view_product(product_id):
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT * FROM product
        WHERE id = ? AND is_hidden = 0
        """,
        (product_id,),
    )
    product = cursor.fetchone()

    if product is None:
        flash("상품을 찾을 수 없거나 차단된 상품입니다.")
        return redirect(url_for("dashboard" if "user_id" in session else "index"))

    viewed_products = session.get("viewed_products", [])
    if product_id not in viewed_products:
        cursor.execute(
            "UPDATE product SET view_count = view_count + 1 WHERE id = ?",
            (product_id,),
        )
        db.commit()
        viewed_products.append(product_id)
        session["viewed_products"] = viewed_products[-200:]
        cursor.execute("SELECT * FROM product WHERE id = ?", (product_id,))
        product = cursor.fetchone()

    cursor.execute(
        "SELECT id, username, nickname, bio, created_at FROM user WHERE id = ?",
        (product["seller_id"],),
    )
    seller = cursor.fetchone()

    cursor.execute(
        """
        SELECT id, filename, sort_order
        FROM product_image
        WHERE product_id = ?
        ORDER BY sort_order, rowid
        """,
        (product_id,),
    )
    images = [
        image for image in cursor.fetchall()
        if product_image_exists(image["filename"])
    ]

    cursor.execute(
        "SELECT COUNT(*) AS count FROM favorite WHERE product_id = ?",
        (product_id,),
    )
    favorite_count = cursor.fetchone()["count"]

    is_favorited = False
    if session.get("user_id"):
        cursor.execute(
            "SELECT id FROM favorite WHERE user_id = ? AND product_id = ?",
            (session["user_id"], product_id),
        )
        is_favorited = cursor.fetchone() is not None

    return render_template(
        "view_product.html",
        product=product,
        seller=seller,
        images=images,
        favorite_count=favorite_count,
        is_favorited=is_favorited,
    )


@app.route("/product/<product_id>/favorite", methods=["POST"])
def toggle_favorite(product_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, seller_id FROM product WHERE id = ? AND is_hidden = 0",
        (product_id,),
    )
    product = cursor.fetchone()
    if product is None:
        abort(404)

    if product["seller_id"] == session["user_id"]:
        flash("본인의 상품은 찜할 수 없습니다.")
        return redirect(url_for("view_product", product_id=product_id))

    cursor.execute(
        "SELECT id FROM favorite WHERE user_id = ? AND product_id = ?",
        (session["user_id"], product_id),
    )
    favorite = cursor.fetchone()

    if favorite:
        cursor.execute("DELETE FROM favorite WHERE id = ?", (favorite["id"],))
        flash("찜 목록에서 삭제했습니다.")
    else:
        cursor.execute(
            "INSERT INTO favorite (id, user_id, product_id) VALUES (?, ?, ?)",
            (str(uuid.uuid4()), session["user_id"], product_id),
        )
        flash("관심 상품으로 찜했습니다.")

    db.commit()
    return redirect(request.referrer or url_for("view_product", product_id=product_id))


@app.route("/favorites")
def favorites():
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT
            product.*,
            user.username AS seller_username,
            (
                SELECT filename FROM product_image
                WHERE product_image.product_id = product.id
                ORDER BY sort_order, rowid LIMIT 1
            ) AS image_filename,
            (
                SELECT COUNT(*) FROM favorite AS count_favorite
                WHERE count_favorite.product_id = product.id
            ) AS favorite_count
        FROM favorite
        JOIN product ON product.id = favorite.product_id
        LEFT JOIN user ON user.id = product.seller_id
        WHERE favorite.user_id = ? AND product.is_hidden = 0
        ORDER BY favorite.created_at DESC
        """,
        (session["user_id"],),
    )
    return render_template("favorites.html", products=cursor.fetchall())


@app.route("/my-products")
def my_products():
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT
            product.*,
            (
                SELECT filename FROM product_image
                WHERE product_image.product_id = product.id
                ORDER BY sort_order, rowid LIMIT 1
            ) AS image_filename,
            (
                SELECT COUNT(*) FROM favorite
                WHERE favorite.product_id = product.id
            ) AS favorite_count
        FROM product
        WHERE product.seller_id = ?
        ORDER BY product.created_at DESC, product.rowid DESC
        """,
        (session["user_id"],),
    )
    return render_template("my_products.html", products=cursor.fetchall())


@app.route("/my-products/<product_id>/status", methods=["POST"])
def change_product_status(product_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    status = request.form.get("status", "").strip()
    if status not in PRODUCT_STATUSES:
        flash("올바른 판매 상태를 선택해주세요.")
        return redirect(url_for("my_products"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE product SET status = ?
        WHERE id = ? AND seller_id = ?
        """,
        (status, product_id, session["user_id"]),
    )
    db.commit()

    if cursor.rowcount != 1:
        abort(404)

    flash("판매 상태를 변경했습니다.")
    return redirect(request.referrer or url_for("my_products"))

# 신고
@app.route("/report", methods=["GET", "POST"])
def report():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    target_id = (
        request.form.get("target_id", "").strip()
        if request.method == "POST"
        else request.args.get("target_id", "").strip()
    )
    requested_type = (
        request.form.get("target_type", "").strip().lower()
        if request.method == "POST"
        else request.args.get("target_type", "").strip().lower()
    )
    if requested_type not in {"", "product", "user"}:
        requested_type = ""

    db = get_db()
    cursor = db.cursor()

    # 상단 메뉴에서 들어오면 상품과 사용자를 한 화면에서 선택한다.
    if not target_id:
        cursor.execute(
            """
            SELECT id, title
            FROM product
            WHERE is_hidden = 0
              AND seller_id != ?
            ORDER BY rowid DESC
            """,
            (session["user_id"],),
        )
        reportable_products = cursor.fetchall()

        cursor.execute(
            """
            SELECT id, username, nickname
            FROM user
            WHERE id != ?
              AND is_suspended = 0
              AND is_admin = 0
            ORDER BY COALESCE(NULLIF(TRIM(nickname), ''), username), username
            """,
            (session["user_id"],),
        )
        reportable_users = cursor.fetchall()

        return render_template(
            "report_select.html",
            products=reportable_products,
            users=reportable_users,
        )

    target_user = None
    target_product = None

    if requested_type in {"", "user"}:
        cursor.execute(
            """
            SELECT id, username, nickname, is_admin
            FROM user
            WHERE id = ?
            """,
            (target_id,),
        )
        target_user = cursor.fetchone()

    if requested_type in {"", "product"}:
        cursor.execute(
            """
            SELECT id, title, seller_id, is_hidden
            FROM product
            WHERE id = ?
            """,
            (target_id,),
        )
        target_product = cursor.fetchone()

    if requested_type == "user":
        target_product = None
    elif requested_type == "product":
        target_user = None

    if target_user is None and target_product is None:
        flash("존재하지 않는 신고 대상입니다.")
        return redirect(url_for("report"))

    if target_product is not None:
        target_name = target_product["title"]
        target_type = "product"
        target_type_label = "상품"
    else:
        target_name = target_user["nickname"] or target_user["username"]
        target_type = "user"
        target_type_label = "사용자"

    if request.method == "POST":
        reason = request.form.get("reason", "").strip()

        if len(reason) < 5 or len(reason) > 500:
            flash("신고 사유는 5자 이상 500자 이하로 입력해주세요.")
            return redirect(
                url_for(
                    "report",
                    target_id=target_id,
                    target_type=target_type,
                )
            )

        if target_user is not None:
            if target_user["id"] == session["user_id"]:
                flash("자기 자신은 신고할 수 없습니다.")
                return redirect(url_for("dashboard"))
            if target_user["is_admin"] == 1:
                flash("관리자 계정은 일반 신고 대상으로 선택할 수 없습니다.")
                return redirect(url_for("report"))

        if (
            target_product is not None
            and target_product["seller_id"] == session["user_id"]
        ):
            flash("자신이 등록한 상품은 신고할 수 없습니다.")
            return redirect(
                url_for("view_product", product_id=target_product["id"])
            )

        cursor.execute(
            """
            SELECT COUNT(*) AS report_count
            FROM report
            WHERE reporter_id = ?
              AND datetime(created_at) >= datetime('now', '-1 hour')
            """,
            (session["user_id"],),
        )
        recent_report_count = cursor.fetchone()["report_count"]

        if recent_report_count >= REPORT_HOURLY_LIMIT:
            flash("신고는 한 시간에 최대 5건까지 접수할 수 있습니다.")
            return redirect(url_for("report"))

        try:
            cursor.execute(
                """
                INSERT INTO report (
                    id,
                    reporter_id,
                    target_id,
                    target_type,
                    reason,
                    status,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    str(uuid.uuid4()),
                    session["user_id"],
                    target_id,
                    target_type,
                    reason,
                    datetime.now().astimezone().isoformat(timespec="seconds"),
                ),
            )

            if target_type == "product":
                cursor.execute(
                    """
                    UPDATE product
                    SET report_count = report_count + 1
                    WHERE id = ?
                    """,
                    (target_id,),
                )
            else:
                cursor.execute(
                    """
                    UPDATE user
                    SET report_count = report_count + 1
                    WHERE id = ?
                    """,
                    (target_id,),
                )

            db.commit()

        except sqlite3.IntegrityError:
            db.rollback()
            flash("이미 신고한 대상입니다.")
            return redirect(
                url_for(
                    "report",
                    target_id=target_id,
                    target_type=target_type,
                )
            )
        except sqlite3.Error:
            db.rollback()
            flash("신고 처리 중 오류가 발생했습니다.")
            return redirect(
                url_for(
                    "report",
                    target_id=target_id,
                    target_type=target_type,
                )
            )

        flash("신고가 접수되었습니다. 관리자가 내용을 검토합니다.")
        return redirect(url_for("dashboard"))

    return render_template(
        "report.html",
        target_id=target_id,
        target_name=target_name,
        target_type=target_type,
        target_type_label=target_type_label,
    )

# 햇켓페이 송금
@app.route("/transfer", methods=["GET", "POST"])
def transfer():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    current_user_id = session["user_id"]
    db = get_db()
    cursor = db.cursor()

    product_id = (
        request.form.get("product_id", "").strip()
        if request.method == "POST"
        else request.args.get("product_id", "").strip()
    )

    selected_product = None

    if product_id:
        cursor.execute(
            """
            SELECT
                product.id,
                product.title,
                product.price,
                product.status,
                product.seller_id,
                seller.username AS seller_username,
                COALESCE(seller.nickname, seller.username) AS seller_name
            FROM product
            JOIN user AS seller
              ON seller.id = product.seller_id
            WHERE product.id = ?
              AND product.is_hidden = 0
              AND seller.is_suspended = 0
            """,
            (product_id,),
        )
        selected_product = cursor.fetchone()

        if selected_product is None:
            flash("송금할 상품을 찾을 수 없습니다.")
            return redirect(url_for("dashboard"))

        if selected_product["seller_id"] == current_user_id:
            flash("본인의 상품에는 송금할 수 없습니다.")
            return redirect(
                url_for("view_product", product_id=product_id)
            )

        if selected_product["status"] == "판매완료":
            flash("이미 거래가 완료된 상품입니다.")
            return redirect(
                url_for("view_product", product_id=product_id)
            )

        if users_have_block_relation(
            cursor,
            current_user_id,
            selected_product["seller_id"],
        ):
            flash("차단 관계인 사용자에게는 송금할 수 없습니다.")
            return redirect(
                url_for("view_product", product_id=product_id)
            )

    if request.method == "POST":
        payment_pin = request.form.get("payment_pin", "").strip()

        cursor.execute(
            """
            SELECT payment_password
            FROM user
            WHERE id = ?
              AND is_suspended = 0
            """,
            (current_user_id,),
        )
        payment_security = cursor.fetchone()

        if payment_security is None:
            session.clear()
            flash("사용자 정보를 확인할 수 없습니다.")
            return redirect(url_for("login"))

        if not payment_security["payment_password"]:
            flash("송금 전에 결제 비밀번호를 먼저 설정해주세요.")
            return redirect(url_for("payment_password"))

        now_timestamp = datetime.now().timestamp()
        locked_until = float(
            session.get("payment_pin_locked_until", 0) or 0
        )

        if locked_until > now_timestamp:
            remaining_seconds = int(locked_until - now_timestamp) + 1
            flash(
                f"결제 비밀번호 입력이 잠겼습니다. "
                f"{remaining_seconds}초 후 다시 시도해주세요."
            )
            return redirect(
                url_for("transfer", product_id=product_id)
                if product_id
                else url_for("transfer")
            )

        if not re.fullmatch(r"\d{6}", payment_pin) or not check_password_hash(
            payment_security["payment_password"],
            payment_pin,
        ):
            failures = int(session.get("payment_pin_failures", 0)) + 1

            if failures >= 5:
                session["payment_pin_failures"] = 0
                session["payment_pin_locked_until"] = now_timestamp + 300
                flash(
                    "결제 비밀번호를 5회 잘못 입력하여 "
                    "5분 동안 송금이 잠겼습니다."
                )
            else:
                session["payment_pin_failures"] = failures
                flash(
                    f"결제 비밀번호가 올바르지 않습니다. "
                    f"남은 시도 횟수: {5 - failures}회"
                )

            return redirect(
                url_for("transfer", product_id=product_id)
                if product_id
                else url_for("transfer")
            )

        session.pop("payment_pin_failures", None)
        session.pop("payment_pin_locked_until", None)

        if selected_product is not None:
            receiver_id = selected_product["seller_id"]
            receiver_name = selected_product["seller_name"]
            amount = selected_product["price"]
        else:
            receiver_username = request.form.get(
                "receiver_username",
                "",
            ).strip()
            amount_text = request.form.get("amount", "").strip()

            try:
                amount = int(amount_text)
            except ValueError:
                flash("송금 금액은 원 단위 숫자로 입력해주세요.")
                return redirect(url_for("transfer"))

            if amount <= 0:
                flash("송금 금액은 1원 이상이어야 합니다.")
                return redirect(url_for("transfer"))

            if amount > 100_000_000:
                flash("한 번에 최대 1억 원까지 송금할 수 있습니다.")
                return redirect(url_for("transfer"))

            cursor.execute(
                """
                SELECT
                    id,
                    username,
                    COALESCE(nickname, username) AS display_name
                FROM user
                WHERE username = ?
                  AND is_suspended = 0
                """,
                (receiver_username,),
            )
            receiver = cursor.fetchone()

            if receiver is None:
                flash("존재하지 않거나 이용이 정지된 사용자입니다.")
                return redirect(url_for("transfer"))

            receiver_id = receiver["id"]
            receiver_name = receiver["display_name"]

            if receiver_id == current_user_id:
                flash("자기 자신에게는 송금할 수 없습니다.")
                return redirect(url_for("transfer"))

            if users_have_block_relation(
                cursor,
                current_user_id,
                receiver_id,
            ):
                flash("차단 관계인 사용자에게는 송금할 수 없습니다.")
                return redirect(url_for("transfer"))

        try:
            # 잔액 차감·증가·거래 기록·상품 상태 변경을 한 번에 처리
            db.execute("BEGIN IMMEDIATE")

            if product_id:
                cursor.execute(
                    """
                    SELECT
                        product.price,
                        product.seller_id,
                        product.status,
                        product.is_hidden,
                        seller.is_suspended,
                        COALESCE(seller.nickname, seller.username) AS seller_name
                    FROM product
                    JOIN user AS seller
                      ON seller.id = product.seller_id
                    WHERE product.id = ?
                    """,
                    (product_id,),
                )
                locked_product = cursor.fetchone()

                if (
                    locked_product is None
                    or locked_product["is_hidden"] == 1
                    or locked_product["is_suspended"] == 1
                    or locked_product["status"] == "판매완료"
                    or locked_product["seller_id"] == current_user_id
                ):
                    db.rollback()
                    flash("상품 상태가 변경되어 송금할 수 없습니다.")
                    return redirect(
                        url_for("view_product", product_id=product_id)
                    )

                cursor.execute(
                    """
                    SELECT id
                    FROM transfer
                    WHERE product_id = ?
                    LIMIT 1
                    """,
                    (product_id,),
                )
                if cursor.fetchone() is not None:
                    db.rollback()
                    flash("이미 결제가 완료된 상품입니다.")
                    return redirect(
                        url_for("view_product", product_id=product_id)
                    )

                # 화면에서 전달된 값이 아니라 DB의 최신 상품 가격·판매자를 사용
                amount = locked_product["price"]
                receiver_id = locked_product["seller_id"]
                receiver_name = locked_product["seller_name"]

            cursor.execute(
                """
                UPDATE user
                SET balance = balance - ?
                WHERE id = ?
                  AND is_suspended = 0
                  AND balance >= ?
                """,
                (amount, current_user_id, amount),
            )

            if cursor.rowcount != 1:
                db.rollback()
                flash("송금 가능 잔액이 부족합니다.")
                return redirect(
                    url_for("transfer", product_id=product_id)
                    if product_id
                    else url_for("transfer")
                )

            cursor.execute(
                """
                UPDATE user
                SET balance = balance + ?
                WHERE id = ?
                  AND is_suspended = 0
                """,
                (amount, receiver_id),
            )

            if cursor.rowcount != 1:
                raise sqlite3.Error("받는 사용자 잔액 갱신 실패")

            cursor.execute(
                """
                INSERT INTO transfer (
                    id,
                    sender_id,
                    receiver_id,
                    amount,
                    product_id
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    current_user_id,
                    receiver_id,
                    amount,
                    product_id or None,
                ),
            )

            if product_id:
                cursor.execute(
                    """
                    UPDATE product
                    SET status = '판매완료'
                    WHERE id = ?
                      AND status != '판매완료'
                    """,
                    (product_id,),
                )

                if cursor.rowcount != 1:
                    raise sqlite3.Error("상품 판매 상태 갱신 실패")

            db.commit()

        except sqlite3.IntegrityError:
            db.rollback()
            flash("이미 처리된 상품 결제이거나 중복 송금 요청입니다.")
            return redirect(
                url_for("view_product", product_id=product_id)
                if product_id
                else url_for("transfer")
            )

        except sqlite3.Error:
            db.rollback()
            flash("송금 처리 중 오류가 발생했습니다.")
            return redirect(
                url_for("transfer", product_id=product_id)
                if product_id
                else url_for("transfer")
            )

        flash(f"{receiver_name}님에게 {amount:,}원을 송금했습니다.")

        if product_id:
            return redirect(
                url_for("view_product", product_id=product_id)
            )

        return redirect(url_for("transfer"))

    cursor.execute(
        """
        SELECT id, username, nickname, balance, payment_password
        FROM user
        WHERE id = ?
        """,
        (current_user_id,),
    )
    current_user = cursor.fetchone()

    if current_user is None:
        session.clear()
        flash("사용자 정보를 확인할 수 없습니다.")
        return redirect(url_for("login"))

    cursor.execute(
        """
        SELECT
            transfer.id,
            transfer.sender_id,
            transfer.receiver_id,
            transfer.amount,
            transfer.created_at,
            transfer.product_id,
            COALESCE(sender.nickname, sender.username) AS sender_name,
            COALESCE(receiver.nickname, receiver.username) AS receiver_name,
            product.title AS product_title
        FROM transfer
        JOIN user AS sender
          ON sender.id = transfer.sender_id
        JOIN user AS receiver
          ON receiver.id = transfer.receiver_id
        LEFT JOIN product
          ON product.id = transfer.product_id
        WHERE transfer.sender_id = ?
           OR transfer.receiver_id = ?
        ORDER BY transfer.created_at DESC, transfer.rowid DESC
        LIMIT 30
        """,
        (current_user_id, current_user_id),
    )
    transfers = cursor.fetchall()

    return render_template(
        "transfer.html",
        user=current_user,
        transfers=transfers,
        selected_product=selected_product,
        has_payment_password=bool(current_user["payment_password"]),
    )

# 채팅 페이지
@app.route("/chat")
def chat():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, nickname
        FROM user
        WHERE id = ?
        """,
        (session["user_id"],),
    )
    current_user = cursor.fetchone()

    if current_user is None:
        session.clear()
        flash("사용자 정보를 찾을 수 없습니다.")
        return redirect(url_for("login"))

    cursor.execute(
        """
        SELECT
            room.id,
            room.product_id,
            CASE
                WHEN room.user1_id = ? THEN other2.id
                ELSE other1.id
            END AS other_user_id,
            CASE
                WHEN room.user1_id = ? THEN COALESCE(other2.nickname, other2.username)
                ELSE COALESCE(other1.nickname, other1.username)
            END AS other_name,
            product.title AS product_title,
            (
                SELECT message
                FROM private_chat_message
                WHERE room_id = room.id
                ORDER BY created_at DESC, rowid DESC
                LIMIT 1
            ) AS last_message,
            (
                SELECT created_at
                FROM private_chat_message
                WHERE room_id = room.id
                ORDER BY created_at DESC, rowid DESC
                LIMIT 1
            ) AS last_message_at,
            (
                SELECT COUNT(*)
                FROM private_chat_message
                WHERE room_id = room.id
                  AND sender_id != ?
                  AND read_at IS NULL
            ) AS unread_count
        FROM private_chat_room AS room
        JOIN user AS other1 ON room.user1_id = other1.id
        JOIN user AS other2 ON room.user2_id = other2.id
        LEFT JOIN product ON room.product_id = product.id
        WHERE room.user1_id = ? OR room.user2_id = ?
        ORDER BY COALESCE(last_message_at, room.created_at) DESC
        """,
        (
            session["user_id"],
            session["user_id"],
            session["user_id"],
            session["user_id"],
            session["user_id"],
        ),
    )
    private_rooms = cursor.fetchall()
    
    private_rooms = [
        room
        for room in private_rooms
        if not users_have_block_relation(
            cursor,
            session["user_id"],
            room["other_user_id"],
        )
    ]

    return render_template(
        "chat.html",
        user=current_user,
        private_rooms=private_rooms,
    )
# 소켓 연결 시 사용자별 개인 방 입장
@socketio.on("connect")
def handle_socket_connect():
    user_id = session.get("user_id")

    if not user_id:
        return False

    # 사용자 개인 Socket.IO 방
    join_room(f"user:{user_id}")
    return None


# 전체 채팅 메시지 전송
@socketio.on("send_message")
def handle_send_message_event(data):
    user_id = session.get("user_id")

    if not user_id:
        emit(
            "chat_error",
            {
                "message": "로그인이 필요합니다."
            },
        )
        return

    if not isinstance(data, dict):
        return

    message = str(
        data.get("message", "")
    ).strip()

    if not message:
        emit(
            "chat_error",
            {
                "message": "메시지를 입력해주세요."
            },
        )
        return

    if len(message) > 500:
        emit(
            "chat_error",
            {
                "message": (
                    "메시지는 최대 500자까지 "
                    "전송할 수 있습니다."
                )
            },
        )
        return

    if not realtime_action_allowed(
        ("global_chat", user_id),
        limit=1,
        window_seconds=GLOBAL_CHAT_WINDOW_SECONDS,
    ):
        emit(
            "chat_error",
            {
                "message": "전체 채팅은 3초에 한 번만 보낼 수 있습니다."
            },
        )
        return

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT
            id,
            username,
            COALESCE(nickname, username)
                AS display_name
        FROM user
        WHERE id = ?
          AND is_suspended = 0
        """,
        (user_id,),
    )

    sender = cursor.fetchone()

    if sender is None:
        emit(
            "chat_error",
            {
                "message": (
                    "사용자 정보를 "
                    "확인할 수 없습니다."
                )
            },
        )
        return

    message_id = str(uuid.uuid4())

    created_at = (
        datetime.now()
        .astimezone()
        .isoformat(timespec="seconds")
    )

    message_data = {
        "message_id": message_id,
        "sender_id": sender["id"],
        "username": sender["display_name"],
        "message": message,
        "created_at": created_at,
    }

    unread_chat_messages[message_id] = (
        sender["id"]
    )

    if len(unread_chat_messages) > 1000:
        oldest_message_id = next(
            iter(unread_chat_messages)
        )

        unread_chat_messages.pop(
            oldest_message_id,
            None,
        )

    # 현재 발신자와 차단 관계가 없는 사용자만 조회
    cursor.execute(
        """
        SELECT recipient.id
        FROM user AS recipient
        WHERE recipient.is_suspended = 0
          AND NOT EXISTS (
              SELECT 1
              FROM user_block
              WHERE (
                  blocker_id = ?
                  AND blocked_id = recipient.id
              )
              OR (
                  blocker_id = recipient.id
                  AND blocked_id = ?
              )
          )
        """,
        (
            user_id,
            user_id,
        ),
    )

    recipients = cursor.fetchall()

    print(
        "[전체 채팅 전송]",
        user_id,
        "수신자 수:",
        len(recipients),
    )

    for recipient in recipients:
        socketio.emit(
            "message",
            message_data,
            to=f"user:{recipient['id']}",
        )


# 다른 사용자가 메시지를 수신했을 때 읽음 처리
@socketio.on("message_read")
def handle_message_read_event(data):
    reader_id = session.get("user_id")

    if not reader_id or not isinstance(data, dict):
        return

    message_id = str(data.get("message_id", "")).strip()

    if not message_id:
        return

    sender_id = unread_chat_messages.get(message_id)

    if sender_id is None:
        return

    # 발신자가 자기 메시지에 읽음 신호를 보내는 것 차단
    if sender_id == reader_id:
        return

    # 첫 번째 읽음 확인 이후에는 중복 신호 제거
    unread_chat_messages.pop(message_id, None)

    # 해당 메시지를 보낸 사용자의 개인 소켓 방으로만 전달
    emit(
        "message_read",
        {
            "message_id": message_id,
        },
        to=sender_id,
    )
# 사용자 차단
@app.route("/users/<user_id>/block", methods=["POST"])
def block_user(user_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    current_user_id = session["user_id"]

    if user_id == current_user_id:
        flash("자기 자신은 차단할 수 없습니다.")
        return redirect(
            url_for(
                "public_profile",
                user_id=user_id,
            )
        )

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id
        FROM user
        WHERE id = ?
          AND is_suspended = 0
        """,
        (user_id,),
    )

    if cursor.fetchone() is None:
        abort(404)

    user1_id, user2_id = normalize_user_pair(
        current_user_id,
        user_id,
    )

    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO user_block (
                id,
                blocker_id,
                blocked_id
            )
            VALUES (?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                current_user_id,
                user_id,
            ),
        )

        # 기존 친구 관계 제거
        cursor.execute(
            """
            DELETE FROM friendship
            WHERE user1_id = ?
              AND user2_id = ?
            """,
            (user1_id, user2_id),
        )

        # 양쪽 친구 신청 기록 제거
        cursor.execute(
            """
            DELETE FROM friend_request
            WHERE (
                sender_id = ?
                AND receiver_id = ?
            )
            OR (
                sender_id = ?
                AND receiver_id = ?
            )
            """,
            (
                current_user_id,
                user_id,
                user_id,
                current_user_id,
            ),
        )

        db.commit()
        # 현재 사용자의 전체 채팅 화면에 차단 상태 전달
        socketio.emit(
            "global_block_updated",
            {
                "other_user_id": user_id,
                "blocked": True,
            },
            to=str(current_user_id),
        )

        # 차단된 상대방의 전체 채팅 화면에도 전달
        socketio.emit(
            "global_block_updated",
            {
                "other_user_id": current_user_id,
                "blocked": True,
            },
            to=str(user_id),
        )
        

    except sqlite3.Error:
        db.rollback()
        flash("사용자 차단 중 오류가 발생했습니다.")

        return redirect(
            url_for(
                "public_profile",
                user_id=user_id,
            )
        )

    flash("사용자를 차단했습니다. 해당 사용자의 상품과 활동이 숨겨집니다.")

    return redirect(url_for("dashboard"))


# 사용자 차단 해제
@app.route("/users/<user_id>/unblock", methods=["POST"])
def unblock_user(user_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        DELETE FROM user_block
        WHERE blocker_id = ?
          AND blocked_id = ?
        """,
        (
            session["user_id"],
            user_id,
        ),
    )

    db.commit()
    socketio.emit(
        "global_block_updated",
        {
            "other_user_id": user_id,
            "blocked": False,
        },
        to=str(session["user_id"]),
    )

    socketio.emit(
        "global_block_updated",
        {
            "other_user_id": session["user_id"],
            "blocked": False,
        },
        to=str(user_id),
    )

    if cursor.rowcount == 1:
        flash("사용자 차단을 해제했습니다.")
    else:
        flash("차단된 사용자가 아닙니다.")

        if (
        request.form.get("return_to")
        == "blocked_management"
    ):
            return redirect(
            url_for("blocked_management")
        )

    return redirect(
        url_for(
            "public_profile",
            user_id=user_id,
        )
    )

# 특정 상품을 내 피드에서 숨기기
@app.route("/product/<product_id>/hide-for-me", methods=["POST"])
def hide_product_for_me(product_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, seller_id
        FROM product
        WHERE id = ?
          AND is_hidden = 0
        """,
        (product_id,),
    )

    product = cursor.fetchone()

    if product is None:
        abort(404)

    if product["seller_id"] == session["user_id"]:
        flash("내 상품은 관심 없음 처리할 수 없습니다.")
        return redirect(
            url_for(
                "view_product",
                product_id=product_id,
            )
        )

    cursor.execute(
        """
        INSERT OR IGNORE INTO hidden_product (
            id,
            user_id,
            product_id
        )
        VALUES (?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            session["user_id"],
            product_id,
        ),
    )

    db.commit()

    flash("이 상품을 내 피드에서 숨겼습니다.")

    return redirect(url_for("dashboard"))


# 숨긴 상품 다시 표시
@app.route("/product/<product_id>/unhide-for-me", methods=["POST"])
def unhide_product_for_me(product_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        DELETE FROM hidden_product
        WHERE user_id = ?
          AND product_id = ?
        """,
        (
            session["user_id"],
            product_id,
        ),
    )

    db.commit()

    if cursor.rowcount == 1:
        flash("숨긴 상품을 다시 표시합니다.")
    else:
        flash("숨긴 상품이 아닙니다.")

        if (
        request.form.get("return_to")
        == "blocked_management"
    ):
            return redirect(
            url_for("blocked_management")
        )

    if (
        request.form.get("return_to")
        == "blocked_management"
    ):
        return redirect(
            url_for("blocked_management")
        )

    return redirect(
        url_for(
            "view_product",
            product_id=product_id,
        )
    )   
# 차단한 사용자 및 숨긴 상품 관리
@app.route("/mypage/blocked")
def blocked_management():
    if "user_id" not in session:
        return redirect(url_for("login"))

    current_user_id = session["user_id"]

    db = get_db()
    cursor = db.cursor()

    # 내가 차단한 사용자 목록
    cursor.execute(
        """
        SELECT
            user.id,
            user.username,
            user.nickname,
            user.bio,
            user_block.created_at AS blocked_at
        FROM user_block
        JOIN user
          ON user.id = user_block.blocked_id
        WHERE user_block.blocker_id = ?
        ORDER BY user_block.created_at DESC
        """,
        (current_user_id,),
    )

    blocked_users = cursor.fetchall()

    # 내가 관심 없음 처리한 상품 목록
    cursor.execute(
        """
        SELECT
            product.id,
            product.title,
            product.price,
            product.status,
            product.seller_id,
            hidden_product.created_at AS hidden_at,
            COALESCE(
                seller.nickname,
                seller.username
            ) AS seller_name
        FROM hidden_product
        JOIN product
          ON product.id = hidden_product.product_id
        JOIN user AS seller
          ON seller.id = product.seller_id
        WHERE hidden_product.user_id = ?
        ORDER BY hidden_product.created_at DESC
        """,
        (current_user_id,),
    )

    hidden_products = cursor.fetchall()

    return render_template(
        "blocked_management.html",
        blocked_users=blocked_users,
        hidden_products=hidden_products,
    )

# 공개 사용자 프로필
@app.route("/users/<user_id>")
def public_profile(user_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, nickname, bio, created_at, is_suspended
        FROM user
        WHERE id = ?
        """,
        (user_id,),
    )
    profile_user = cursor.fetchone()

    if profile_user is None or profile_user["is_suspended"] == 1:
        abort(404)

    cursor.execute(
        """
        SELECT
            product.*,
            (
                SELECT filename
                FROM product_image
                WHERE product_id = product.id
                ORDER BY sort_order, rowid
                LIMIT 1
            ) AS image_filename,
            (
                SELECT COUNT(*)
                FROM favorite
                WHERE product_id = product.id
            ) AS favorite_count
        FROM product
        WHERE seller_id = ?
          AND is_hidden = 0
        ORDER BY created_at DESC, rowid DESC
        LIMIT 12
        """,
        (user_id,),
    )
    products = cursor.fetchall()

    current_user_id = session["user_id"]

    is_self = user_id == current_user_id
    is_friend = False
    outgoing_request = None
    incoming_request = None
    is_blocked_by_me = False
    is_blocking_me = False

    if not is_self:
        cursor.execute(
            """
            SELECT id
            FROM user_block
            WHERE blocker_id = ?
              AND blocked_id = ?
            """,
            (
                current_user_id,
                user_id,
            ),
        )

        is_blocked_by_me = (
            cursor.fetchone() is not None
        )

        cursor.execute(
            """
            SELECT id
            FROM user_block
            WHERE blocker_id = ?
              AND blocked_id = ?
            """,
            (
                user_id,
                current_user_id,
            ),
        )

        is_blocking_me = (
            cursor.fetchone() is not None
        )

        # 상대방이 나를 차단한 경우 프로필 비공개
        if is_blocking_me:
            abort(404)

        if not is_blocked_by_me:
            is_friend = (
                get_friendship(
                    cursor,
                    current_user_id,
                    user_id,
                )
                is not None
            )

            cursor.execute(
                """
                SELECT id, status
                FROM friend_request
                WHERE sender_id = ?
                  AND receiver_id = ?
                """,
                (
                    current_user_id,
                    user_id,
                ),
            )

            outgoing_request = cursor.fetchone()

            cursor.execute(
                """
                SELECT id, status
                FROM friend_request
                WHERE sender_id = ?
                  AND receiver_id = ?
                """,
                (
                    user_id,
                    current_user_id,
                ),
            )

            incoming_request = cursor.fetchone()

        cursor.execute(
            """
            SELECT id, status
            FROM friend_request
            WHERE sender_id = ? AND receiver_id = ?
            """,
            (session["user_id"], user_id),
        )
        outgoing_request = cursor.fetchone()

        cursor.execute(
            """
            SELECT id, status
            FROM friend_request
            WHERE sender_id = ? AND receiver_id = ?
            """,
            (user_id, session["user_id"]),
        )
        incoming_request = cursor.fetchone()

    return render_template(
        "public_profile.html",
        profile_user=profile_user,
        products=products,
        is_self=is_self,
        is_friend=is_friend,
        outgoing_request=outgoing_request,
        incoming_request=incoming_request,
        is_blocked_by_me=is_blocked_by_me,

    )


# 친구 목록과 요청 관리
@app.route("/friends")
def friends():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    current_user_id = session["user_id"]
    query = request.args.get("q", "").strip()[:50]

    cursor.execute(
        """
        SELECT
            friendship.id AS friendship_id,
            CASE
                WHEN friendship.user1_id = ? THEN user2.id
                ELSE user1.id
            END AS friend_id,
            CASE
                WHEN friendship.user1_id = ? THEN COALESCE(user2.nickname, user2.username)
                ELSE COALESCE(user1.nickname, user1.username)
            END AS friend_name,
            CASE
                WHEN friendship.user1_id = ? THEN user2.username
                ELSE user1.username
            END AS friend_username,
            CASE
                WHEN friendship.user1_id = ? THEN user2.bio
                ELSE user1.bio
            END AS friend_bio,
            friendship.created_at
        FROM friendship
        JOIN user AS user1 ON friendship.user1_id = user1.id
        JOIN user AS user2 ON friendship.user2_id = user2.id
        WHERE friendship.user1_id = ? OR friendship.user2_id = ?
        ORDER BY friend_name
        """,
        (
            current_user_id,
            current_user_id,
            current_user_id,
            current_user_id,
            current_user_id,
            current_user_id,
        ),
    )
    friend_list = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            request.id,
            request.created_at,
            sender.id AS sender_id,
            sender.username,
            COALESCE(sender.nickname, sender.username) AS sender_name,
            sender.bio
        FROM friend_request AS request
        JOIN user AS sender ON request.sender_id = sender.id
        WHERE request.receiver_id = ?
          AND request.status = 'pending'
        ORDER BY request.created_at DESC
        """,
        (current_user_id,),
    )
    incoming_requests = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            request.id,
            request.created_at,
            receiver.id AS receiver_id,
            receiver.username,
            COALESCE(receiver.nickname, receiver.username) AS receiver_name
        FROM friend_request AS request
        JOIN user AS receiver ON request.receiver_id = receiver.id
        WHERE request.sender_id = ?
          AND request.status = 'pending'
        ORDER BY request.created_at DESC
        """,
        (current_user_id,),
    )
    outgoing_requests = cursor.fetchall()

    search_results = []
    if query:
        escaped = query.replace("!", "!!").replace("%", "!%").replace("_", "!_")
        pattern = f"%{escaped}%"
        cursor.execute(
            """
            SELECT id, username, nickname, bio
            FROM user
            WHERE id != ?
              AND is_suspended = 0
              AND (
                    username LIKE ? ESCAPE '!'
                    OR nickname LIKE ? ESCAPE '!'
              )
            ORDER BY nickname, username
            LIMIT 30
            """,
            (current_user_id, pattern, pattern),
        )
        search_results = cursor.fetchall()

    return render_template(
        "friends.html",
        friends=friend_list,
        incoming_requests=incoming_requests,
        outgoing_requests=outgoing_requests,
        search_results=search_results,
        query=query,
    )


@app.route("/friends/request/<user_id>", methods=["POST"])
def send_friend_request(user_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    current_user_id = session["user_id"]
    if user_id == current_user_id:
        flash("자기 자신에게 친구 신청을 보낼 수 없습니다.")
        return redirect(url_for("public_profile", user_id=user_id))
    if users_have_block_relation(
        cursor,
        current_user_id,
        user_id,
    ):
        flash("차단 관계인 사용자에게는 친구 신청을 보낼 수 없습니다.")

        return redirect(
            url_for(
                "public_profile",
                user_id=user_id,
            )
        )

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id FROM user WHERE id = ? AND is_suspended = 0",
        (user_id,),
    )
    if cursor.fetchone() is None:
        abort(404)

    if get_friendship(cursor, current_user_id, user_id):
        flash("이미 친구인 사용자입니다.")
        return redirect(url_for("public_profile", user_id=user_id))

    cursor.execute(
        """
        SELECT id
        FROM friend_request
        WHERE sender_id = ?
          AND receiver_id = ?
          AND status = 'pending'
        """,
        (user_id, current_user_id),
    )
    reverse_request = cursor.fetchone()
    if reverse_request:
        flash("상대방이 이미 친구 신청을 보냈습니다. 친구 페이지에서 수락해주세요.")
        return redirect(url_for("friends"))

    cursor.execute(
        """
        SELECT id, status
        FROM friend_request
        WHERE sender_id = ? AND receiver_id = ?
        """,
        (current_user_id, user_id),
    )
    existing_request = cursor.fetchone()

    if existing_request and existing_request["status"] == "pending":
        flash("이미 친구 신청을 보냈습니다.")
    elif existing_request:
        cursor.execute(
            """
            UPDATE friend_request
            SET status = 'pending',
                created_at = CURRENT_TIMESTAMP,
                responded_at = NULL
            WHERE id = ?
            """,
            (existing_request["id"],),
        )
        db.commit()
        flash("친구 신청을 다시 보냈습니다.")
    else:
        cursor.execute(
            """
            INSERT INTO friend_request (
                id, sender_id, receiver_id, status
            ) VALUES (?, ?, ?, 'pending')
            """,
            (str(uuid.uuid4()), current_user_id, user_id),
        )
        db.commit()
        flash("친구 신청을 보냈습니다.")

    return redirect(url_for("public_profile", user_id=user_id))


@app.route("/friends/request/<request_id>/accept", methods=["POST"])
def accept_friend_request(request_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT id, sender_id, receiver_id, status
        FROM friend_request
        WHERE id = ? AND receiver_id = ?
        """,
        (request_id, session["user_id"]),
    )
    friend_request = cursor.fetchone()

    if friend_request is None or friend_request["status"] != "pending":
        flash("처리할 수 없는 친구 신청입니다.")
        return redirect(url_for("friends"))

    user1_id, user2_id = normalize_user_pair(
        friend_request["sender_id"],
        friend_request["receiver_id"],
    )

    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO friendship (
                id, user1_id, user2_id
            ) VALUES (?, ?, ?)
            """,
            (str(uuid.uuid4()), user1_id, user2_id),
        )
        cursor.execute(
            """
            UPDATE friend_request
            SET status = 'accepted', responded_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (request_id,),
        )
        cursor.execute(
            """
            UPDATE friend_request
            SET status = 'accepted', responded_at = CURRENT_TIMESTAMP
            WHERE sender_id = ? AND receiver_id = ? AND status = 'pending'
            """,
            (friend_request["receiver_id"], friend_request["sender_id"]),
        )
        db.commit()
    except sqlite3.Error:
        db.rollback()
        flash("친구 신청 수락 중 오류가 발생했습니다.")
        return redirect(url_for("friends"))

    flash("친구 신청을 수락했습니다.")
    return redirect(url_for("friends"))


@app.route("/friends/request/<request_id>/reject", methods=["POST"])
def reject_friend_request(request_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE friend_request
        SET status = 'rejected', responded_at = CURRENT_TIMESTAMP
        WHERE id = ?
          AND receiver_id = ?
          AND status = 'pending'
        """,
        (request_id, session["user_id"]),
    )
    db.commit()

    if cursor.rowcount == 1:
        flash("친구 신청을 거절했습니다.")
    else:
        flash("처리할 수 없는 친구 신청입니다.")

    return redirect(url_for("friends"))


@app.route("/friends/<user_id>/remove", methods=["POST"])
def remove_friend(user_id):
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    user1_id, user2_id = normalize_user_pair(session["user_id"], user_id)
    cursor.execute(
        "DELETE FROM friendship WHERE user1_id = ? AND user2_id = ?",
        (user1_id, user2_id),
    )
    db.commit()

    if cursor.rowcount == 1:
        flash("친구 관계를 삭제했습니다.")
    else:
        flash("친구 관계를 찾을 수 없습니다.")

    return redirect(url_for("friends"))


# 1:1 채팅방 시작 또는 기존 채팅방 열기
@app.route("/chat/private/<user_id>", methods=["POST"])
def start_private_chat(user_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    current_user_id = session["user_id"]
    if user_id == current_user_id:
        flash("자기 자신과 채팅할 수 없습니다.")
        return redirect(url_for("chat"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT id, username, is_suspended FROM user WHERE id = ?",
        (user_id,),
    )
    target_user = cursor.fetchone()
    if target_user is None or target_user["is_suspended"] == 1:
        abort(404)

    if users_have_block_relation(cursor, current_user_id, user_id):
        flash("차단 관계인 사용자와는 채팅할 수 없습니다.")
        return redirect(url_for("chat"))

    product_id = request.form.get("product_id", "").strip() or None
    if product_id:
        cursor.execute(
            """
            SELECT id, seller_id, status, is_hidden
            FROM product
            WHERE id = ?
            """,
            (product_id,),
        )
        product = cursor.fetchone()
        if (
            product is None
            or product["seller_id"] != user_id
            or product["is_hidden"] == 1
        ):
            flash("해당 상품의 판매자와 채팅을 시작할 수 없습니다.")
            return redirect(url_for("dashboard"))

    room = get_private_room_for_pair(cursor, current_user_id, user_id)
    if room is None:
        user1_id, user2_id = normalize_user_pair(current_user_id, user_id)
        room_id = str(uuid.uuid4())
        cursor.execute(
            """
            INSERT INTO private_chat_room (
                id, user1_id, user2_id, product_id
            ) VALUES (?, ?, ?, ?)
            """,
            (room_id, user1_id, user2_id, product_id),
        )
        db.commit()
    else:
        room_id = room["id"]
        if product_id and room["product_id"] != product_id:
            cursor.execute(
                "UPDATE private_chat_room SET product_id = ? WHERE id = ?",
                (product_id, room_id),
            )
            db.commit()

    return redirect(url_for("private_chat_room", room_id=room_id))


@app.route("/chat/room/<room_id>")
def private_chat_room(room_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    current_user_id = session["user_id"]
    db = get_db()
    cursor = db.cursor()

    room = user_can_access_private_room(cursor, room_id, current_user_id)
    if room is None:
        abort(403)

    other_user_id = (
        room["user2_id"]
        if room["user1_id"] == current_user_id
        else room["user1_id"]
    )

    if users_have_block_relation(cursor, current_user_id, other_user_id):
        flash("차단 관계인 사용자와는 채팅할 수 없습니다.")
        return redirect(url_for("chat"))

    cursor.execute(
        """
        SELECT id, username, nickname, bio, is_suspended
        FROM user
        WHERE id = ?
        """,
        (other_user_id,),
    )
    other_user = cursor.fetchone()
    if other_user is None or other_user["is_suspended"] == 1:
        flash("현재 대화할 수 없는 사용자입니다.")
        return redirect(url_for("chat"))

    product = None
    if room["product_id"]:
        cursor.execute(
            """
            SELECT id, title, price, status
            FROM product
            WHERE id = ? AND is_hidden = 0
            """,
            (room["product_id"],),
        )
        product = cursor.fetchone()

    cursor.execute(
        """
        UPDATE private_chat_message
        SET read_at = CURRENT_TIMESTAMP
        WHERE room_id = ?
          AND sender_id != ?
          AND read_at IS NULL
        """,
        (room_id, current_user_id),
    )
    db.commit()

    cursor.execute(
        """
        SELECT
            message.id,
            message.sender_id,
            message.message,
            message.created_at,
            message.read_at,
            COALESCE(sender.nickname, sender.username) AS sender_name
        FROM private_chat_message AS message
        JOIN user AS sender ON message.sender_id = sender.id
        WHERE message.room_id = ?
        ORDER BY message.created_at, message.rowid
        LIMIT 500
        """,
        (room_id,),
    )
    messages = cursor.fetchall()

    return render_template(
        "private_chat.html",
        room=room,
        other_user=other_user,
        product=product,
        messages=messages,
        current_user_id=current_user_id,
    )


@socketio.on("join_private_room")
def handle_join_private_room(data):
    user_id = session.get("user_id")

    if not user_id or not isinstance(data, dict):
        return

    room_id = str(
        data.get("room_id", "")
    ).strip()

    if not room_id:
        return

    db = get_db()
    cursor = db.cursor()

    room = user_can_access_private_room(
        cursor,
        room_id,
        user_id,
    )

    if room is None:
        emit(
            "private_message_error",
            {
                "code": "forbidden",
                "message": "접근할 수 없는 채팅방입니다.",
            },
        )
        return

    other_user_id = (
        room["user2_id"]
        if room["user1_id"] == user_id
        else room["user1_id"]
    )

    # 차단한 사람과 차단당한 사람 모두 채팅방 입장 차단
    if users_have_block_relation(
        cursor,
        user_id,
        other_user_id,
    ):
        emit(
            "private_message_error",
            {
                "code": "blocked",
                "message": (
                    "차단 관계인 사용자와는 "
                    "메시지를 주고받을 수 없습니다."
                ),
            },
        )
        return

    join_room(
        f"private:{room_id}"
    )

    cursor.execute(
        """
        UPDATE private_chat_message
        SET read_at = CURRENT_TIMESTAMP
        WHERE room_id = ?
          AND sender_id != ?
          AND read_at IS NULL
        """,
        (
            room_id,
            user_id,
        ),
    )

    db.commit()

    emit(
        "private_messages_read",
        {
            "room_id": room_id,
            "reader_id": user_id,
        },
        to=f"private:{room_id}",
    )

@socketio.on("send_private_message")
def handle_send_private_message(data):
    user_id = session.get("user_id")

    if not user_id or not isinstance(data, dict):
        return

    room_id = str(
        data.get("room_id", "")
    ).strip()

    message_text = str(
        data.get("message", "")
    ).strip()

    if (
        not room_id
        or not message_text
        or len(message_text) > 500
    ):
        emit(
            "private_message_error",
            {
                "code": "invalid_message",
                "message": (
                    "메시지는 1자 이상 "
                    "500자 이하로 입력해주세요."
                ),
            },
        )
        return

    if not realtime_action_allowed(
        ("private_chat", user_id),
        limit=1,
        window_seconds=PRIVATE_CHAT_WINDOW_SECONDS,
    ):
        emit(
            "private_message_error",
            {
                "code": "rate_limited",
                "message": "메시지는 1초에 한 번만 보낼 수 있습니다.",
            },
        )
        return

    db = get_db()
    cursor = db.cursor()

    room = user_can_access_private_room(
        cursor,
        room_id,
        user_id,
    )

    if room is None:
        emit(
            "private_message_error",
            {
                "code": "forbidden",
                "message": "접근할 수 없는 채팅방입니다.",
            },
        )
        return

    other_user_id = (
        room["user2_id"]
        if room["user1_id"] == user_id
        else room["user1_id"]
    )

    # 가장 중요한 서버 측 차단 검사
    if users_have_block_relation(
        cursor,
        user_id,
        other_user_id,
    ):
        emit(
            "private_message_error",
            {
                "code": "blocked",
                "message": (
                    "차단 관계인 사용자와는 "
                    "메시지를 주고받을 수 없습니다."
                ),
            },
        )
        return

    cursor.execute(
        """
        SELECT
            username,
            COALESCE(nickname, username)
                AS sender_name
        FROM user
        WHERE id = ?
          AND is_suspended = 0
        """,
        (user_id,),
    )

    sender = cursor.fetchone()

    if sender is None:
        emit(
            "private_message_error",
            {
                "code": "invalid_user",
                "message": "사용자 정보를 확인할 수 없습니다.",
            },
        )
        return

    message_id = str(
        uuid.uuid4()
    )

    created_at = (
        datetime.now()
        .astimezone()
        .isoformat(timespec="seconds")
    )

    try:
        cursor.execute(
            """
            INSERT INTO private_chat_message (
                id,
                room_id,
                sender_id,
                message,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message_id,
                room_id,
                user_id,
                message_text,
                created_at,
            ),
        )

        db.commit()

    except sqlite3.Error:
        db.rollback()

        emit(
            "private_message_error",
            {
                "code": "database_error",
                "message": "메시지 저장 중 오류가 발생했습니다.",
            },
        )
        return

    emit(
        "private_message",
        {
            "message_id": message_id,
            "room_id": room_id,
            "sender_id": user_id,
            "sender_name": sender["sender_name"],
            "message": message_text,
            "created_at": created_at,
            "read_at": None,
        },
        to=f"private:{room_id}",
    )


# 관리자 페이지
@app.route("/admin")
def admin():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, is_admin
        FROM user
        WHERE id = ?
        """,
        (session["user_id"],),
    )
    current_user = cursor.fetchone()

    if current_user is None:
        session.clear()
        flash("사용자 정보를 찾을 수 없습니다.")
        return redirect(url_for("login"))

    if current_user["is_admin"] != 1:
        abort(403)

    cursor.execute(
        """
        SELECT
            id,
            username,
            nickname,
            bio,
            report_count,
            is_suspended,
            is_admin
        FROM user
        ORDER BY username
        """
    )
    users = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            product.id,
            product.title,
            product.price,
            product.report_count,
            product.is_hidden,
            user.username AS seller_username
        FROM product
        LEFT JOIN user
            ON product.seller_id = user.id
        ORDER BY product.rowid DESC
        """
    )
    products = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) AS count FROM report")
    report_total = cursor.fetchone()["count"]

    cursor.execute(
        "SELECT COUNT(*) AS count FROM report WHERE status = 'pending'"
    )
    pending_report_total = cursor.fetchone()["count"]

    cursor.execute(
        """
        SELECT
            COUNT(*) AS transfer_count,
            COALESCE(SUM(amount), 0) AS transfer_total
        FROM transfer
        """
    )
    transfer_stats = cursor.fetchone()

    cursor.execute(
        """
        SELECT COUNT(*) AS count
        FROM admin_action_log
        """
    )
    admin_log_total = cursor.fetchone()["count"]

    cursor.execute(
        """
        SELECT
            action_type,
            target_label,
            admin_username,
            created_at
        FROM admin_action_log
        ORDER BY created_at DESC, rowid DESC
        LIMIT 5
        """
    )
    recent_admin_logs = cursor.fetchall()

    return render_template(
        "admin.html",
        users=users,
        products=products,
        report_total=report_total,
        pending_report_total=pending_report_total,
        transfer_count=transfer_stats["transfer_count"],
        transfer_total=transfer_stats["transfer_total"],
        admin_log_total=admin_log_total,
        recent_admin_logs=recent_admin_logs,
    )


# 관리자: 신고 검토 목록 — 동일 대상별 그룹화
@app.route("/admin/reports")
def admin_reports():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, username, is_admin FROM user WHERE id = ?",
        (session["user_id"],),
    )
    current_admin = cursor.fetchone()
    if current_admin is None or current_admin["is_admin"] != 1:
        abort(403)

    status_filter = request.args.get("status", "pending").strip().lower()
    type_filter = request.args.get("type", "all").strip().lower()
    if status_filter not in {"all", "pending", "approved", "rejected"}:
        status_filter = "pending"
    if type_filter not in {"all", "user", "product"}:
        type_filter = "all"

    type_condition = ""
    params = []
    if type_filter != "all":
        type_condition = "WHERE report.target_type = ?"
        params.append(type_filter)

    # 개별 신고를 가져온 뒤 target_type + target_id 기준으로 묶는다.
    # 이렇게 하면 같은 상품/사용자에 접수된 신고 사유와 신고자를 한 카드에서 확인할 수 있다.
    cursor.execute(
        f"""
        SELECT
            report.id,
            report.target_id,
            report.target_type,
            report.reason,
            report.status,
            report.admin_note,
            report.created_at,
            report.reviewed_at,
            reporter.username AS reporter_username,
            reporter.nickname AS reporter_nickname,
            target_user.username AS target_username,
            target_user.nickname AS target_nickname,
            target_user.is_suspended AS target_is_suspended,
            target_product.title AS target_product_title,
            target_product.is_hidden AS target_product_hidden,
            seller.username AS seller_username,
            reviewer.username AS reviewer_username
        FROM report
        JOIN user AS reporter
            ON report.reporter_id = reporter.id
        LEFT JOIN user AS target_user
            ON report.target_type = 'user'
           AND report.target_id = target_user.id
        LEFT JOIN product AS target_product
            ON report.target_type = 'product'
           AND report.target_id = target_product.id
        LEFT JOIN user AS seller
            ON target_product.seller_id = seller.id
        LEFT JOIN user AS reviewer
            ON report.reviewed_by = reviewer.id
        {type_condition}
        ORDER BY report.created_at DESC, report.rowid DESC
        LIMIT 1000
        """,
        tuple(params),
    )
    report_rows = cursor.fetchall()

    grouped = {}
    for row in report_rows:
        key = (row["target_type"], row["target_id"])
        if key not in grouped:
            grouped[key] = {
                "target_id": row["target_id"],
                "target_type": row["target_type"],
                "target_username": row["target_username"],
                "target_nickname": row["target_nickname"],
                "target_is_suspended": row["target_is_suspended"],
                "target_product_title": row["target_product_title"],
                "target_product_hidden": row["target_product_hidden"],
                "seller_username": row["seller_username"],
                "latest_created_at": row["created_at"],
                "latest_reviewed_at": None,
                "latest_reviewer_username": None,
                "latest_admin_note": None,
                "reports": [],
                "total_count": 0,
                "pending_count": 0,
                "approved_count": 0,
                "rejected_count": 0,
            }

        group = grouped[key]
        report_item = {
            "id": row["id"],
            "reason": row["reason"],
            "status": row["status"],
            "created_at": row["created_at"],
            "reviewed_at": row["reviewed_at"],
            "admin_note": row["admin_note"],
            "reporter_username": row["reporter_username"],
            "reporter_nickname": row["reporter_nickname"],
            "reviewer_username": row["reviewer_username"],
        }
        group["reports"].append(report_item)
        group["total_count"] += 1
        status = row["status"]
        if status == "pending":
            group["pending_count"] += 1
        elif status == "approved":
            group["approved_count"] += 1
        else:
            group["rejected_count"] += 1

        if row["reviewed_at"] and group["latest_reviewed_at"] is None:
            group["latest_reviewed_at"] = row["reviewed_at"]
            group["latest_reviewer_username"] = row["reviewer_username"]
            group["latest_admin_note"] = row["admin_note"]

    all_groups = []
    for group in grouped.values():
        # 미처리 신고가 한 건이라도 있으면 그룹 전체를 검토 대기로 표시한다.
        # 모두 처리된 경우 승인 이력이 있으면 승인, 전부 기각이면 기각으로 표시한다.
        if group["pending_count"] > 0:
            group["group_status"] = "pending"
        elif group["approved_count"] > 0:
            group["group_status"] = "approved"
        else:
            group["group_status"] = "rejected"
        all_groups.append(group)

    group_stats = {
        "target_total": len(all_groups),
        "pending_target_count": sum(
            1 for group in all_groups if group["group_status"] == "pending"
        ),
        "approved_target_count": sum(
            1 for group in all_groups if group["group_status"] == "approved"
        ),
        "rejected_target_count": sum(
            1 for group in all_groups if group["group_status"] == "rejected"
        ),
    }

    if status_filter == "all":
        report_groups = all_groups
    else:
        report_groups = [
            group
            for group in all_groups
            if group["group_status"] == status_filter
        ]

    report_groups.sort(
        key=lambda group: (
            0 if group["group_status"] == "pending" else 1,
            group["latest_created_at"] or "",
        ),
        reverse=False,
    )
    # 같은 상태 안에서는 최신 신고 대상이 먼저 오도록 다시 정렬한다.
    pending_groups = sorted(
        [g for g in report_groups if g["group_status"] == "pending"],
        key=lambda g: g["latest_created_at"] or "",
        reverse=True,
    )
    processed_groups = sorted(
        [g for g in report_groups if g["group_status"] != "pending"],
        key=lambda g: g["latest_created_at"] or "",
        reverse=True,
    )
    report_groups = pending_groups + processed_groups

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
            SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_count,
            SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected_count
        FROM report
        """
    )
    stats = cursor.fetchone()

    return render_template(
        "admin_reports.html",
        report_groups=report_groups,
        status_filter=status_filter,
        type_filter=type_filter,
        total_count=stats["total_count"] or 0,
        pending_count=stats["pending_count"] or 0,
        approved_count=stats["approved_count"] or 0,
        rejected_count=stats["rejected_count"] or 0,
        **group_stats,
    )


# 관리자: 같은 대상의 미처리 신고를 한 번에 승인 또는 기각
@app.route(
    "/admin/report-group/<target_type>/<target_id>/review",
    methods=["POST"],
)
def admin_review_report_group(target_type, target_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    target_type = target_type.strip().lower()
    action = request.form.get("action", "").strip().lower()
    admin_note = request.form.get("admin_note", "").strip()

    if target_type not in {"user", "product"}:
        abort(404)
    if action not in {"approve", "reject"}:
        flash("올바르지 않은 신고 처리 요청입니다.")
        return redirect(url_for("admin_reports"))
    if len(admin_note) > 500:
        flash("관리자 처리 메모는 500자 이하로 입력해주세요.")
        return redirect(url_for("admin_reports"))
    if action == "reject" and len(admin_note) < 2:
        flash("신고 기각 시에는 처리 사유를 2자 이상 입력해주세요.")
        return redirect(url_for("admin_reports"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, username, is_admin FROM user WHERE id = ?",
        (session["user_id"],),
    )
    current_admin = cursor.fetchone()
    if current_admin is None or current_admin["is_admin"] != 1:
        abort(403)

    cursor.execute(
        """
        SELECT id, reason
        FROM report
        WHERE target_type = ?
          AND target_id = ?
          AND status = 'pending'
        ORDER BY created_at ASC, rowid ASC
        """,
        (target_type, target_id),
    )
    pending_reports = cursor.fetchall()
    if not pending_reports:
        flash("이 대상에 남아 있는 검토 대기 신고가 없습니다.")
        return redirect(url_for("admin_reports", status="all"))

    if target_type == "user":
        cursor.execute(
            """
            SELECT id, username, nickname, is_admin
            FROM user
            WHERE id = ?
            """,
            (target_id,),
        )
        target = cursor.fetchone()
        if target is None and action == "approve":
            flash("신고 대상 사용자가 삭제되어 승인할 수 없습니다.")
            return redirect(url_for("admin_reports"))
        if target is not None and target["is_admin"] == 1 and action == "approve":
            flash("관리자 계정 신고는 승인할 수 없습니다.")
            return redirect(url_for("admin_reports"))
        target_label = (
            (target["nickname"] or target["username"])
            if target is not None
            else "삭제된 사용자"
        )
    else:
        cursor.execute(
            "SELECT id, title FROM product WHERE id = ?",
            (target_id,),
        )
        target = cursor.fetchone()
        if target is None and action == "approve":
            flash("신고 대상 상품이 삭제되어 승인할 수 없습니다.")
            return redirect(url_for("admin_reports"))
        target_label = target["title"] if target is not None else "삭제된 상품"

    reviewed_at = datetime.now().astimezone().isoformat(timespec="seconds")
    new_status = "approved" if action == "approve" else "rejected"
    report_count = len(pending_reports)

    try:
        if action == "approve":
            if target_type == "user":
                cursor.execute(
                    "UPDATE user SET is_suspended = 1 WHERE id = ?",
                    (target_id,),
                )
                action_type = "REPORT_GROUP_APPROVE_USER"
                action_detail = "신고 묶음 승인 및 계정 정지"
                audit_target_type = "USER"
            else:
                cursor.execute(
                    "UPDATE product SET is_hidden = 1 WHERE id = ?",
                    (target_id,),
                )
                action_type = "REPORT_GROUP_APPROVE_PRODUCT"
                action_detail = "신고 묶음 승인 및 상품 숨김"
                audit_target_type = "PRODUCT"
        else:
            action_type = "REPORT_GROUP_REJECT"
            action_detail = "신고 묶음 기각"
            audit_target_type = target_type.upper()

        cursor.execute(
            """
            UPDATE report
            SET status = ?,
                admin_note = ?,
                reviewed_by = ?,
                reviewed_at = ?
            WHERE target_type = ?
              AND target_id = ?
              AND status = 'pending'
            """,
            (
                new_status,
                admin_note or None,
                current_admin["id"],
                reviewed_at,
                target_type,
                target_id,
            ),
        )
        if cursor.rowcount != report_count:
            raise sqlite3.IntegrityError("report group changed during review")

        reason_preview = " / ".join(
            report_row["reason"].replace("\n", " ")[:80]
            for report_row in pending_reports[:3]
        )
        if report_count > 3:
            reason_preview += f" 외 {report_count - 3}건"

        record_admin_action(
            cursor=cursor,
            admin_id=current_admin["id"],
            admin_username=current_admin["username"],
            action_type=action_type,
            target_type=audit_target_type,
            target_id=target_id,
            target_label=target_label,
            details=(
                f"{action_detail} · 일괄 처리 {report_count}건"
                + (f" · 신고 사유: {reason_preview}" if reason_preview else "")
                + (f" · 관리자 메모: {admin_note}" if admin_note else "")
            ),
        )
        db.commit()
    except sqlite3.Error:
        db.rollback()
        flash("신고 묶음 처리 중 오류가 발생했습니다.")
        return redirect(url_for("admin_reports"))

    if action == "approve":
        flash(f"신고 {report_count}건을 함께 승인하고 운영 조치를 적용했습니다.")
    else:
        flash(f"신고 {report_count}건을 함께 기각했습니다.")
    return redirect(url_for("admin_reports"))


# 관리자: 신고 승인 또는 기각
@app.route("/admin/report/<report_id>/review", methods=["POST"])
def admin_review_report(report_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    action = request.form.get("action", "").strip().lower()
    admin_note = request.form.get("admin_note", "").strip()
    if action not in {"approve", "reject"}:
        flash("올바르지 않은 신고 처리 요청입니다.")
        return redirect(url_for("admin_reports"))
    if len(admin_note) > 500:
        flash("관리자 처리 메모는 500자 이하로 입력해주세요.")
        return redirect(url_for("admin_reports"))
    if action == "reject" and len(admin_note) < 2:
        flash("신고 기각 시에는 처리 사유를 2자 이상 입력해주세요.")
        return redirect(url_for("admin_reports"))

    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, username, is_admin FROM user WHERE id = ?",
        (session["user_id"],),
    )
    current_admin = cursor.fetchone()
    if current_admin is None or current_admin["is_admin"] != 1:
        abort(403)

    cursor.execute(
        """
        SELECT
            report.id,
            report.target_id,
            report.target_type,
            report.reason,
            report.status,
            target_user.username AS target_username,
            target_user.nickname AS target_nickname,
            target_user.is_admin AS target_is_admin,
            target_product.title AS target_product_title,
            seller.username AS seller_username
        FROM report
        LEFT JOIN user AS target_user
            ON report.target_type = 'user'
           AND report.target_id = target_user.id
        LEFT JOIN product AS target_product
            ON report.target_type = 'product'
           AND report.target_id = target_product.id
        LEFT JOIN user AS seller
            ON target_product.seller_id = seller.id
        WHERE report.id = ?
        """,
        (report_id,),
    )
    report_row = cursor.fetchone()
    if report_row is None:
        flash("신고 내역을 찾을 수 없습니다.")
        return redirect(url_for("admin_reports"))
    if report_row["status"] != "pending":
        flash("이미 처리된 신고입니다.")
        return redirect(url_for("admin_reports", status="all"))

    target_type = report_row["target_type"]
    if target_type == "user":
        target_label = (
            report_row["target_nickname"]
            or report_row["target_username"]
            or "삭제된 사용자"
        )
        if action == "approve" and not report_row["target_username"]:
            flash("신고 대상 사용자가 삭제되어 승인할 수 없습니다.")
            return redirect(url_for("admin_reports"))
        if action == "approve" and report_row["target_is_admin"] == 1:
            flash("관리자 계정 신고는 승인할 수 없습니다.")
            return redirect(url_for("admin_reports"))
    else:
        target_label = report_row["target_product_title"] or "삭제된 상품"
        if action == "approve" and not report_row["target_product_title"]:
            flash("신고 대상 상품이 삭제되어 승인할 수 없습니다.")
            return redirect(url_for("admin_reports"))

    reviewed_at = datetime.now().astimezone().isoformat(timespec="seconds")
    new_status = "approved" if action == "approve" else "rejected"

    try:
        if action == "approve":
            if target_type == "user":
                cursor.execute(
                    "UPDATE user SET is_suspended = 1 WHERE id = ?",
                    (report_row["target_id"],),
                )
                action_type = "REPORT_APPROVE_USER"
                action_detail = "신고 승인 및 계정 정지"
                audit_target_type = "USER"
            else:
                cursor.execute(
                    "UPDATE product SET is_hidden = 1 WHERE id = ?",
                    (report_row["target_id"],),
                )
                action_type = "REPORT_APPROVE_PRODUCT"
                action_detail = "신고 승인 및 상품 숨김"
                audit_target_type = "PRODUCT"
        else:
            action_type = "REPORT_REJECT"
            action_detail = "신고 기각"
            audit_target_type = target_type.upper()

        cursor.execute(
            """
            UPDATE report
            SET status = ?,
                admin_note = ?,
                reviewed_by = ?,
                reviewed_at = ?
            WHERE id = ? AND status = 'pending'
            """,
            (
                new_status,
                admin_note or None,
                current_admin["id"],
                reviewed_at,
                report_id,
            ),
        )
        if cursor.rowcount != 1:
            raise sqlite3.IntegrityError("report already reviewed")

        record_admin_action(
            cursor=cursor,
            admin_id=current_admin["id"],
            admin_username=current_admin["username"],
            action_type=action_type,
            target_type=audit_target_type,
            target_id=report_row["target_id"],
            target_label=target_label,
            details=(
                f"{action_detail} · 신고 사유: {report_row['reason']}"
                + (f" · 관리자 메모: {admin_note}" if admin_note else "")
            ),
        )
        db.commit()
    except sqlite3.Error:
        db.rollback()
        flash("신고 처리 중 오류가 발생했습니다.")
        return redirect(url_for("admin_reports"))

    if action == "approve":
        flash("신고를 승인하고 대상에 운영 조치를 적용했습니다.")
    else:
        flash("신고를 기각했습니다.")
    return redirect(url_for("admin_reports"))


# 관리자: 감사 로그 전체 조회
@app.route("/admin/logs")
def admin_logs():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, is_admin
        FROM user
        WHERE id = ?
        """,
        (session["user_id"],),
    )
    current_admin = cursor.fetchone()

    if current_admin is None or current_admin["is_admin"] != 1:
        abort(403)

    cursor.execute(
        """
        SELECT
            id,
            admin_username,
            action_type,
            target_type,
            target_label,
            details,
            created_at
        FROM admin_action_log
        ORDER BY created_at DESC, rowid DESC
        LIMIT 200
        """
    )
    logs = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_count,
            COUNT(DISTINCT admin_id) AS admin_count,
            SUM(
                CASE
                    WHEN date(created_at, 'localtime') = date('now', 'localtime')
                    THEN 1 ELSE 0
                END
            ) AS today_count
        FROM admin_action_log
        """
    )
    stats = cursor.fetchone()

    return render_template(
        "admin_logs.html",
        logs=logs,
        total_count=stats["total_count"],
        admin_count=stats["admin_count"],
        today_count=stats["today_count"] or 0,
    )


# 관리자: 전체 송금 거래 내역 조회
@app.route("/admin/transfers")
def admin_transfers():
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, is_admin
        FROM user
        WHERE id = ?
        """,
        (session["user_id"],),
    )
    current_admin = cursor.fetchone()

    if current_admin is None or current_admin["is_admin"] != 1:
        abort(403)

    cursor.execute(
        """
        SELECT
            transfer.id,
            transfer.amount,
            transfer.created_at,
            sender.id AS sender_id,
            sender.username AS sender_username,
            sender.nickname AS sender_nickname,
            receiver.id AS receiver_id,
            receiver.username AS receiver_username,
            receiver.nickname AS receiver_nickname
        FROM transfer
        JOIN user AS sender
            ON transfer.sender_id = sender.id
        JOIN user AS receiver
            ON transfer.receiver_id = receiver.id
        ORDER BY transfer.created_at DESC, transfer.rowid DESC
        LIMIT 200
        """
    )
    transfers = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_count,
            COALESCE(SUM(amount), 0) AS total_amount,
            COALESCE(MAX(amount), 0) AS max_amount,
            COALESCE(SUM(
                CASE
                    WHEN date(created_at, 'localtime') = date('now', 'localtime')
                    THEN amount ELSE 0
                END
            ), 0) AS today_amount
        FROM transfer
        """
    )
    stats = cursor.fetchone()

    return render_template(
        "admin_transfers.html",
        transfers=transfers,
        total_count=stats["total_count"],
        total_amount=stats["total_amount"],
        max_amount=stats["max_amount"],
        today_amount=stats["today_amount"],
    )

# 관리자: 사용자 정지 또는 정지 해제
@app.route("/admin/user/<user_id>/toggle-suspension", methods=["POST"])
def admin_toggle_user_suspension(user_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, is_admin
        FROM user
        WHERE id = ?
        """,
        (session["user_id"],),
    )
    current_admin = cursor.fetchone()

    if current_admin is None or current_admin["is_admin"] != 1:
        abort(403)

    if user_id == session["user_id"]:
        flash("현재 로그인한 관리자 계정은 정지할 수 없습니다.")
        return redirect(url_for("admin"))

    cursor.execute(
        """
        SELECT id, username, nickname, is_admin, is_suspended, report_count
        FROM user
        WHERE id = ?
        """,
        (user_id,),
    )
    target_user = cursor.fetchone()

    if target_user is None:
        flash("사용자를 찾을 수 없습니다.")
        return redirect(url_for("admin"))

    if target_user["is_admin"] == 1:
        flash("관리자 계정은 이 화면에서 정지할 수 없습니다.")
        return redirect(url_for("admin"))

    new_status = 0 if target_user["is_suspended"] == 1 else 1
    action_type = "USER_SUSPEND" if new_status == 1 else "USER_RESTORE"
    target_label = target_user["nickname"] or target_user["username"]

    try:
        cursor.execute(
            """
            UPDATE user
            SET is_suspended = ?
            WHERE id = ?
            """,
            (new_status, user_id),
        )

        record_admin_action(
            cursor=cursor,
            admin_id=current_admin["id"],
            admin_username=current_admin["username"],
            action_type=action_type,
            target_type="USER",
            target_id=user_id,
            target_label=f"{target_label} (@{target_user['username']})",
            details=f"누적 신고 {target_user['report_count']}건",
        )
        db.commit()
    except sqlite3.Error:
        db.rollback()
        flash("사용자 상태 변경 중 오류가 발생했습니다.")
        return redirect(url_for("admin"))

    if new_status == 1:
        flash(f"{target_user['username']} 계정을 정지했습니다.")
    else:
        flash(f"{target_user['username']} 계정의 정지를 해제했습니다.")

    return redirect(url_for("admin"))

# 관리자: 상품 숨김 또는 복구
@app.route(
    "/admin/product/<product_id>/toggle-hidden",
    methods=["POST"],
)
def admin_toggle_product_hidden(product_id):
    if "user_id" not in session:
        flash("로그인이 필요합니다.")
        return redirect(url_for("login"))

    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT id, username, is_admin
        FROM user
        WHERE id = ?
        """,
        (session["user_id"],),
    )
    current_admin = cursor.fetchone()

    if current_admin is None or current_admin["is_admin"] != 1:
        abort(403)

    cursor.execute(
        """
        SELECT
            product.id,
            product.title,
            product.is_hidden,
            seller.username AS seller_username
        FROM product
        LEFT JOIN user AS seller
            ON product.seller_id = seller.id
        WHERE product.id = ?
        """,
        (product_id,),
    )
    target_product = cursor.fetchone()

    if target_product is None:
        flash("상품을 찾을 수 없습니다.")
        return redirect(url_for("admin"))

    new_status = 0 if target_product["is_hidden"] == 1 else 1
    action_type = "PRODUCT_HIDE" if new_status == 1 else "PRODUCT_RESTORE"

    try:
        cursor.execute(
            """
            UPDATE product
            SET is_hidden = ?
            WHERE id = ?
            """,
            (new_status, product_id),
        )

        record_admin_action(
            cursor=cursor,
            admin_id=current_admin["id"],
            admin_username=current_admin["username"],
            action_type=action_type,
            target_type="PRODUCT",
            target_id=product_id,
            target_label=target_product["title"],
            details=f"판매자 @{target_product['seller_username'] or '알 수 없음'}",
        )
        db.commit()
    except sqlite3.Error:
        db.rollback()
        flash("상품 노출 상태 변경 중 오류가 발생했습니다.")
        return redirect(url_for("admin"))

    if new_status == 1:
        flash(f"{target_product['title']} 상품을 숨겼습니다.")
    else:
        flash(f"{target_product['title']} 상품을 다시 공개했습니다.")

    return redirect(url_for("admin"))

if __name__ == "__main__":
    init_db()
    socketio.run(
        app,
        host="127.0.0.1",
        port=5000,
        debug=False,
        allow_unsafe_werkzeug=True,
    )