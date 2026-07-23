"""기존 회원 계정의 관리자 권한을 설정하거나 해제한다."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATABASE = BASE_DIR / "market.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="햇켓 관리자 권한 설정")
    parser.add_argument("username", help="관리자 권한을 변경할 사용자명")
    parser.add_argument(
        "--remove",
        action="store_true",
        help="관리자 권한을 부여하지 않고 해제합니다.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not DATABASE.exists():
        print("market.db가 없습니다. 앱을 한 번 실행하고 회원가입을 먼저 진행하세요.")
        return 1

    with sqlite3.connect(DATABASE) as connection:
        cursor = connection.execute(
            "SELECT id, username, is_admin FROM user WHERE username = ?",
            (args.username,),
        )
        user = cursor.fetchone()

        if user is None:
            print(f"사용자 '{args.username}'을 찾을 수 없습니다.")
            return 1

        new_value = 0 if args.remove else 1
        connection.execute(
            "UPDATE user SET is_admin = ? WHERE username = ?",
            (new_value, args.username),
        )
        connection.commit()

    action = "해제" if args.remove else "부여"
    print(f"'{args.username}' 계정의 관리자 권한을 {action}했습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
