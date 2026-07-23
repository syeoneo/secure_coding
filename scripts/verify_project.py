"""제출 전 Python/Jinja 구문과 필수 파일을 점검한다."""

from __future__ import annotations

import py_compile
import sys
from pathlib import Path

from jinja2 import Environment, TemplateSyntaxError

ROOT = Path(__file__).resolve().parent.parent
REQUIRED_FILES = [
    ROOT / "app.py",
    ROOT / "requirements.txt",
    ROOT / ".env.example",
    ROOT / "README.md",
    ROOT / "templates" / "base.html",
    ROOT / "templates" / "dashboard.html",
    ROOT / "templates" / "admin_reports.html",
    ROOT / "static" / "style.css",
]


def main() -> int:
    errors: list[str] = []

    for path in REQUIRED_FILES:
        if not path.exists():
            errors.append(f"필수 파일 없음: {path.relative_to(ROOT)}")

    for path in [ROOT / "app.py", ROOT / "scripts" / "make_admin.py"]:
        try:
            py_compile.compile(str(path), doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f"Python 문법 오류: {path.name}: {exc.msg}")

    environment = Environment(autoescape=True)
    for template in sorted((ROOT / "templates").glob("*.html")):
        try:
            environment.parse(template.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, TemplateSyntaxError) as exc:
            errors.append(f"템플릿 오류: {template.name}: {exc}")

    if errors:
        print("[FAIL] 프로젝트 검사 실패")
        for error in errors:
            print(f"- {error}")
        return 1

    print("[OK] Python 문법, Jinja 템플릿, 필수 파일 검사를 통과했습니다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
