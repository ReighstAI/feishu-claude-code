#!/usr/bin/env python3
"""Stop hook: 每次对话结束后自动记录时间线条目到当天的记忆文件。"""
import fcntl
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
WORKSPACE = SCRIPT_DIR.parent.parent
MEMORY_DIR = WORKSPACE / "memory"
LOCK_FILE = MEMORY_DIR / ".daily-memory.lock"
MAX_TIMELINE = 60


def today_str():
    now = datetime.now()
    if now.hour < 5:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return now.strftime("%Y-%m-%d")


def extract_text(content):
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for b in content:
            if isinstance(b, dict) and b.get("type") == "text":
                t = b.get("text", "").strip()
                if t:
                    parts.append(t)
        return " ".join(parts)
    return ""


def find_last_user(transcript_path):
    try:
        lines = Path(transcript_path).read_text().strip().split("\n")
        for line in reversed(lines):
            try:
                e = json.loads(line)
                if e.get("type") != "user" or e.get("userType") != "external":
                    continue
                text = extract_text(e.get("message", {}).get("content", ""))
                if not text or text.startswith("/") or text.startswith("<"):
                    continue
                return text
            except (json.JSONDecodeError, KeyError):
                continue
    except Exception:
        pass
    return None


def find_last_assistant(transcript_path):
    try:
        lines = Path(transcript_path).read_text().strip().split("\n")
        for line in reversed(lines):
            try:
                e = json.loads(line)
                if e.get("type") != "assistant":
                    continue
                text = extract_text(e.get("message", {}).get("content", ""))
                if not text:
                    continue
                for tl in text.split("\n"):
                    s = tl.strip()
                    if len(s) >= 10:
                        clean = re.sub(r"\s+", " ", s)
                        return clean[:80] if len(clean) > 80 else clean
            except (json.JSONDecodeError, KeyError):
                continue
    except Exception:
        pass
    return None


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        print("{}")
        return

    if data.get("stop_hook_active"):
        print("{}")
        return

    tp = data.get("transcript_path", "")
    if not tp or not Path(tp).exists():
        print("{}")
        return

    user_msg = find_last_user(tp)
    if not user_msg:
        print("{}")
        return

    date = today_str()
    ts = datetime.now().strftime("%H:%M")

    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    mem_file = MEMORY_DIR / f"{date}.md"
    if not mem_file.exists():
        mem_file.write_text(f"# {date}\n\n## Timeline\n\n## Memory\n")

    user_text = re.sub(r"\s+", " ", user_msg).strip()
    if len(user_text) > 80:
        user_text = user_text[:77] + "..."
    user_line = f"{ts} -- {user_text}"

    asst_line = find_last_assistant(tp)

    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        content = mem_file.read_text()

        existing = [l for l in content.split("\n") if re.match(r"\d{2}:\d{2} -- ", l)]
        if len(existing) >= MAX_TIMELINE:
            print("{}")
            return

        if existing:
            last = existing[-1]
            if last[8:] == user_line[8:]:
                print("{}")
                return

        entry = user_line + "\n"
        if asst_line:
            entry += f"    >> {asst_line}\n"

        tl_pos = content.find("## Timeline\n")
        if tl_pos >= 0:
            mem_pos = content.find("\n## Memory", tl_pos)
            if mem_pos >= 0:
                before = content[:mem_pos]
                after = content[mem_pos:]
                content = before.rstrip("\n") + "\n" + entry + after
            else:
                content = content.rstrip("\n") + "\n" + entry
        else:
            content = content.rstrip("\n") + "\n## Timeline\n" + entry

        mem_file.write_text(content)
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()

    print("{}")


if __name__ == "__main__":
    main()
