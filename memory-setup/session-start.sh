#!/bin/bash
# SessionStart hook: instruct model to Read user.md + daily memory files.
#
# Path 2 approach: instead of injecting full content (which exceeds 10K char limit
# and gets persisted to disk, requiring unreliable model Read), output a short
# directive with exact file paths. Short output goes directly into context.
#
# Resume guard: skip on resumed sessions (Bridge follow-up messages).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/../.." && pwd)"
MEMORY_DIR="$WORKSPACE/memory"
USER_MD="$HOME/.claude/user.md"

# --- Resume guard ---
HOOK_INPUT=$(cat /dev/stdin 2>/dev/null || echo '{}')
SOURCE=$(echo "$HOOK_INPUT" | python3 -c "import json,sys; print(json.load(sys.stdin).get('source',''))" 2>/dev/null)
if [ "$SOURCE" = "resume" ]; then
    echo '{}'
    exit 0
fi

# --- Compute today/yesterday with 5AM boundary + current datetime ---
read TODAY YESTERDAY NOWSTR < <(python3 -c "
from datetime import datetime, timedelta
now = datetime.now()
days_cn = ['周一','周二','周三','周四','周五','周六','周日']
dow = days_cn[now.weekday()]
nowstr = now.strftime('%Y-%m-%d %H:%M') + ' ' + dow
if now.hour < 5:
    t = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    y = (now - timedelta(days=2)).strftime('%Y-%m-%d')
else:
    t = now.strftime('%Y-%m-%d')
    y = (now - timedelta(days=1)).strftime('%Y-%m-%d')
print(t, y, nowstr)
")

# --- Build file list ---
FILES_TO_READ=""

if [ -f "$USER_MD" ]; then
    FILES_TO_READ="1. $USER_MD"
else
    FILES_TO_READ="1. [MISSING] $USER_MD — user profile not found"
fi

TODAY_FILE="$MEMORY_DIR/${TODAY}.md"
if [ -f "$TODAY_FILE" ]; then
    FILES_TO_READ="$FILES_TO_READ
2. $TODAY_FILE"
fi

YESTERDAY_FILE="$MEMORY_DIR/${YESTERDAY}.md"
if [ -f "$YESTERDAY_FILE" ]; then
    FILES_TO_READ="$FILES_TO_READ
3. $YESTERDAY_FILE"
fi

# --- Output short directive ---
python3 -c "
import json
files = '''$FILES_TO_READ'''
nowstr = '''$NOWSTR'''
msg = '[SESSION START — MANDATORY READ]\nCurrent time: ' + nowstr + '\n\nBLOCKING REQUIREMENT: You MUST call the Read tool on every file listed below BEFORE producing ANY text or tool call. This is not optional. Do not respond to the user\'s message first. Do not skip this because the task seems urgent. Read these files NOW:\n' + files
print(json.dumps({
    'hookSpecificOutput': {
        'hookEventName': 'SessionStart',
        'additionalContext': msg,
    }
}, ensure_ascii=False))
"
exit 0
