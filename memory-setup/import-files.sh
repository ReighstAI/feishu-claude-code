#!/bin/bash
# Bulk import existing files into Hindsight memory bank.
# Scans workspace, OpenClaw backup, and Claude auto-memory for text-based files,
# then uploads each to the Hindsight retain API for fact extraction.
#
# Usage: bash import-files.sh [bank_id]
# Default bank_id: "assistant"

set -e

BANK_ID="${1:-assistant}"
API_URL="http://localhost:9077"
IMPORTED=0
SKIPPED=0
ERRORS=0

# --- Check daemon health ---
if ! curl -sf "$API_URL/health" > /dev/null 2>&1; then
    echo "[ERROR] Hindsight daemon not running at $API_URL"
    echo "Start it first: uvx hindsight-embed@0.6.0 daemon --profile claude-code start"
    exit 1
fi
echo "[OK] Daemon healthy at $API_URL"

# --- Collect files to import ---
FILES=()

# 1. Current workspace
if [ -d "memory" ]; then
    while IFS= read -r f; do FILES+=("$f"); done < <(find memory -name "*.md" -type f 2>/dev/null)
fi
if [ -f "CLAUDE.md" ]; then
    FILES+=("CLAUDE.md")
fi

# 2. OpenClaw backup
for oc_dir in ~/Downloads/.openclaw/workspace ~/.openclaw/workspace; do
    if [ -d "$oc_dir" ]; then
        echo "[SCAN] OpenClaw backup: $oc_dir"
        while IFS= read -r f; do FILES+=("$f"); done < <(find "$oc_dir" -maxdepth 3 -type f \( -name "*.md" -o -name "*.txt" -o -name "*.json" \) -size -800k 2>/dev/null)
    fi
done

# 3. Claude Code auto-memory
for mem_dir in ~/.claude/projects/*/memory; do
    if [ -d "$mem_dir" ]; then
        echo "[SCAN] Auto-memory: $mem_dir"
        while IFS= read -r f; do FILES+=("$f"); done < <(find "$mem_dir" -name "*.md" -type f -size -800k 2>/dev/null)
    fi
done

# 4. user.md
if [ -f ~/.claude/user.md ]; then
    FILES+=("$HOME/.claude/user.md")
fi

TOTAL=${#FILES[@]}
echo "[INFO] Found $TOTAL files to import into bank '$BANK_ID'"

if [ "$TOTAL" -eq 0 ]; then
    echo "[DONE] No files to import."
    exit 0
fi

# --- Import each file ---
for f in "${FILES[@]}"; do
    # Skip empty files
    if [ ! -s "$f" ]; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Skip binary files
    if file "$f" | grep -q "binary\|executable\|image\|archive"; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    FNAME=$(basename "$f")
    RESPONSE=$(curl -sf -X POST "$API_URL/v1/default/banks/$BANK_ID/files/retain" \
        -F "files=@$f" \
        -F "request={\"tags\":[\"bulk-import\"],\"context\":\"initial-import\"}" \
        --max-time 60 2>&1) || {
        echo "[FAIL] $FNAME"
        ERRORS=$((ERRORS + 1))
        continue
    }

    IMPORTED=$((IMPORTED + 1))
    # Progress every 10 files
    if [ $((IMPORTED % 10)) -eq 0 ]; then
        echo "[PROGRESS] $IMPORTED/$TOTAL imported..."
    fi
done

echo ""
echo "[DONE] Import complete."
echo "  Imported: $IMPORTED"
echo "  Skipped:  $SKIPPED"
echo "  Errors:   $ERRORS"
echo "  Total:    $TOTAL"
echo ""
echo "Note: Fact extraction runs in the background. Full processing may take several minutes."
