#!/bin/bash
# ABOUTME: Discord notification script for Claude task completions
# ABOUTME: Called by Claude when completing tasks during work sessions

set -e

# Load webhook URL from config or environment
if [ -f .discord-webhook ]; then
    DISCORD_WEBHOOK=$(cat .discord-webhook)
elif [ -n "$DISCORD_WEBHOOK" ]; then
    DISCORD_WEBHOOK="$DISCORD_WEBHOOK"
else
    echo "Error: Discord webhook not configured"
    echo "Create .discord-webhook file with your webhook URL or set DISCORD_WEBHOOK env var"
    exit 1
fi

# Get task details from arguments
TASK_TITLE="${1:-Task completed}"
TASK_DESCRIPTION="${2:-}"
TASK_STATUS="${3:-✅}"
ISSUE_NUMBER="${4:-}"

# Get git context
BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")
LAST_COMMIT=$(git log -1 --pretty=format:"%h - %s" 2>/dev/null || echo "No recent commit")
REPO_URL=$(git remote get-url origin 2>/dev/null | sed 's/\.git$//' || echo "")

# Build embed description
EMBED_DESC="**Branch:** \`${BRANCH}\`\n**Last Commit:** ${LAST_COMMIT}"

if [ -n "$TASK_DESCRIPTION" ]; then
    EMBED_DESC="${EMBED_DESC}\n\n${TASK_DESCRIPTION}"
fi

# Add issue link if provided
if [ -n "$ISSUE_NUMBER" ]; then
    EMBED_DESC="${EMBED_DESC}\n\n[View Issue #${ISSUE_NUMBER}](${REPO_URL}/issues/${ISSUE_NUMBER})"
fi

# Determine color based on status
COLOR=3066993  # Green
if [[ "$TASK_STATUS" == "⚠️" ]]; then
    COLOR=16776960  # Yellow
elif [[ "$TASK_STATUS" == "❌" ]]; then
    COLOR=15158332  # Red
fi

# Send notification
curl -X POST "$DISCORD_WEBHOOK" \
  -H "Content-Type: application/json" \
  -d "{
    \"content\": \"${TASK_STATUS} **AI Engineer Task Update**\",
    \"embeds\": [{
      \"title\": \"${TASK_TITLE}\",
      \"description\": \"${EMBED_DESC}\",
      \"color\": ${COLOR},
      \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\"
    }]
  }" \
  2>/dev/null || echo "Failed to send Discord notification"

echo "✓ Discord notification sent"
