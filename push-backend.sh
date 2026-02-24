#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: not inside a git repository: $REPO_DIR" >&2
  exit 1
fi

BRANCH="${1:-$(git rev-parse --abbrev-ref HEAD)}"
COMMIT_MSG="${2:-chore: switch service day boundary from 4am to 12am}"

echo "Staging backend changes..."
git add database.js

if git diff --cached --quiet; then
  echo "No staged backend changes to commit."
  exit 0
fi

echo "Committing with message: $COMMIT_MSG"
git commit -m "$COMMIT_MSG"

echo "Pushing branch '$BRANCH' to origin..."
git push origin "$BRANCH"

echo "Done."
