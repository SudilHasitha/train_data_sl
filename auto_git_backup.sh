#!/usr/bin/env bash
set -euo pipefail

# Config (env overrides)
REPO_DIR="${REPO_DIR:-$PWD}"          # path to repo
PREFIX="${PREFIX:-data backup}"       # commit message prefix
INTERVAL="${INTERVAL:-900}"           # seconds between checks (default 15m)
START_FROM="${START_FROM:-3}"         # if no existing "data backup N", start from this

cd "$REPO_DIR"

# Determine branch once
BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# Find last "data backup N" in history (subject line only)
last_num="$(
  git log --grep="^${PREFIX} [0-9]\{1,\}$" -n1 --pretty=format:%s \
  | grep -Eo '[0-9]+$' || true
)"
if [[ -z "${last_num}" ]]; then
  last_num="${START_FROM}"
fi

# Helper: is working tree clean?
is_clean() {
  # Porcelain output is stable for scripts
  # Non-empty output means there are changes to commit.
  # https://git-scm.com/docs/git-status (porcelain format)
  [[ -z "$(git status --porcelain)" ]]
}

echo "[auto_backup] repo=$REPO_DIR branch=$BRANCH prefix='$PREFIX' start_from=$last_num interval=${INTERVAL}s"

while true; do
  ts="$(date -Is)"
  if is_clean; then
    echo "[$ts] no changes; sleeping ${INTERVAL}s"
    sleep "$INTERVAL"
    continue
  fi

  # Bump counter and compose message
  next_num=$((10#$last_num + 1))
  msg=$(printf "%s %02d" "$PREFIX" "$next_num")

  # Add, commit, push
  git add -A
  # Use a regular commit; if nothing staged, Git will error (which is fine).
  git commit -m "$msg"
  # Push current branch to the same branch name on remote
  # https://git-scm.com/docs/git-push/2.7.6  (git push origin HEAD)
  git push -u origin HEAD

  echo "[$ts] pushed: '$msg' to origin/$BRANCH"
  last_num="$next_num"
  sleep "$INTERVAL"
done
