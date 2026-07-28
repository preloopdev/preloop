#!/bin/sh
# Deliberately fails until marker.txt says FIXED. Edit it on the host, then
# `:sync` + `:retry` (or `s`) from the paused session.
marker="$(dirname "$0")/marker.txt"
echo "verifying $marker ..."
if grep -q FIXED "$marker"; then
    echo "verification passed"
    exit 0
fi
echo "verification FAILED: marker says '$(cat "$marker")', expected FIXED"
exit 1
