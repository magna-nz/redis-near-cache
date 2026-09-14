#!/usr/bin/env bash
# Fails when a test suite silently did not run. `dotnet test` only fails on a failed test, so a filter change,
# a skip attribute or a missing container that leaves a whole suite at zero tests would otherwise stay green.
#
#   check-suites-ran.sh <trx file> <Suite>=<min passed> [<Suite>=<min passed> ...]
#
# <Suite> is the namespace segment after RedisNearCache.Tests (Sentinel, Managed, ...); "Core" means classes
# directly in RedisNearCache.Tests. Writes a per-suite table (and every skipped test) to $GITHUB_STEP_SUMMARY
# when it is set.
set -euo pipefail
trx="$1"; shift
[ -f "$trx" ] || { echo "no test results at $trx" >&2; exit 1; }

# One "<suite> <outcome> <test name>" line per result. Theory arguments in the name may contain dots and
# spaces, so the namespace is read from the part before "(" and the name is kept whole after the tab.
results="$(grep -o '<UnitTestResult [^>]*>' "$trx" | sed -E 's/.*testName="([^"]*)".*outcome="([^"]*)".*/\2	\1/' |
  awk -F'\t' '{
    name = $2
    method = name; sub(/\(.*/, "", method)
    rest = substr(method, length("RedisNearCache.Tests.") + 1)
    n = split(rest, parts, ".")
    suite = (n >= 3) ? parts[1] : "Core"
    print suite, $1, name
  }')"

summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
{
  echo "### Test suites ($(basename "$trx"))"
  echo
  echo "| Suite | Passed | Failed | Skipped |"
  echo "|---|---|---|---|"
  echo "$results" | awk '{ s[$1]; c[$1" "$2]++ } END { for (k in s) printf "| %s | %d | %d | %d |\n", k, c[k" Passed"], c[k" Failed"], c[k" NotExecuted"] }' | sort
  skipped="$(echo "$results" | awk '$2 == "NotExecuted" { print "- " $3 }')"
  if [ -n "$skipped" ]; then echo; echo "Skipped:"; echo "$skipped"; fi
} >> "$summary"

status=0
for requirement in "$@"; do
  suite="${requirement%%=*}"
  min="${requirement#*=}"
  passed="$(echo "$results" | awk -v s="$suite" '$1 == s && $2 == "Passed"' | wc -l | tr -d ' ')"
  if [ "$passed" -lt "$min" ]; then
    echo "::error::suite $suite: $passed passed, expected at least $min"
    status=1
  else
    echo "suite $suite: $passed passed (min $min)"
  fi
done
exit $status
