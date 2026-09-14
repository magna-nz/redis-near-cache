#!/usr/bin/env bash
# Runs locally what the required CI checks run, for when a PR has to be merged without GitHub Actions:
# build, unit tests, then for each server image the whole Docker stack, the integration suite and the
# "every suite ran" check. Stops at the first failure. Results land in tests/*/TestResults/.
#
#   ./ci-local.sh            # redis:7.4 only (the "integration tests" check)
#   ./ci-local.sh --matrix   # redis:7.4 plus every image in the CI matrix (all required checks)
set -euo pipefail
cd "$(dirname "$0")"
DOTNET="${DOTNET:-$(command -v dotnet || echo "$HOME/.dotnet/dotnet")}"

images=("redis:7.4")
if [ "${1:-}" = "--matrix" ]; then
  # Same list as the matrix job in .github/workflows/ci.yml.
  images+=($(sed -n 's/^ *image: \[\(.*\)\]/\1/p' .github/workflows/ci.yml | tr -d '",'))
fi
required="$(sed -n 's/^ *RNC_REQUIRED_SUITES: *//p' .github/workflows/ci.yml)"

"$DOTNET" build -c Release -nologo
"$DOTNET" test tests/RedisNearCache.UnitTests -c Release --no-build -nologo

summary="tests/RedisNearCache.Tests/TestResults/ci-local-summary.md"
mkdir -p "$(dirname "$summary")"
: > "$summary"
for image in "${images[@]}"; do
  echo "=== $image"
  trx="ci-local-${image//[:\/]/-}.trx"
  RNC_REDIS_IMAGE="$image" ./up.sh
  RNC_REDIS_IMAGE="$image" "$DOTNET" test tests/RedisNearCache.Tests -c Release --no-build -nologo \
    --filter "Category!=Soak" --logger "trx;LogFileName=$trx"
  managed=3
  case "$image" in redis:6*) managed=1 ;; esac
  # shellcheck disable=SC2086
  GITHUB_STEP_SUMMARY="$summary" .github/scripts/check-suites-ran.sh "tests/RedisNearCache.Tests/TestResults/$trx" $required Managed=$managed
done

if [ ${#images[@]} -gt 1 ]; then RNC_REDIS_IMAGE=redis:7.4 ./up.sh >/dev/null; fi
echo
echo "All required checks passed locally on: ${images[*]} (commit $(git rev-parse --short HEAD)$(git diff --quiet || echo ', with uncommitted changes'))"
echo "Per-suite counts: $summary"
