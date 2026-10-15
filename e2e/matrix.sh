#!/usr/bin/env bash
# Eureka compatibility: runs the Eureka scenario with Eureka Server and Eureka clients built from each
# Spring Cloud train, then prints one summary line per train.
#
#   ./matrix.sh [run.sh options except -s]      TRAINS="2021.0 2025.0" ./matrix.sh -n v3.2.4
cd "$(dirname "$0")"
OUT_DIR=${OUT_DIR:-out}
TRAINS=${TRAINS:-"2021.0 2022.0 2023.0 2024.0 2025.0"}
SCENARIOS=${SCENARIOS:-01-eureka-nacos}
rc=0
# Only reports of this run count: a train that failed before writing one must not show an older result.
mkdir -p "$OUT_DIR"; started="$OUT_DIR/.matrix-started"; touch "$started"
for t in $TRAINS; do
  ./run.sh -s "$t" "$@" $SCENARIOS || rc=1
done
echo
echo "| Spring Cloud train | Result |"
echo "|---|---|"
for t in $TRAINS; do
  r=$(find "$OUT_DIR" -maxdepth 1 -name "report-*-sc$t.md" -newer "$started" | sort | tail -1)
  echo "| $t | $( [ -n "$r" ] && grep -E '^PASS [0-9]+, FAIL' "$r" || echo 'not run') |"
done
exit $rc
