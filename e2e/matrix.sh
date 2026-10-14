#!/usr/bin/env bash
# Eureka compatibility: runs the Eureka scenario with Eureka Server and Eureka clients built from each
# Spring Cloud train, then prints one summary line per train.
#
#   ./matrix.sh [run.sh options except -s]      TRAINS="2021.0 2025.0" ./matrix.sh -n v3.2.4
cd "$(dirname "$0")"
TRAINS=${TRAINS:-"2021.0 2022.0 2023.0 2024.0 2025.0"}
SCENARIOS=${SCENARIOS:-01-eureka-nacos}
rc=0
for t in $TRAINS; do
  ./run.sh -s "$t" "$@" $SCENARIOS || rc=1
done
echo
echo "| Spring Cloud train | Result |"
echo "|---|---|"
for t in $TRAINS; do
  r=$(ls -t out/report-*-sc"$t".md 2>/dev/null | head -1)
  echo "| $t | $( [ -n "$r" ] && grep -E '^PASS [0-9]+, FAIL' "$r" || echo 'not run') |"
done
exit $rc
