#!/usr/bin/env bash
# Builds nacos-sync and the example apps, starts the E2E environment and runs the scenarios.
#
#   ./run.sh [-r <nacos-sync repo>] [-b <git ref>] [-e <examples repo>] [-E <examples ref>] [-n <nacos tag>]
#            [-s <spring cloud train>] [-k] [scenario ...]
#
#   -r  nacos-sync checkout to build (default: the one this script is in). Only committed code is built
#       (git archive), so local uncommitted changes such as credentials never end up in the image.
#   -b  git ref to build (default: HEAD)
#   -e  nacos-sync-example checkout or git URL with the example applications (default: the GitHub repository)
#   -E  examples ref to build (default: the commit pinned below). Also only committed code.
#   -n  nacos/nacos-server tag, e.g. v2.5.4 or v3.2.4 (default: NACOS_VERSION in .env)
#   -s  Spring Cloud train for the Eureka Server and the Eureka example apps: 2021.0 2022.0 2023.0 2024.0 2025.0
#   -k  keep the environment running afterwards
#   scenario  names from scenarios/, e.g. 01-eureka-nacos (default: all)
set -uo pipefail
cd "$(dirname "$0")"
REPO=$(git rev-parse --show-toplevel) || exit 1
REF=HEAD
EXAMPLES=https://github.com/paderlol/nacos-sync-example.git
# The scenarios rely on fixes in the examples (runnable jars, a Eureka server that does not replicate to itself),
# so they run against a fixed commit rather than a branch.
EXAMPLES_REF=4fc95a5a7bf004db64ac3a2c4f644ffb03514531
KEEP=0
while getopts r:b:e:E:n:s:k opt; do
  case $opt in
    r) REPO=$(cd "$OPTARG" && pwd) ;;
    b) REF=$OPTARG ;;
    e) EXAMPLES=$OPTARG ;;
    E) EXAMPLES_REF=$OPTARG ;;
    n) export NACOS_VERSION=$OPTARG ;;
    s) export SC_TRAIN=$OPTARG ;;
    k) KEEP=1 ;;
    *) sed -n '2,16p' "$0"; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
export SYNC_REPO=$REPO   # 07-upgrade builds the previous release from the same checkout
. ./lib.sh   # -n / -s were exported above, so they win over .env

sc_versions() { # <train> -> "<spring-boot.version> <spring-cloud.version>"
  case $1 in
    2021.0) echo "2.7.18 2021.0.9" ;;
    2022.0) echo "3.0.13 2022.0.5" ;;
    2023.0) echo "3.3.13 2023.0.6" ;;
    2024.0) echo "3.4.13 2024.0.3" ;;
    2025.0) echo "3.5.16 2025.0.3" ;;
    *) return 1 ;;
  esac
}
read -r SB_VERSION SC_VERSION <<<"$(sc_versions "$SC_TRAIN")" || { log "unknown Spring Cloud train $SC_TRAIN"; exit 2; }

mvn_q() { (cd "$1" && shift && mvn -B -q -DskipTests "$@") > "$OUT_DIR/logs/build.log" 2>&1 || { log "build failed in $1, see $OUT_DIR/logs/build.log"; exit 1; }; }

# ---------------------------------------------------------------- build
SRC="$OUT_DIR/nacos-sync-src"
COMMIT=$(git -C "$REPO" rev-parse --short "$REF") || exit 1
if [ "$(cat "$SRC/.commit" 2>/dev/null)" != "$COMMIT" ]; then
  log "building nacos-sync $COMMIT from $REPO"
  rm -rf "$SRC" && mkdir -p "$SRC" && git -C "$REPO" archive "$REF" | tar -x -C "$SRC" || exit 1
  mvn_q "$SRC" package
  echo "$COMMIT" > "$SRC/.commit"
fi
# One results file and report per run, so earlier runs never leak into this report.
RUN_ID=$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT_DIR/runs"
export RESULTS="$OUT_DIR/runs/$RUN_ID-$NACOS_VERSION-sc$SC_TRAIN.tsv"
export RUN_LABEL="nacos=$NACOS_VERSION sc=$SC_TRAIN sync=$COMMIT"
rm -rf nacos-sync/dist && cp -R "$SRC"/nacossync-distribution/target/nacos-sync-*/nacos-sync nacos-sync/dist
docker build -q -t nacos-sync-e2e/nacos-sync:local nacos-sync >/dev/null || exit 1

if [ -d "$EXAMPLES" ]; then
  EXAMPLES_GIT=$(cd "$EXAMPLES" && pwd)
else
  EXAMPLES_GIT="$OUT_DIR/cache/nacos-sync-example.git"
  [ -d "$EXAMPLES_GIT" ] || git clone -q --bare "$EXAMPLES" "$EXAMPLES_GIT" || exit 1
  git -C "$EXAMPLES_GIT" cat-file -e "$EXAMPLES_REF^{commit}" 2>/dev/null ||
    git -C "$EXAMPLES_GIT" fetch -q origin '+refs/heads/*:refs/heads/*' || exit 1
fi
EXAMPLES_COMMIT=$(git -C "$EXAMPLES_GIT" rev-parse --short "$EXAMPLES_REF^{commit}" 2>/dev/null) ||
  { log "examples ref $EXAMPLES_REF not found in $EXAMPLES"; exit 1; }
if [ "$(cat "$EXAMPLES_DIR/.commit" 2>/dev/null)" != "$EXAMPLES_COMMIT" ]; then
  log "extracting examples $EXAMPLES_COMMIT from $EXAMPLES"
  rm -rf "$EXAMPLES_DIR" && mkdir -p "$EXAMPLES_DIR" && git -C "$EXAMPLES_GIT" archive "$EXAMPLES_REF" |
    tar -x -C "$EXAMPLES_DIR" || exit 1
  echo "$EXAMPLES_COMMIT" > "$EXAMPLES_DIR/.commit"
fi
log "building examples $EXAMPLES_COMMIT (Eureka apps on Spring Cloud $SC_VERSION / Boot $SB_VERSION)"
[ -f "$EXAMPLES_DIR"/cloud-consul/consul-provider-cloud/target/consul-provider-cloud-1.0-SNAPSHOT.jar ] ||
  mvn_q "$EXAMPLES_DIR" package
mvn_q "$EXAMPLES_DIR" package -pl cloud-eureka/eureka-server,cloud-eureka/eureka-provider -am \
  -Dspring-boot.version="$SB_VERSION" -Dspring-cloud.version="$SC_VERSION"
cp "$EXAMPLES_DIR"/cloud-eureka/eureka-server/target/eureka-server-1.0-SNAPSHOT.jar eureka-server/app.jar
docker build -q -t "nacos-sync-e2e/eureka-server:$SC_TRAIN" eureka-server >/dev/null || exit 1
[ -f tools/nacos-load/target/nacos-load-1.0-SNAPSHOT.jar ] || mvn_q tools/nacos-load package

# ---------------------------------------------------------------- environment
log "starting environment: $RUN_LABEL, nacos-sync $COMMIT"
compose down -v --remove-orphans >/dev/null 2>&1
compose up -d --wait --wait-timeout 300 >/dev/null 2>&1 || { compose ps; log "environment did not become healthy"; exit 1; }
init_registries || exit 1
record INFO "environment up: nacos-sync $COMMIT, examples $EXAMPLES_COMMIT, $RUN_LABEL (Boot $SB_VERSION, Cloud $SC_VERSION)"

# ---------------------------------------------------------------- scenarios
if [ $# -eq 0 ]; then set -- $(ls scenarios | sed 's/\.sh$//'); fi
for s in "$@"; do
  log "=== scenario $s"
  RUN_LABEL=$RUN_LABEL bash "scenarios/$s.sh" 2>&1 | tee "$OUT_DIR/logs/scenario-$s.log" >&2
  stop_all_apps; delete_all_tasks
  compose logs nacos-sync > "$OUT_DIR/logs/nacos-sync-$s.log" 2>&1
done

# ---------------------------------------------------------------- report
REPORT="$OUT_DIR/report-$RUN_ID-${NACOS_VERSION}-sc${SC_TRAIN}.md"
{
  echo "# nacos-sync E2E: $RUN_LABEL"
  echo
  echo "nacos-sync \`$COMMIT\`, examples \`$EXAMPLES_COMMIT\`, Spring Boot $SB_VERSION, Spring Cloud $SC_VERSION, $(date '+%Y-%m-%d %H:%M')"
  echo
  awk -F'\t' -v l="$RUN_LABEL" '$1 == l && $3 != "INFO" {n[$3]++} END {printf "PASS %d, FAIL %d\n\n", n["PASS"], n["FAIL"]}' "$RESULTS"
  echo "| Scenario | Result | Check |"
  echo "|---|---|---|"
  awk -F'\t' -v l="$RUN_LABEL" '$1 == l {printf "| %s | %s | %s |\n", $2, $3, $4}' "$RESULTS"
} > "$REPORT"
log "report: $REPORT"
grep -E '^PASS [0-9]+, FAIL' "$REPORT" >&2

[ "$KEEP" = 1 ] || compose down -v >/dev/null 2>&1
awk -F'\t' -v l="$RUN_LABEL" '$1 == l && $3 == "FAIL" {f=1} END {exit f}' "$RESULTS"
