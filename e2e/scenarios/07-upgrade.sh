#!/usr/bin/env bash
# Upgrade from the previous release (OLD_REF, default 0.5.0) to the nacos-sync under test and back, on one MySQL
# database: 1. single-node swap, 4. rollback, 2. both versions running at once, 3. the new version cleans up copies
# the old one registered, 5. the old release against Nacos 3.x. Gaps are measured by background samplers (one per
# synced copy, about 1 s apart). Needs Nacos 2.x (-n v2.5.4): 0.5.0 talks to Nacos over the v1 HTTP API only.
# Writes out/report-upgrade-<timestamp>.md.
#
#   OLD_REF        previous release, a git ref in the nacos-sync checkout (default 0.5.0)
#   OLD_JAVA_HOME  JDK that builds it (default JAVA_HOME; 0.5.0 needs 11)
#   OLD_JRE        eclipse-temurin JRE tag it runs on (default 11)
#   OVERLAP        seconds both versions run side by side (default 180)
#   SWAP_WAIT      seconds between stopping the old node and starting the new one in case 1 (default 0)
#   GAP_MAX        longest acceptable gap in seconds for any synced copy during a transition (default 60)
. "$(dirname "$0")/../lib.sh"
SCENARIO=upgrade-setup
OLD_REF=${OLD_REF:-0.5.0}
OLD_JAVA_HOME=${OLD_JAVA_HOME:-${JAVA_HOME:-}}
OLD_JRE=${OLD_JRE:-11}
OVERLAP=${OVERLAP:-180}
GAP_MAX=${GAP_MAX:-60}
SWAP_WAIT=${SWAP_WAIT:-0}
SYNC_REPO=${SYNC_REPO:-$(git -C "$E2E_DIR" rev-parse --show-toplevel)}
NEW_URL=$SYNC_URL              # service nacos-sync: the version under test
OLD_URL=http://localhost:8084  # service nacos-sync-old (compose profile "upgrade")
V3_PORT=28848                  # service nacos-v3
TS=$(date +%Y%m%d-%H%M%S)
UP_DIR="$OUT_DIR/upgrade/$TS"  # samples, schema dumps, report fragments
REPORT="$OUT_DIR/report-upgrade-$TS.md"
mkdir -p "$UP_DIR"

if nacos_is_v3; then
  record INFO "skipped: needs Nacos 2.x (-n v2.5.4), $OLD_REF only speaks the Nacos v1 HTTP API"
  exit 0
fi

# The gaps are measured with EPOCHREALTIME, which bash has since 5.0; its decimal separator follows the locale.
if ((BASH_VERSINFO[0] < 5)); then
  record FAIL "needs bash 5 or later to measure the gaps, this is bash $BASH_VERSION"
  exit 0
fi

ucompose() { compose --profile upgrade "$@"; }
now_ms() { local t=${EPOCHREALTIME/[.,]/}; echo "${t:0:13}"; }
since() { echo $((($(now_ms) - $1) / 1000)); }
wait_until() { while (($(now_ms) < $1)); do sleep 1; done; }
on() { # on old|new <command...>: run a nacos-sync API helper against one of the two versions
  local SYNC_URL
  case $1 in old) SYNC_URL=$OLD_URL ;; new) SYNC_URL=$NEW_URL ;; esac
  shift; "$@"
}

# ---------------------------------------------------------------- old release image and database
build_old_image() {
  local src="$OUT_DIR/nacos-sync-old-src" commit dist
  commit=$(git -C "$SYNC_REPO" rev-parse --short "$OLD_REF^{commit}") || return 1
  if [ "$(cat "$src/.commit" 2>/dev/null)" != "$commit" ]; then
    log "building nacos-sync $OLD_REF ($commit) with ${OLD_JAVA_HOME:-the default JDK}"
    rm -rf "$src" && mkdir -p "$src" && git -C "$SYNC_REPO" archive "$OLD_REF" | tar -x -C "$src" || return 1
    (cd "$src" && JAVA_HOME=$OLD_JAVA_HOME mvn -B -q -DskipTests package) > "$OUT_DIR/logs/build-old.log" 2>&1 ||
      { log "build of $OLD_REF failed, see $OUT_DIR/logs/build-old.log"; return 1; }
    echo "$commit" > "$src/.commit"
  fi
  dist=$(ls -d "$src"/nacossync-distribution/target/nacos-sync-*/ | head -1)
  docker build -q --build-arg JRE_VERSION="$OLD_JRE" --build-arg DIST=nacos-sync -f "$E2E_DIR/nacos-sync/Dockerfile" \
    -t nacos-sync-e2e/nacos-sync:old "$dist" >/dev/null
}

mysql_q() { compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql mysql -uroot -N -B -r "$@"; }

schema_dump() { # schema_dump <file>: DDL of every nacos-sync table
  local t
  for t in $(mysql_q -e "SELECT table_name FROM information_schema.tables WHERE table_schema='nacos_sync' ORDER BY 1"); do
    mysql_q nacos_sync -e "SHOW CREATE TABLE \`$t\`" | cut -f2- | sed 's/ AUTO_INCREMENT=[0-9]*//'
  done > "$1"
}

schema_errors() { # schema_errors <service> <since-epoch-s>: DDL/schema failures logged since then
  ucompose logs --no-log-prefix --since "$2" "$1" 2>&1 |
    grep -iE 'GenerationTarget encountered exception|SchemaManagementException|Error executing DDL|Unknown column|doesn.t exist'
}
no_schema_errors() { ! schema_errors "$@"; }

start_old() { # start_old: prints seconds until its API answers
  local t; t=$(now_ms)
  ucompose up -d nacos-sync-old >/dev/null 2>&1 && on old wait_sync_ready && since "$t"
}

start_new() {
  local t; t=$(now_ms)
  compose start nacos-sync >/dev/null 2>&1 && on new wait_sync_ready && since "$t"
}

tasks_loaded() { # tasks_loaded <taskId...>: the node at SYNC_URL lists exactly these tasks
  [ "$(task_ids | sort | tr '\n' ' ')" = "$(printf '%s\n' "$@" | sort | tr '\n' ' ')" ]
}

# ---------------------------------------------------------------- what is synced, and probes for it
EP=spring-cloud-eureka-provider  # Eureka -> Nacos A
NC=spring-cloud-nacos-consumer   # Nacos A -> Eureka
CP=consul-provider-cloud         # Consul -> Nacos A
NP=nacos-provider-cloud          # Nacos A -> Consul, Nacos A -> Nacos B
IFACE=com.paderlol.nacos.api.DemoService
ZK_SVC="providers:$IFACE:1.0.0:zk"        # Zookeeper -> Nacos A (group zk)
NACOS_SVC="providers:$IFACE:1.0.0:nacos"  # Nacos A -> Zookeeper
PS=e2e-upgrade-persistent                 # persistent instances, Nacos A -> Nacos B
GA=E2E_ALL                                # group copied by the serviceName=ALL task, Nacos A -> Nacos B
dubbo_json() { jq -nc --arg a "$1" '{dubbo: {registry: {address: $a}, "metadata-report": {address: $a}, application: {"qos-enable": false}}}'; }

nacos_copies() { # nacos_copies <port> <service> <group> <sourceClusterId>: "<healthy> <total>" synced copies
  nacos_instances "$1" "$2" "$3" |
    jq -r --arg c "$4" '[.[] | select(.metadata.sourceClusterId == $c)] | "\(map(select(.healthy)) | length) \(length)"'
}

probe() { # probe <name>: "<healthy> <total>" synced copies at that destination
  local n
  case $1 in
    eureka-nacos) nacos_copies $NACOS_A $EP DEFAULT_GROUP "$EUREKA_ID" ;;
    consul-nacos) nacos_copies $NACOS_A $CP DEFAULT_GROUP "$CONSUL_ID" ;;
    zk-nacos) nacos_copies $NACOS_A "$ZK_SVC" zk "$ZK_ID" ;;
    nacos-nacos) nacos_copies $NACOS_B $NP DEFAULT_GROUP "$NACOS_A_ID" ;;
    # Nacos health-checks persistent instances by TCP and these addresses do not exist: count presence only.
    nacos-nacos-persistent) n=$(nacos_copies $NACOS_B $PS DEFAULT_GROUP "$NACOS_A_ID" | cut -d' ' -f2); echo "$n $n" ;;
    nacos-all-1) nacos_copies $NACOS_B $NP $GA "$NACOS_A_ID" ;;
    nacos-all-2) nacos_copies $NACOS_B $NC $GA "$NACOS_A_ID" ;;
    nacos-eureka) eureka_instances $NC | jq -r --arg c "$NACOS_A_ID" \
      '[.[] | select(.metadata.sourceClusterId == $c)] | "\(map(select(.status == "UP")) | length) \(length)"' ;;
    nacos-consul) consul_instances $NP | jq -r --arg c "$NACOS_A_ID" \
      '[.[] | select(any(.tags[]?; . == "sourceClusterId=" + $c))] | "\(map(select(.passing)) | length) \(length)"' ;;
    nacos-zk) n=$(zk_providers $IFACE | grep 'group=nacos' | grep -c "sourceClusterId=$NACOS_A_ID"); echo "$n $n" ;;
    created-by-new) nacos_copies $NACOS_B $NC DEFAULT_GROUP "$NACOS_A_ID" ;;
  esac
}
expected() { case $1 in nacos-nacos-persistent) echo 2 ;; *) echo 1 ;; esac; }
PROBES=(eureka-nacos nacos-eureka consul-nacos nacos-consul zk-nacos nacos-zk nacos-nacos nacos-nacos-persistent nacos-all-1 nacos-all-2)

synced_once() { # every probe sees exactly the expected number of copies, all healthy; prints the first mismatch
  local p h n e
  for p in "${PROBES[@]}"; do
    read -r h n <<<"$(probe "$p")"; e=$(expected "$p")
    [ "${h:-0}" -eq "$e" ] && [ "${n:-0}" -eq "$e" ] || { echo "$p: $h healthy of $n copies, expected $e"; return 1; }
  done
}
converge() { # converge <timeout> <description>
  eventually "$1" "$2" synced_once || record INFO "$2: still $(synced_once)"
}
copies_gone() { local h n; read -r h n <<<"$(probe "$1")"; [ "${n:-0}" -eq 0 ]; }
zk_native_ok() { [ "$(zk_providers $IFACE | grep -c group=zk)" -eq 1 ]; }
nacos_native_ok() { nacos_has_port $NACOS_A $NC 18035 && nacos_has_port $NACOS_A $NP 18088 && nacos_count $NACOS_A $PS 2; }
v3() { local NACOS_VERSION=v3.2.4; "$@"; }   # v3 <lib helper...>: query the Nacos 3.x server

# ---------------------------------------------------------------- gap sampling
SAMPLERS=()
sample_start() { # sample_start <name>: one loop per probe appending "<epoch-ms> <probe> <healthy> <total>"
  local p f="$UP_DIR/$1.samples"
  rm -f "$OUT_DIR"/cache/token-*; nacos_token $NACOS_A >/dev/null; nacos_token $NACOS_B >/dev/null  # no refresh races
  : > "$f"
  for p in "${PROBES[@]}"; do
    (while :; do echo "$(now_ms) $p $(probe "$p")" >> "$f"; sleep 1; done) >/dev/null 2>&1 &
    SAMPLERS+=($!)
  done
}
sample_stop() {
  ((${#SAMPLERS[@]})) || return 0
  kill "${SAMPLERS[@]}" 2>/dev/null; wait "${SAMPLERS[@]}" 2>/dev/null; SAMPLERS=()
}

gap_report() { # gap_report <name> <title> <t0-ms> [<from-ms> <to-ms>]: <name>.md table and <name>.tsv summary
  python3 -I - "$UP_DIR/$1.samples" "$UP_DIR/$1" "$2" "$3" "${4:-}" "${5:-}" <<'PY'
import collections, statistics, sys
samples, out, title, t0 = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
lo, hi = (int(v) if v else None for v in sys.argv[5:7])
expected = {'nacos-nacos-persistent': 2}
rows, bad = collections.defaultdict(list), collections.Counter()
for line in open(samples):
    f = line.split()
    if len(f) != 4:
        if len(f) >= 2: bad[f[1]] += 1
        continue
    t = int(f[0])
    if (lo and t < lo) or (hi and t > hi): continue
    rows[f[1]].append((t, int(f[2]), int(f[3])))
sec = lambda ms: f"{ms / 1000:.1f}"
md = [f"#### {title}", "",
      "| Synced copy | Outages | Missing from (s after T0) | Restored (s after T0) | Longest gap (s) | Max copies seen (expected) | Samples |",
      "|---|---|---|---|---|---|---|"]
tsv = []
for p in sorted(rows):
    s, e = sorted(rows[p]), expected.get(p, 1)
    runs, start = [], None
    for t, h, n in s:
        if h < e and start is None: start = t
        if h >= e and start is not None: runs.append((start, t)); start = None
    gaps = [b - a for a, b in runs] + ([s[-1][0] - start] if start is not None else [])
    first = runs[0][0] if runs else start
    restored = 'not restored' if start is not None else (sec(runs[-1][1] - t0) if runs else '-')
    maxn = max(n for _, _, n in s)
    step = statistics.median(b[0] - a[0] for a, b in zip(s, s[1:])) if len(s) > 1 else 0
    md.append(f"| {p} | {len(gaps)} | {sec(first - t0) if first is not None else '-'} | {restored} | "
              f"{sec(max(gaps)) if gaps else '0'} | {maxn} ({e}) | {len(s)}, every {sec(step)} s"
              + (f", {bad[p]} failed" if bad[p] else "") + " |")
    tsv.append("\t".join([p, str(len(gaps)), sec(max(gaps)) if gaps else '0', str(maxn), str(e),
                          '0' if start is not None else '1']))
open(out + '.md', 'w').write("\n".join(md) + "\n")
open(out + '.tsv', 'w').write("\n".join(tsv) + "\n")
PY
}
gaps_info() { # gaps_info <name> <what>: one INFO line with the longest gap per synced copy
  record INFO "$2: longest gap per synced copy (s): $(awk -F'\t' '{printf "%s%s=%s", sep, $1, $3; sep=", "}' "$UP_DIR/$1.tsv")"
}
no_dups() { awk -F'\t' '$4 > $5 {print $1 " reached " $4 " copies"; bad=1} END {exit bad}' "$UP_DIR/$1.tsv"; }
gaps_within() { # gaps_within <name>: no synced copy missing longer than GAP_MAX, and every one restored
  awk -F'\t' -v m="$GAP_MAX" '$3 > m || $6 == 0 {print $1 " missing " $3 "s" ($6 == 0 ? " (not restored)" : ""); bad=1} END {exit bad}' "$UP_DIR/$1.tsv"
}
dup_diff() { # dup_diff <port> <service> <group>: how two listed copies of one ip:port differ (raw v1 list, Nacos 2.x)
  curl -s -G "localhost:$1/nacos/v1/ns/instance/list" --data-urlencode "serviceName=$2" --data-urlencode "groupName=$3" \
    --data-urlencode "accessToken=$(nacos_token "$1")" |
    jq -r '[.hosts[]?] | group_by("\(.ip):\(.port)")[] | select(length > 1) | .[0:2] as [$a, $b]
      | ([$a.metadata, $b.metadata] | map(keys) | add | unique | map(select($a.metadata[.] != $b.metadata[.])) | map("metadata." + .))
        + ($a | del(.metadata) | keys | map(select($a[.] != $b[.])) | map("\(.) \($a[.]) vs \($b[.])")) | join(", ")'
}
plain() { sed -E 's/<[^>]*>//g; s/[|]/\//g' | tr -s ' \n' ' ' | cut -c1-"${1:-300}"; }  # log text safe for a Markdown table
no_flaps() { awk -F'\t' '$2 > 0 {print $1 " missing " $2 "x, longest " $3 "s"; bad=1} END {exit bad}' "$UP_DIR/$1.tsv"; }
check_out() { # check_out <description> <command...>: like check, appending the command output on failure
  local desc=$1 o; shift
  if o=$("$@" 2>&1); then record PASS "$desc"; else record FAIL "$desc: $(echo $o)"; fi
}

# ---------------------------------------------------------------- report and cleanup
write_report() {
  {
    echo "# nacos-sync upgrade E2E: $OLD_REF <-> ${RUN_LABEL##*sync=}"
    echo
    echo "$(date '+%Y-%m-%d %H:%M'), $RUN_LABEL, Nacos $NACOS_VERSION (case 5: Nacos v3.2.4), overlap ${OVERLAP}s." \
      "Old: \`$OLD_REF\` ($(cat "$OUT_DIR/nacos-sync-old-src/.commit" 2>/dev/null)) on JRE $OLD_JRE."
    echo
    awk -F'\t' -v l="$RUN_LABEL" '$1 == l && $2 ~ /^upgrade/ && $3 != "INFO" {n[$3]++}
      END {printf "PASS %d, FAIL %d\n\n", n["PASS"], n["FAIL"]}' "$RESULTS"
    echo "| Case | Result | Check |"
    echo "|---|---|---|"
    awk -F'\t' -v l="$RUN_LABEL" '$1 == l && $2 ~ /^upgrade/ {gsub(/\|/, "\\|", $4); printf "| %s | %s | %s |\n", $2, $3, $4}' "$RESULTS"
    echo
    echo "## Gaps"
    echo
    echo "T0 is named in each table title. A copy counts as missing while fewer than the expected number of healthy"
    echo "copies are listed (Eureka: status UP; Consul: passing; Zookeeper: provider node present; persistent: present)."
    echo "Resolution is the sample interval; Eureka answers from its 30 s read-only response cache."
    echo
    for f in swap rollback overlap overlap-stop; do [ -f "$UP_DIR/$f.md" ] && cat "$UP_DIR/$f.md" && echo; done
    echo "## Schema"
    echo
    for f in old new rollback; do
      [ -f "$UP_DIR/schema-$f.sql" ] || continue
      echo "<details><summary>after $f</summary>"; echo; echo '```sql'; cat "$UP_DIR/schema-$f.sql"; echo '```'; echo "</details>"; echo
    done
    echo "Samples, schema dumps and logs: \`${UP_DIR#$E2E_DIR/}\`, \`out/logs/\`."
  } > "$REPORT"
  log "upgrade report: $REPORT"
}

cleanup() {
  sample_stop
  ucompose logs --no-log-prefix nacos-sync-old > "$OUT_DIR/logs/nacos-sync-old-upgrade.log" 2>&1
  ucompose logs --no-log-prefix nacos-v3 > "$OUT_DIR/logs/nacos-v3-upgrade.log" 2>&1
  ucompose rm -sf nacos-sync-old nacos-v3 >/dev/null 2>&1
  start_new >/dev/null   # leave the version under test running for run.sh
  write_report
}
trap cleanup EXIT

nacos_register_persistent() { # <port> <service> <ip> <instancePort>
  local tok; tok=$(nacos_token "$1")
  if nacos_is_v3; then
    curl -s -X POST -H "accessToken: $tok" "localhost:$1/nacos/v3/admin/ns/instance" --data-urlencode "serviceName=$2" \
      --data-urlencode "groupName=DEFAULT_GROUP" --data-urlencode "namespaceId=public" --data-urlencode "ip=$3" \
      --data-urlencode "port=$4" --data-urlencode "ephemeral=false"
  else
    curl -s -X POST "localhost:$1/nacos/v1/ns/instance" --data-urlencode "serviceName=$2" --data-urlencode "ip=$3" \
      --data-urlencode "port=$4" --data-urlencode "ephemeral=false" --data-urlencode "accessToken=$tok"
  fi
}

# ================================================================ setup: 0.5.0 creates the schema and every task
build_old_image || { record FAIL "build the $OLD_REF image"; exit 1; }
compose stop nacos-sync >/dev/null 2>&1
mysql_q -e 'DROP DATABASE IF EXISTS nacos_sync; CREATE DATABASE nacos_sync' || exit 1
s=$(start_old) || { record FAIL "$OLD_REF did not start on an empty database"; exit 1; }
record PASS "$OLD_REF started on an empty database (API up after ${s}s)"
schema_dump "$UP_DIR/schema-old.sql"
record INFO "$OLD_REF schema: $(grep -c '^CREATE TABLE' "$UP_DIR/schema-old.sql") tables, $(grep -o 'ENGINE=[A-Za-z]*' "$UP_DIR/schema-old.sql" | sort | uniq -c | xargs)"
SYNC_URL=$OLD_URL setup_clusters || { record FAIL "$OLD_REF: add clusters"; exit 1; }

start_app eureka-provider http://localhost:18032/hello || exit 1
start_app nacos-cloud-consumer http://localhost:18035/hello $(nacos_app_env) || exit 1
start_app consul-provider-cloud http://localhost:18089/hello || exit 1
start_app nacos-provider-cloud http://localhost:18088/hello $(nacos_app_env) || exit 1
start_app zookeeper-provider "log:is ready" SPRING_APPLICATION_JSON="$(dubbo_json zookeeper://127.0.0.1:2181)" || exit 1
start_app nacos-provider "log:is ready" SPRING_APPLICATION_JSON="$(dubbo_json "$(dubbo_nacos_url)")" || exit 1
start_app "nacos-provider-cloud#2" http://localhost:18098/hello $(nacos_app_env) SERVER_PORT=18098 \
  SPRING_CLOUD_NACOS_DISCOVERY_GROUP=$GA || exit 1
start_app "nacos-cloud-consumer#2" http://localhost:18045/hello $(nacos_app_env) SERVER_PORT=18045 \
  MANAGEMENT_SERVER_PORT=32012 SPRING_CLOUD_NACOS_DISCOVERY_GROUP=$GA || exit 1
nacos_register_persistent $NACOS_A $PS 10.31.0.1 31001 >/dev/null
nacos_register_persistent $NACOS_A $PS 10.31.0.2 31002 >/dev/null

OLD_TASKS=(
  "$(on old add_task "$EUREKA_ID" "$NACOS_A_ID" $EP)"
  "$(on old add_task "$NACOS_A_ID" "$EUREKA_ID" $NC)"
  "$(on old add_task "$CONSUL_ID" "$NACOS_A_ID" $CP)"
  "$(on old add_task "$NACOS_A_ID" "$CONSUL_ID" $NP)"
  "$(on old add_task "$ZK_ID" "$NACOS_A_ID" $IFACE zk 1.0.0)"
  "$(on old add_task "$NACOS_A_ID" "$ZK_ID" "$NACOS_SVC" DEFAULT_GROUP)"
  "$(on old add_task "$NACOS_A_ID" "$NACOS_B_ID" $NP)"
  "$(on old add_task "$NACOS_A_ID" "$NACOS_B_ID" $PS)"
  "$(on old add_task "$NACOS_A_ID" "$NACOS_B_ID" ALL $GA)"
)
check "$OLD_REF accepted all 9 tasks (Eureka, Consul, ZK both ways; Nacos A->B single, persistent, ALL)" \
  test "$(printf '%s\n' "${OLD_TASKS[@]}" | grep -c .)" -eq 9
converge 180 "$OLD_REF baseline: every task synced exactly once"

# ================================================================ 1. single-node swap 0.5.0 -> new
SCENARIO=upgrade-swap
sample_start swap; sleep 5
T0=$(now_ms)
ucompose stop nacos-sync-old >/dev/null 2>&1
record INFO "$OLD_REF stopped (SIGTERM) in $(since "$T0")s"
((SWAP_WAIT)) && { record INFO "waiting ${SWAP_WAIT}s before starting the new version (SWAP_WAIT)"; sleep "$SWAP_WAIT"; }
T_EPOCH=$(date +%s)
if s=$(start_new); then record PASS "new version started on the $OLD_REF database (API up ${s}s after start, $(since "$T0")s after T0)"
else record FAIL "new version did not start on the $OLD_REF database"; exit 1; fi
check_out "new version: no schema/DDL errors on the $OLD_REF schema (ddl-auto=update)" no_schema_errors nacos-sync "$T_EPOCH"
check "new version lists exactly the 9 tasks created by $OLD_REF" on new tasks_loaded "${OLD_TASKS[@]}"
converge 360 "new version re-synced every task created by $OLD_REF"
wait_until $((T0 + (SWAP_WAIT + 60) * 1000))   # copies 0.5.0 registered over v1 HTTP expire 30 s after its last beat
stays 30 "after the swap: every synced copy present exactly once, nothing left over from $OLD_REF" synced_once
sample_stop
gap_report swap "Case 1: single-node swap $OLD_REF -> new, new started ${SWAP_WAIT}s after the stop (T0 = $OLD_REF stop)" "$T0"
gaps_info swap "swap"
check_out "swap: every synced copy back within ${GAP_MAX}s" gaps_within swap
if o=$(no_dups swap); then record INFO "swap: never more copies than expected during the transition"
else record INFO "swap: transient extra copies during the transition: $(echo $o)"; fi
schema_dump "$UP_DIR/schema-new.sql"
if diff -u "$UP_DIR/schema-old.sql" "$UP_DIR/schema-new.sql" > "$UP_DIR/schema-new.diff"; then
  record PASS "schema: the new version left the $OLD_REF tables unchanged"
else record INFO "schema: the new version changed the $OLD_REF tables, see ${UP_DIR#$E2E_DIR/}/schema-new.diff"; fi

# ================================================================ 4. rollback new -> 0.5.0 on the same database
SCENARIO=upgrade-rollback
T_NEW=$(on new add_task "$NACOS_A_ID" "$NACOS_B_ID" $NC)   # a task the old release has never seen
PROBES+=(created-by-new)
converge 60 "new version: task created after the upgrade synced"
sample_start rollback; sleep 5
T0=$(now_ms)
compose stop nacos-sync >/dev/null 2>&1
record INFO "new version stopped (SIGTERM) in $(since "$T0")s"
T_EPOCH=$(date +%s)
if s=$(start_old); then record PASS "$OLD_REF started on the database the new version used (API up ${s}s after start, $(since "$T0")s after T0)"
else record FAIL "$OLD_REF did not start after the rollback"; exit 1; fi
check_out "$OLD_REF: no schema/DDL errors after the new version ran" no_schema_errors nacos-sync-old "$T_EPOCH"
check "$OLD_REF lists all 10 tasks, including the one created by the new version" on old tasks_loaded "${OLD_TASKS[@]}" "$T_NEW"
converge 360 "$OLD_REF re-synced every task after the rollback"
wait_until $((T0 + 60000))
stays 30 "after the rollback: every synced copy present exactly once" synced_once
sample_stop
gap_report rollback "Case 4: rollback new -> $OLD_REF (T0 = new version stop)" "$T0"
gaps_info rollback "rollback"
check_out "rollback: every synced copy back within ${GAP_MAX}s" gaps_within rollback
if o=$(no_dups rollback); then record INFO "rollback: never more copies than expected during the transition"
else record INFO "rollback: transient extra copies during the transition: $(echo $o)"; fi
schema_dump "$UP_DIR/schema-rollback.sql"
diff -q "$UP_DIR/schema-new.sql" "$UP_DIR/schema-rollback.sql" >/dev/null ||
  record INFO "schema: $OLD_REF changed the tables again after the rollback"

# ================================================================ 2. overlap: both versions on one database
SCENARIO=upgrade-overlap
sample_start overlap
T0=$(now_ms)
if s=$(start_new); then record PASS "new version started next to a running $OLD_REF (API up ${s}s)"
else record FAIL "new version did not start next to $OLD_REF"; exit 1; fi
T_READY=$(now_ms)
sleep "$OVERLAP"
T_END=$(now_ms)
for d in "$NACOS_A $EP DEFAULT_GROUP" "$NACOS_A $CP DEFAULT_GROUP" "$NACOS_A $ZK_SVC zk" "$NACOS_B $NP DEFAULT_GROUP"; do
  o=$(dup_diff $d); [ -n "$o" ] && record INFO "overlap: Nacos lists ${d#* } twice for one ip:port; the copies differ in: $o"
done
sample_stop
gap_report overlap "Case 2a: both versions running for ${OVERLAP}s (T0 = new version start)" "$T0" "$T_READY" "$T_END"
check_out "overlap: no synced copy went missing (no flapping) while both versions ran ${OVERLAP}s" no_flaps overlap
# Each version registers its own copy of every ip:port, so Nacos lists it twice until one of them stops: the
# trade-off of starting the new version first (UPGRADE.md, way B). Recorded, not checked.
if o=$(no_dups overlap); then record INFO "overlap: never more copies than expected while both versions ran"
else record INFO "overlap: copies listed twice while both versions ran, as UPGRADE.md describes: $(echo $o)"; fi
record INFO "overlap: ERROR lines logged while both ran: new $(compose logs --no-log-prefix --since "$(((T0 / 1000)))" nacos-sync 2>&1 | grep -c ' ERROR '), $OLD_REF $(ucompose logs --no-log-prefix --since "$(((T0 / 1000)))" nacos-sync-old 2>&1 | grep -c ' ERROR ')"

# A task deleted on one node during the overlap: the other node never learns about it (it only reads the rows
# that exist), so it keeps running the task from memory.
T_EPOCH=$(date +%s)
on new delete_task "$T_NEW"
sleep 30
o=$(compose logs --no-log-prefix --since "$T_EPOCH" nacos-sync 2>&1 | grep -A1 'delete task from nacos to nacos was failed' | tail -1 | plain 200)
[ -n "$o" ] && record INFO "overlap: the new version failed to delete the task's copies: $o"
read -r h n <<<"$(probe created-by-new)"
record INFO "overlap: task deleted on the new version while $OLD_REF runs: $n copies in Nacos B 30s later"
PROBES=("${PROBES[@]/created-by-new/}"); PROBES=(${PROBES[@]})

sample_start overlap-stop; sleep 5
T0=$(now_ms)
ucompose stop nacos-sync-old >/dev/null 2>&1
converge 360 "overlap ended ($OLD_REF stopped): the new version keeps every task synced"
eventually 90 "overlap ended: copy of the task deleted during the overlap is gone" copies_gone created-by-new
wait_until $((T0 + 60000))
stays 30 "overlap ended: every synced copy present exactly once" synced_once
sample_stop
gap_report overlap-stop "Case 2b: $OLD_REF leaves after the overlap (T0 = $OLD_REF stop)" "$T0"
gaps_info overlap-stop "overlap ended"
check_out "overlap ended: every synced copy back within ${GAP_MAX}s" gaps_within overlap-stop

# ================================================================ 3. the new version cleans up what 0.5.0 registered
# The persistent copies in Nacos B were registered by 0.5.0 during setup; nothing re-registered them since.
SCENARIO=upgrade-cleanup
for t in "${OLD_TASKS[@]}"; do on new delete_task "$t" || record FAIL "delete task $t on the new version"; done
for p in "${PROBES[@]}"; do eventually 120 "task delete on the new version: $p copies removed" copies_gone "$p"; done
check "no task rows left" test -z "$(on new task_ids)"
check "native Eureka instance untouched" eureka_has_port $EP 18032
check "native Consul instance untouched" consul_has_port $CP 18089
check "native Zookeeper provider untouched" zk_native_ok
check "native Nacos instances untouched" nacos_native_ok
for a in eureka-provider nacos-cloud-consumer consul-provider-cloud zookeeper-provider nacos-provider \
  "nacos-provider-cloud#2" "nacos-cloud-consumer#2"; do stop_app "$a"; done

# ================================================================ 5. 0.5.0 against Nacos 3.2.4 with auth
SCENARIO=upgrade-nacos3
compose stop nacos-sync >/dev/null 2>&1   # only the old release may run these tasks
ucompose up -d --wait --wait-timeout 300 nacos-v3 >/dev/null 2>&1 || { record FAIL "Nacos v3.2.4 did not start"; exit 1; }
nacos_init_admin $V3_PORT; rm -f "$OUT_DIR"/cache/token-*
V3_SVC=e2e-upgrade-v3
v3 nacos_register_persistent $V3_PORT $V3_SVC 10.32.0.1 32001 >/dev/null
check "Nacos 3.2.4 source instance registered" v3 nacos_count $V3_PORT $V3_SVC 1
T_EPOCH=$(date +%s)
s=$(start_old) || { record FAIL "$OLD_REF did not start"; exit 1; }
V3_ID=$(on old add_cluster nacos-v3 NACOS nacos-v3:8848)
T_FROM=$(on old add_task "$V3_ID" "$NACOS_B_ID" $V3_SVC)
T_TO=$(on old add_task "$NACOS_A_ID" "$V3_ID" $NP)
check "$OLD_REF accepts a Nacos 3.2.4 cluster and tasks from/to it" test -n "$V3_ID" -a -n "$T_FROM" -a -n "$T_TO"
sleep 60
from=FAILED; nacos_synced_from $NACOS_B $V3_SVC 32001 "$V3_ID" && from=WORKS
to=FAILED; v3 nacos_synced_from $V3_PORT $NP 18088 "$NACOS_A_ID" && to=WORKS
record INFO "$OLD_REF with Nacos 3.2.4 as source: sync $from after 60s"
record INFO "$OLD_REF with Nacos 3.2.4 as destination: sync $to after 60s"
cause=$(ucompose logs --no-log-prefix --since "$T_EPOCH" nacos-sync-old 2>&1 | grep -iE 'nacos-v3|8848' |
  grep -iE 'error|fail|exception|code' | sed -E 's/^[0-9: .-]+\[[^]]*\] //' | cut -c1-220 | sort | uniq -c | sort -rn | head -3)
[ -n "$cause" ] && record INFO "$OLD_REF errors talking to Nacos 3.2.4: $(echo "$cause" | plain)"
check "compatibility matrix: $OLD_REF cannot sync from or to Nacos 3.2.4 (the documented assumption holds)" \
  test "$from$to" = FAILEDFAILED
on old delete_task "$T_FROM"; on old delete_task "$T_TO"
on old sync_api DELETE "/v1/cluster/delete?clusterId=$V3_ID" >/dev/null
for i in 1 2; do   # persistent sources outlive their registrations
  curl -s -X DELETE "localhost:$NACOS_A/nacos/v1/ns/instance" --data-urlencode "serviceName=$PS" --data-urlencode "ip=10.31.0.$i" \
    --data-urlencode "port=3100$i" --data-urlencode "ephemeral=false" --data-urlencode "accessToken=$(nacos_token $NACOS_A)" >/dev/null
done
stop_app nacos-provider-cloud
