#!/usr/bin/env bash
# Shared helpers for the nacos-sync E2E scenarios. Source it, do not run it.
# Requires: docker compose, curl, jq, a JDK 17 `java` (JAVA_HOME is honoured).

E2E_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT_DIR=${OUT_DIR:-$E2E_DIR/out}
# The example applications, extracted from nacos-sync-example by run.sh.
EXAMPLES_DIR=$OUT_DIR/examples-src
mkdir -p "$OUT_DIR/logs" "$OUT_DIR/pids" "$OUT_DIR/cache"
[ -f "$E2E_DIR/.env" ] || cp "$E2E_DIR/.env.example" "$E2E_DIR/.env"
# .env only provides defaults: variables already exported (e.g. by run.sh -n/-s) win.
while IFS='=' read -r _k _v; do
  [[ $_k =~ ^[A-Z_][A-Z0-9_]*$ ]] || continue
  [ -n "${!_k+x}" ] || export "$_k=$_v"
done < "$E2E_DIR/.env"
unset _k _v
# Nacos A and B run the same tag unless NACOS_A_VERSION / NACOS_B_VERSION differ, e.g. to test 2.x -> 3.x sync.
export NACOS_A_VERSION=${NACOS_A_VERSION:-$NACOS_VERSION} NACOS_B_VERSION=${NACOS_B_VERSION:-$NACOS_VERSION}

JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
SYNC_URL=${SYNC_URL:-http://localhost:8083}
EUREKA_URL=http://localhost:18033/eureka
CONSUL_URL=http://localhost:8900
NACOS_A=8848
NACOS_B=18848
RESULTS=${RESULTS:-$OUT_DIR/results.tsv}
RUN_LABEL=${RUN_LABEL:-nacos=${NACOS_VERSION} sc=${SC_TRAIN}}
SCENARIO=${SCENARIO:-adhoc}
# Default convergence timeout in seconds. Eureka/Consul sources are polled every 3 s.
WAIT=${WAIT:-60}

compose() { docker compose -f "$E2E_DIR/docker-compose.yml" --env-file "$E2E_DIR/.env" "$@"; }
# Bound every request, so that a registry that stops answering fails a check instead of stalling the run.
# A --max-time given by the caller comes later on the command line and wins.
curl() { command curl --max-time 30 "$@"; }
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# ---------------------------------------------------------------- results
record() { # record PASS|FAIL|INFO <description>
  printf '%s\t%s\t%s\t%s\n' "$RUN_LABEL" "$SCENARIO" "$1" "$2" >> "$RESULTS"
  log "$1 [$SCENARIO] $2"
}

check() { # check <description> <command...>: run once
  local desc=$1; shift
  if "$@" >/dev/null 2>&1; then record PASS "$desc"; else record FAIL "$desc"; return 1; fi
}

eventually() { # eventually <timeout> <description> <command...>: poll every 2 s until it succeeds
  local timeout=$1 desc=$2 start=$SECONDS; shift 2
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS - start >= timeout )); then record FAIL "$desc (not within ${timeout}s)"; return 1; fi
    sleep 2
  done
  record PASS "$desc ($((SECONDS - start))s)"
}

stays() { # stays <seconds> <description> <command...>: must keep succeeding for the whole period
  local duration=$1 desc=$2 start=$SECONDS; shift 2
  while (( SECONDS - start < duration )); do
    if ! "$@" >/dev/null 2>&1; then record FAIL "$desc (broke after $((SECONDS - start))s)"; return 1; fi
    sleep 2
  done
  record PASS "$desc (held ${duration}s)"
}

# ---------------------------------------------------------------- nacos-sync API
sync_api() { # sync_api <METHOD> <path> [json-body]: prints the response, fails unless .success
  local method=$1 path=$2 body=${3:-} resp
  if [ -n "$body" ]; then
    resp=$(curl -s -X "$method" -H 'Content-Type: application/json' -d "$body" "$SYNC_URL$path")
  else
    resp=$(curl -s -X "$method" "$SYNC_URL$path")
  fi
  if [ "$(jq -r '.success' <<<"$resp" 2>/dev/null)" != true ]; then
    log "nacos-sync $method $path failed: ${resp:0:300}"
    return 1
  fi
  printf '%s' "$resp"
}

md5hex() { printf '%s' "$1" | { md5 -q 2>/dev/null || md5sum | cut -d' ' -f1; }; }

wait_sync_ready() { # nacos-sync answers its API
  local start=$SECONDS
  until sync_api GET '/v1/cluster/list?pageNum=1&pageSize=1' >/dev/null 2>&1; do
    (( SECONDS - start > 180 )) && { log "nacos-sync API not ready"; return 1; }
    sleep 2
  done
}

add_cluster() { # add_cluster <name> <NACOS|EUREKA|CONSUL|ZK> <connectKey[,connectKey]>: prints clusterId
  local name=$1 type=$2 keys=$3 id body
  id=$(md5hex "${name}_${type}")
  if sync_api GET '/v1/cluster/list?pageNum=1&pageSize=100' | jq -e --arg id "$id" 'any(.clusterModels[]; .clusterId==$id)' >/dev/null; then
    echo "$id"; return
  fi
  body=$(jq -nc --arg n "$name" --arg t "$type" --arg k "$keys" --arg u "$NACOS_USERNAME" --arg p "$NACOS_PASSWORD" \
    '{clusterName:$n, clusterType:$t, connectKeyList:($k|split(","))}
     + (if $t == "NACOS" then {userName:$u, password:$p} else {} end)')
  sync_api POST /v1/cluster/add "$body" >/dev/null && echo "$id"
}

# An empty groupName is sent as is, as the console does when no group is entered; a missing one is DEFAULT_GROUP.
add_task() { # add_task <srcClusterId> <destClusterId> <serviceName> [groupName] [version]: prints taskId
  local src=$1 dst=$2 svc=$3 grp=${4-DEFAULT_GROUP} ver=${5:-}
  sync_api POST /v1/task/add "$(jq -nc --arg s "$src" --arg d "$dst" --arg n "$svc" --arg g "$grp" --arg v "$ver" \
    '{sourceClusterId:$s, destClusterId:$d, serviceName:$n, groupName:$g} + (if $v == "" then {} else {version:$v} end)')" >/dev/null &&
    md5hex "${svc}_${grp}_${src}_${dst}"
}

set_task_status() { # set_task_status <taskId> <SYNC|DELETE>
  sync_api POST /v1/task/update "$(jq -nc --arg t "$1" --arg s "$2" '{taskId:$t, taskStatus:$s}')" >/dev/null
}

delete_task() { sync_api DELETE "/v1/task/delete?taskId=$1" >/dev/null; }

task_ids() { sync_api GET '/v1/task/list?pageNum=1&pageSize=1000' | jq -r '.taskModels[].taskId'; }

delete_all_tasks() {
  local id
  for id in $(task_ids); do delete_task "$id" || log "could not delete task $id"; done
}

# ---------------------------------------------------------------- Nacos
# Prints the body of a 200 answer, or <404 body> for a 404, which the registries give for a service they do not have.
# Anything else, e.g. a 403 or a registry that does not answer, prints nothing and fails: the predicates below then
# fail too, instead of reading the failed query as "no instances" and passing a check that something is gone.
registry_get() { # registry_get <404 body> <curl args...>
  local missing=$1 out code; shift
  out=$(curl -s -w '\n%{http_code}' "$@") || return 1
  code=${out##*$'\n'}; out=${out%$'\n'*}
  case $code in
    200) printf '%s' "$out" ;;
    404) printf '%s' "$missing" ;;
    *) return 1 ;;
  esac
}

nacos_init_admin() { # nacos_init_admin <port>: sets the admin password on a fresh 2.4+/3.x server
  local port=$1
  curl -s -X POST "localhost:$port/nacos/v3/auth/user/admin" --data-urlencode "password=$NACOS_PASSWORD" | grep -q '"code":0' && return
  curl -s -X POST "localhost:$port/nacos/v1/auth/users/admin" --data-urlencode "password=$NACOS_PASSWORD" >/dev/null
}

nacos_token() { # nacos_token <port>: cached for 10 minutes
  local port=$1 f="$OUT_DIR/cache/token-$1" tok
  if [ -s "$f" ] && [ -z "$(find "$f" -mmin +10 2>/dev/null)" ]; then cat "$f"; return; fi
  tok=$(curl -s -X POST "localhost:$port/nacos/v3/auth/user/login" \
    --data-urlencode "username=$NACOS_USERNAME" --data-urlencode "password=$NACOS_PASSWORD" | jq -r '.accessToken // empty' 2>/dev/null)
  [ -z "$tok" ] && tok=$(curl -s -X POST "localhost:$port/nacos/v1/auth/users/login" \
    --data-urlencode "username=$NACOS_USERNAME" --data-urlencode "password=$NACOS_PASSWORD" | jq -r '.accessToken // empty' 2>/dev/null)
  printf '%s' "$tok" > "$f"; printf '%s' "$tok"
}

nacos_is_v3() { # nacos_is_v3 [port]: whether the Nacos on that port (default: NACOS_VERSION) runs 3.x
  local v=$NACOS_VERSION
  case ${1:-} in "$NACOS_A") v=$NACOS_A_VERSION ;; "$NACOS_B") v=$NACOS_B_VERSION ;; esac
  [[ $v == v3* ]]
}

nacos_instances() { # nacos_instances <port> <service> [group]: JSON array of {ip,port,healthy,enabled,metadata}
  local port=$1 svc=$2 grp=${3:-DEFAULT_GROUP} tok resp
  tok=$(nacos_token "$port")
  if nacos_is_v3 "$port"; then
    resp=$(registry_get '{"data": []}' -G -H "accessToken: $tok" "localhost:$port/nacos/v3/admin/ns/instance/list" \
      --data-urlencode "serviceName=$svc" --data-urlencode "groupName=$grp" --data-urlencode "namespaceId=public") || return 1
    jq -c '[.data[] | {ip, port, healthy, enabled, metadata}]' <<<"$resp" 2>/dev/null
  else
    resp=$(registry_get '{"hosts": []}' -G "localhost:$port/nacos/v1/ns/instance/list" --data-urlencode "serviceName=$svc" \
      --data-urlencode "groupName=$grp" --data-urlencode "namespaceId=public" --data-urlencode "accessToken=$tok") || return 1
    jq -c '[.hosts[] | {ip, port, healthy, enabled, metadata}]' <<<"$resp" 2>/dev/null
  fi
}

nacos_services() { # nacos_services <port> [group]: JSON array of service names
  local port=$1 grp=${2:-DEFAULT_GROUP} tok
  tok=$(nacos_token "$port")
  if nacos_is_v3 "$port"; then
    registry_get '{"data": {"pageItems": []}}' -G -H "accessToken: $tok" "localhost:$port/nacos/v3/admin/ns/service/list" \
      --data-urlencode "namespaceId=public" --data-urlencode "groupNameParam=$grp" --data-urlencode "pageNo=1" \
      --data-urlencode "pageSize=1000" | jq -c '[.data.pageItems[] | .name]' 2>/dev/null
  else
    registry_get '{"doms": []}' -G "localhost:$port/nacos/v1/ns/service/list" --data-urlencode "groupName=$grp" \
      --data-urlencode "pageNo=1" --data-urlencode "pageSize=1000" --data-urlencode "accessToken=$tok" | jq -c '.doms' 2>/dev/null
  fi
}

nacos_set_instance() { # nacos_set_instance <port> <service> <ip> <port> <enabled true|false> [metadataJson]
  local nport=$1 svc=$2 ip=$3 iport=$4 enabled=$5 meta=${6:-} tok
  tok=$(nacos_token "$nport")
  local args=(--data-urlencode "serviceName=$svc" --data-urlencode "groupName=DEFAULT_GROUP" --data-urlencode "namespaceId=public"
    --data-urlencode "ip=$ip" --data-urlencode "port=$iport" --data-urlencode "enabled=$enabled" --data-urlencode "ephemeral=true"
    --data-urlencode "clusterName=DEFAULT" --data-urlencode "weight=1")
  [ -n "$meta" ] && args+=(--data-urlencode "metadata=$meta")
  if nacos_is_v3 "$nport"; then
    curl -s -X PUT -H "accessToken: $tok" "localhost:$nport/nacos/v3/admin/ns/instance" "${args[@]}"
  else
    curl -s -X PUT "localhost:$nport/nacos/v1/ns/instance" "${args[@]}" --data-urlencode "accessToken=$tok"
  fi
}

# ---------------------------------------------------------------- Eureka / Consul / Zookeeper
eureka_instances() { # eureka_instances <app>: JSON array of {ip,port,status,metadata}
  local app
  app=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
  # -s: Eureka 1.x (Spring Cloud 2021.0) answers an app without instances with an empty 404 body, which plain jq
  # turns into no output at all instead of [].
  registry_get '' -H 'Accept: application/json' "$EUREKA_URL/apps/$app" |
    jq -cs '[(.[0].application.instance // []) | (if type == "array" then .[] else . end)
            | {id: .instanceId, ip: .ipAddr, port: (.port["$"] | tonumber), status, metadata: (.metadata // {})}]' 2>/dev/null
}

eureka_set_status() { # eureka_set_status <app> <instanceId> <UP|OUT_OF_SERVICE|DOWN>
  local app; app=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
  curl -s -o /dev/null -w '%{http_code}' -X PUT "$EUREKA_URL/apps/$app/$2/status?value=$3"
}

eureka_set_metadata() { # eureka_set_metadata <app> <instanceId> <key> <value>
  local app; app=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
  curl -s -o /dev/null -w '%{http_code}' -X PUT "$EUREKA_URL/apps/$app/$2/metadata?$3=$4"
}

consul_instances() { # consul_instances <service>: JSON array of {id,ip,port,passing,tags}
  registry_get '[]' "$CONSUL_URL/v1/health/service/$1" |
    jq -c '[.[] | {id: .Service.ID, ip: .Service.Address, port: .Service.Port, tags: .Service.Tags,
                   passing: all(.Checks[]; .Status == "passing")}]' 2>/dev/null
}

zk_providers() { # zk_providers <dubbo interface>: provider URLs (decoded), one per line; fails if ZooKeeper cannot be asked
  local out list
  out=$(compose exec -T zookeeper zkCli.sh -server localhost:2181 ls "/dubbo/$1/providers" 2>&1)
  grep -q 'Node does not exist' <<<"$out" && return 0   # no provider has registered yet
  # zkCli also logs to stdout; the child list is the line starting with "[".
  list=$(grep '^\[' <<<"$out" | tail -1) && [ -n "$list" ] || return 1
  tr -d '[]' <<<"$list" | tr ',' '\n' | sed 's/^ *//' | grep -v '^$' |
    python3 -c 'import sys,urllib.parse; [print(urllib.parse.unquote(l.strip())) for l in sys.stdin]'
}

# ---------------------------------------------------------------- predicates (for check/eventually/stays)
count_eq() { [ "$(jq length)" -eq "$1" ]; }                            # stdin JSON array
has_port() { jq -e --argjson p "$1" 'any(.[]; .port == $p)' >/dev/null; } # stdin JSON array
lacks_port() { jq -e --argjson p "$1" 'all(.[]; .port != $p)' >/dev/null; }

nacos_count() { nacos_instances "$1" "$2" "${4:-DEFAULT_GROUP}" | count_eq "$3"; }
nacos_has_port() { nacos_instances "$1" "$2" | has_port "$3"; }
nacos_lacks_port() { nacos_instances "$1" "$2" | lacks_port "$3"; }
nacos_synced_from() { # nacos_synced_from <port> <service> <instancePort> <sourceClusterId>
  nacos_instances "$1" "$2" | jq -e --argjson p "$3" --arg c "$4" 'any(.[]; .port == $p and .metadata.sourceClusterId == $c)' >/dev/null
}
nacos_meta_is() { # nacos_meta_is <port> <service> <instancePort> <key> <value>
  nacos_instances "$1" "$2" | jq -e --argjson p "$3" --arg k "$4" --arg v "$5" 'any(.[]; .port == $p and .metadata[$k] == $v)' >/dev/null
}
eureka_count() { eureka_instances "$1" | count_eq "$2"; }
eureka_has_port() { eureka_instances "$1" | has_port "$2"; }
eureka_lacks_port() { eureka_instances "$1" | lacks_port "$2"; }
eureka_synced_from() {
  eureka_instances "$1" | jq -e --argjson p "$2" --arg c "$3" 'any(.[]; .port == $p and .metadata.sourceClusterId == $c)' >/dev/null
}
eureka_meta_is() {
  eureka_instances "$1" | jq -e --argjson p "$2" --arg k "$3" --arg v "$4" 'any(.[]; .port == $p and .metadata[$k] == $v)' >/dev/null
}
consul_count() { consul_instances "$1" | count_eq "$2"; }
consul_has_port() { consul_instances "$1" | has_port "$2"; }
consul_lacks_port() { consul_instances "$1" | lacks_port "$2"; }
http_ok() { [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$1")" = 200 ]; }
http_body_has() { curl -s --max-time 5 "$1" | grep -q "$2"; }

# ---------------------------------------------------------------- example applications on the host
app_jar() { # app_jar <name>
  local j
  j=$(ls "$EXAMPLES_DIR"/*/"$1"/target/"$1"-*.jar 2>/dev/null | grep -v original | head -1)
  [ -n "$j" ] || { log "jar for $1 not found, build the examples first"; return 1; }
  echo "$j"
}

nacos_app_env() { # Spring Cloud Alibaba discovery settings for Nacos cluster A
  echo SPRING_CLOUD_NACOS_DISCOVERY_SERVER_ADDR=127.0.0.1:$NACOS_A \
    SPRING_CLOUD_NACOS_DISCOVERY_USERNAME="$NACOS_USERNAME" SPRING_CLOUD_NACOS_DISCOVERY_PASSWORD="$NACOS_PASSWORD"
}

dubbo_nacos_url() { echo "nacos://127.0.0.1:$NACOS_A?username=$NACOS_USERNAME&password=$NACOS_PASSWORD"; }

start_app() { # start_app <name>[#n] <ready-url|log:pattern> [ENV=VALUE | --spring.arg=value ...]
  local name=$1 ready=$2 jar pid start=$SECONDS a; shift 2   # (#n starts another instance of the same app)
  local envs=() args=()
  for a in "$@"; do case $a in --*) args+=("$a") ;; *) envs+=("$a") ;; esac; done
  jar=$(app_jar "${name%%#*}") || return 1
  env "${envs[@]}" nohup "$JAVA" -Xms64m -Xmx256m -jar "$jar" "${args[@]}" > "$OUT_DIR/logs/$name.log" 2>&1 &
  pid=$!; echo "$pid" > "$OUT_DIR/pids/$name"
  until { case $ready in
            log:*) grep -q "${ready#log:}" "$OUT_DIR/logs/$name.log" ;;
            *) http_ok "$ready" ;;
          esac; }; do
    if ! kill -0 "$pid" 2>/dev/null; then log "$name exited during startup, see $OUT_DIR/logs/$name.log"; return 1; fi
    (( SECONDS - start > 180 )) && { log "$name not ready after 180s"; return 1; }
    sleep 2
  done
  log "started $name (pid $pid, $((SECONDS - start))s)"
}

stop_app() { # stop_app <name> [TERM|KILL]: TERM lets the app deregister itself, KILL does not
  local name=$1 sig=${2:-TERM} f="$OUT_DIR/pids/$1" pid start=$SECONDS
  [ -f "$f" ] || return 0
  pid=$(cat "$f"); kill "-$sig" "$pid" 2>/dev/null
  while kill -0 "$pid" 2>/dev/null; do
    (( SECONDS - start > 40 )) && { kill -KILL "$pid" 2>/dev/null; break; }
    sleep 1
  done
  rm -f "$f"
}

stop_all_apps() { local f; for f in "$OUT_DIR"/pids/*; do [ -e "$f" ] && stop_app "$(basename "$f")"; done; }

# ---------------------------------------------------------------- load generators for the batch scenario
start_nacos_load() { # start_nacos_load <name> <nacosPort> <servicePrefix> <services> <instances>
  local name=$1 jar="$E2E_DIR/tools/nacos-load/target/nacos-load-1.0-SNAPSHOT.jar" start=$SECONDS
  [ -f "$jar" ] || (cd "$E2E_DIR/tools/nacos-load" && mvn -B -q package) || return 1
  nohup "$JAVA" -Xmx256m -jar "$jar" "127.0.0.1:$2" "$3" "$4" "$5" > "$OUT_DIR/logs/$name.log" 2>&1 &
  echo $! > "$OUT_DIR/pids/$name"
  until grep -q REGISTERED "$OUT_DIR/logs/$name.log"; do
    (( SECONDS - start > 120 )) && { log "$name did not register"; return 1; }
    sleep 1
  done
}

eureka_register() { # eureka_register <app> <ip> <port>: raw REST registration, instanceId <ip>:<app>:<port>
  local app; app=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
  curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' "$EUREKA_URL/apps/$app" -d "$(jq -nc \
    --arg app "$app" --arg ip "$2" --argjson port "$3" '{instance: {instanceId: "\($ip):\($app):\($port)", hostName: $ip,
      app: $app, ipAddr: $ip, status: "UP", port: {"$": $port, "@enabled": "true"}, vipAddress: ($app | ascii_downcase),
      dataCenterInfo: {"@class": "com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo", name: "MyOwn"},
      leaseInfo: {renewalIntervalInSecs: 30, durationInSecs: 90}, metadata: {"e2e-load": "eureka"}}}')"
}

start_eureka_load() { # start_eureka_load <name> <appPrefix> <apps> <instances>: registers and keeps heartbeating
  local name=$1 prefix=$2 apps=$3 inst=$4 a i
  for ((a = 0; a < apps; a++)); do for ((i = 1; i <= inst; i++)); do
    eureka_register "$prefix-$a" "10.20.$a.$i" $((21000 + i)) >/dev/null
  done; done
  ( while sleep 20; do for ((a = 0; a < apps; a++)); do for ((i = 1; i <= inst; i++)); do
      local app; app=$(printf '%s' "$prefix-$a" | tr '[:lower:]' '[:upper:]')
      curl -s -o /dev/null -X PUT "$EUREKA_URL/apps/$app/10.20.$a.$i:$app:$((21000 + i))?status=UP"
    done; done; done ) > /dev/null 2>&1 &
  echo $! > "$OUT_DIR/pids/$name"
}

eureka_deregister_all() { # eureka_deregister_all <appPrefix> <apps> <instances>
  local a i app
  for ((a = 0; a < $2; a++)); do app=$(printf '%s' "$1-$a" | tr '[:lower:]' '[:upper:]')
    for ((i = 1; i <= $3; i++)); do curl -s -o /dev/null -X DELETE "$EUREKA_URL/apps/$app/10.20.$a.$i:$app:$((21000 + i))"; done
  done
}

# ---------------------------------------------------------------- environment
init_registries() { # admin passwords and nacos-sync readiness after `compose up`
  nacos_init_admin "$NACOS_A"; nacos_init_admin "$NACOS_B"
  rm -f "$OUT_DIR"/cache/token-*
  wait_sync_ready
}

setup_clusters() { # exports cluster ids used by the scenarios
  NACOS_A_ID=$(add_cluster nacos-a NACOS nacos-a:8848) &&
  NACOS_B_ID=$(add_cluster nacos-b NACOS nacos-b:8848) &&
  EUREKA_ID=$(add_cluster eureka EUREKA http://eureka:18033/eureka) &&
  CONSUL_ID=$(add_cluster consul CONSUL consul:8500) &&
  ZK_ID=$(add_cluster zookeeper ZK zookeeper:2181) || return 1
  export NACOS_A_ID NACOS_B_ID EUREKA_ID CONSUL_ID ZK_ID
}
