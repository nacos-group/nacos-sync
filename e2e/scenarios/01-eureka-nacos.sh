#!/usr/bin/env bash
# Eureka <-> Nacos: one-way in both directions, bidirectional loop prevention, metadata updates,
# offline handling (status, disable, graceful stop, kill -9) and real calls through the synced registry.
. "$(dirname "$0")/../lib.sh"
SCENARIO=eureka-nacos
setup_clusters || exit 1

EP=spring-cloud-eureka-provider   # registers in Eureka, port 18032, /hello-nacos calls NC through Eureka
NC=spring-cloud-nacos-consumer    # registers in Nacos A, port 18035, /hello-eureka calls EP through Nacos
start_app eureka-provider http://localhost:18032/hello || exit 1
start_app nacos-cloud-consumer http://localhost:18035/hello $(nacos_app_env) || exit 1
ep_id() { eureka_instances $EP | jq -r '.[] | select(.port == 18032) | .id'; }
nc_ip() { echo "$NC_IP"; }   # set once below: the Nacos 2.x v1 list API leaves disabled instances out

# --- one-way, both directions
T_EN=$(add_task "$EUREKA_ID" "$NACOS_A_ID" $EP)
T_NE=$(add_task "$NACOS_A_ID" "$EUREKA_ID" $NC)
eventually $WAIT "eureka->nacos: Eureka instance registered in Nacos with sync metadata" nacos_synced_from $NACOS_A $EP 18032 "$EUREKA_ID"
eventually $WAIT "nacos->eureka: Nacos instance registered in Eureka with sync metadata" eureka_synced_from $NC 18035 "$NACOS_A_ID"
check "nacos->eureka: synced Eureka instance is UP" bash -c ". '$E2E_DIR/lib.sh'; eureka_instances $NC | jq -e 'all(.[]; .status == \"UP\")'"
# Spring Cloud clients refresh their registry cache every 30 s, so allow two cycles.
eventually 120 "call path Nacos consumer -> Eureka provider through Nacos" http_body_has http://localhost:18035/hello-eureka "from Eureka"
eventually 120 "call path Eureka provider -> Nacos app through Eureka" http_body_has http://localhost:18032/hello-nacos "from Nacos"

# --- several instances of one service: each must be synced, and losing one must not drop the other
start_app "eureka-provider#2" http://localhost:18042/hello SERVER_PORT=18042
eventually $WAIT "eureka->nacos: both instances of $EP synced" nacos_count $NACOS_A $EP 2
start_app "nacos-cloud-consumer#2" http://localhost:18045/hello $(nacos_app_env) SERVER_PORT=18045 MANAGEMENT_SERVER_PORT=32012
eventually $WAIT "nacos->eureka: both instances of $NC synced" eureka_count $NC 2
stop_app "eureka-provider#2" TERM; stop_app "nacos-cloud-consumer#2" TERM
eventually $WAIT "eureka->nacos: second instance removed, first kept" nacos_count $NACOS_A $EP 1
eventually $WAIT "nacos->eureka: second instance removed, first kept" eureka_count $NC 1
check "eureka->nacos: remaining instance is the original one" nacos_has_port $NACOS_A $EP 18032
check "nacos->eureka: remaining instance is the original one" eureka_has_port $NC 18035

# --- bidirectional: reverse tasks for the same services must not copy synced instances back
T_NE2=$(add_task "$NACOS_A_ID" "$EUREKA_ID" $EP)
T_EN2=$(add_task "$EUREKA_ID" "$NACOS_A_ID" $NC)
sleep 10
stays 30 "bidirectional: Eureka keeps exactly 1 $EP (no echo from Nacos)" eureka_count $EP 1
stays 10 "bidirectional: Nacos keeps exactly 1 $NC (no echo from Eureka)" nacos_count $NACOS_A $NC 1
check "bidirectional: Nacos still has exactly 1 $EP" nacos_count $NACOS_A $EP 1
check "bidirectional: Eureka still has exactly 1 $NC" eureka_count $NC 1

# --- metadata updates
NC_IP=$(nacos_instances $NACOS_A $NC | jq -r '.[] | select(.port == 18035) | .ip')
check "Nacos app IP known before changing its instance" test -n "$NC_IP"
check "eureka metadata update accepted" test "$(eureka_set_metadata $EP "$(ep_id)" e2e-version v2)" = 200
eventually $WAIT "eureka->nacos: metadata update propagated" nacos_meta_is $NACOS_A $EP 18032 e2e-version v2
nacos_set_instance $NACOS_A $NC "$(nc_ip)" 18035 true '{"e2e-version":"v2","preserved.register.source":"SPRING_CLOUD","management.port":"32002"}' >/dev/null
eventually $WAIT "nacos->eureka: metadata update propagated" eureka_meta_is $NC 18035 e2e-version v2

# --- offline by status
check "eureka OUT_OF_SERVICE accepted" test "$(eureka_set_status $EP "$(ep_id)" OUT_OF_SERVICE)" = 200
eventually $WAIT "eureka->nacos: OUT_OF_SERVICE instance removed from Nacos" nacos_lacks_port $NACOS_A $EP 18032
eureka_set_status $EP "$(ep_id)" UP >/dev/null
eventually $WAIT "eureka->nacos: instance back in Nacos after UP" nacos_has_port $NACOS_A $EP 18032

nacos_set_instance $NACOS_A $NC "$(nc_ip)" 18035 false >/dev/null
eventually $WAIT "nacos->eureka: disabled Nacos instance removed from Eureka" eureka_lacks_port $NC 18035
nacos_set_instance $NACOS_A $NC "$(nc_ip)" 18035 true >/dev/null
eventually $WAIT "nacos->eureka: instance back in Eureka after enable" eureka_has_port $NC 18035

# --- offline by process exit
stop_app eureka-provider TERM
eventually $WAIT "eureka->nacos: graceful shutdown removes instance from Nacos" nacos_lacks_port $NACOS_A $EP 18032
start_app eureka-provider http://localhost:18032/hello
eventually $WAIT "eureka->nacos: restarted instance re-synced" nacos_has_port $NACOS_A $EP 18032

stop_app nacos-cloud-consumer KILL
eventually $WAIT "nacos->eureka: kill -9 of Nacos app removes instance from Eureka" eureka_lacks_port $NC 18035
start_app nacos-cloud-consumer http://localhost:18035/hello $(nacos_app_env)
eventually $WAIT "nacos->eureka: restarted Nacos app re-synced" eureka_has_port $NC 18035

stop_app eureka-provider KILL
# No deregister: Eureka only evicts after the lease expires (90 s, doubled by the known Eureka lease bug).
eventually 300 "eureka->nacos: kill -9 of Eureka app removed from Nacos after lease eviction" nacos_lacks_port $NACOS_A $EP 18032
start_app eureka-provider http://localhost:18032/hello

# --- deleting the tasks cleans up the destinations
for t in "$T_EN" "$T_NE" "$T_NE2" "$T_EN2"; do delete_task "$t"; done
eventually $WAIT "task delete: synced instance removed from Nacos" nacos_lacks_port $NACOS_A $EP 18032
eventually $WAIT "task delete: synced instance removed from Eureka" eureka_lacks_port $NC 18035
check "task delete: native Nacos instance untouched" nacos_has_port $NACOS_A $NC 18035
check "task delete: native Eureka instance untouched" eureka_has_port $EP 18032

# --- ALL lists the services of a Nacos source. From Eureka such a task used to be added and then never synced.
fails() { ! "$@"; }
has_task() { task_ids | grep -qx "$1"; }
check "ALL task from Eureka is rejected" fails add_task "$EUREKA_ID" "$NACOS_A_ID" ALL
check "rejected ALL task is not stored" fails has_task "$(md5hex "ALL_DEFAULT_GROUP_${EUREKA_ID}_${NACOS_A_ID}")"

stop_app eureka-provider; stop_app nacos-cloud-consumer
