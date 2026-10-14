#!/usr/bin/env bash
# Dubbo over Zookeeper <-> Nacos: one-way in both directions, bidirectional loop prevention,
# offline handling and real Dubbo calls through the synced registry.
. "$(dirname "$0")/../lib.sh"
SCENARIO=zk-nacos
setup_clusters || exit 1

IFACE=com.paderlol.nacos.api.DemoService
ZK_SVC="providers:$IFACE:1.0.0:zk"        # how ZK -> Nacos names the zookeeper-provider service (group zk)
NACOS_SVC="providers:$IFACE:1.0.0:nacos"  # nacos-provider registers this in Nacos A, DEFAULT_GROUP
# The example main classes do not pass args to SpringApplication.run, and Dubbo binds dubbo.* by prefix, which
# misses plain env overrides. SPRING_APPLICATION_JSON gives a real dotted property source that wins over the yaml.
dubbo_json() { jq -nc --arg a "$1" '{dubbo: {registry: {address: $a}, "metadata-report": {address: $a}, application: {"qos-enable": false}}}'; }
ZK_ARGS=(SPRING_APPLICATION_JSON="$(dubbo_json zookeeper://127.0.0.1:2181)")
NACOS_ARGS=(SPRING_APPLICATION_JSON="$(dubbo_json "$(dubbo_nacos_url)")")
zk_count() { [ "$(zk_providers $IFACE | grep -c "group=$1")" -eq "$2" ]; }
zk_synced() { zk_providers $IFACE | grep "group=$1" | grep -q "sourceClusterId=$2"; }

start_app zookeeper-provider "log:is ready" "${ZK_ARGS[@]}" || exit 1
start_app nacos-provider "log:is ready" "${NACOS_ARGS[@]}" || exit 1
check "zookeeper-provider registered in Zookeeper" zk_count zk 1

# --- one-way, both directions. For a ZK source the task group is the Dubbo group and the Nacos group.
T_ZN=$(add_task "$ZK_ID" "$NACOS_A_ID" $IFACE zk 1.0.0)
T_NZ=$(add_task "$NACOS_A_ID" "$ZK_ID" "$NACOS_SVC" DEFAULT_GROUP)
eventually $WAIT "zk->nacos: Dubbo provider registered in Nacos (group zk)" bash -c ". '$E2E_DIR/lib.sh'; nacos_instances $NACOS_A '$ZK_SVC' zk | jq -e 'length == 1 and .[0].metadata.sourceClusterId == \"$ZK_ID\"'"
eventually $WAIT "nacos->zk: Dubbo provider written to Zookeeper with sync metadata" zk_synced nacos "$NACOS_A_ID"

start_app nacos-consumer "log:Started " "${NACOS_ARGS[@]}" || exit 1   # its yaml sets dubbo.registry.group=zk
start_app zookeeper-consumer "log:Started " "${ZK_ARGS[@]}" || exit 1
eventually 120 "call path Nacos consumer -> ZK provider through Nacos" http_body_has http://localhost:18080/sayHello "Zookeeper"
eventually 120 "call path ZK consumer -> Nacos provider through ZK" http_body_has http://localhost:18081/sayHello "Nacos"

# --- several providers of one Dubbo service (Dubbo picks a free port, so the second gets another one)
start_app "zookeeper-provider#2" "log:is ready" "${ZK_ARGS[@]}"
eventually $WAIT "zk: two group=zk providers in Zookeeper" zk_count zk 2
eventually $WAIT "zk->nacos: both providers synced to Nacos" nacos_count $NACOS_A "$ZK_SVC" 2 zk
stays 20 "zk->nacos: both providers stay in Nacos" nacos_count $NACOS_A "$ZK_SVC" 2 zk
stop_app "zookeeper-provider#2" TERM
eventually $WAIT "zk->nacos: stopped provider removed, the other kept" nacos_count $NACOS_A "$ZK_SVC" 1 zk

# --- deleting and re-adding a task without any source change must sync again
delete_task "$T_NZ"
eventually $WAIT "nacos->zk: task delete removes the synced provider" zk_count nacos 0
T_NZ=$(add_task "$NACOS_A_ID" "$ZK_ID" "$NACOS_SVC" DEFAULT_GROUP)
eventually $WAIT "nacos->zk: re-added task syncs the unchanged provider again" zk_count nacos 1

# --- bidirectional
T_NZ2=$(add_task "$NACOS_A_ID" "$ZK_ID" "$ZK_SVC" zk)
T_ZN2=$(add_task "$ZK_ID" "$NACOS_A_ID" $IFACE nacos 1.0.0)
sleep 10
stays 30 "bidirectional: Zookeeper keeps exactly 1 group=zk provider (no echo from Nacos)" zk_count zk 1
check "bidirectional: Nacos keeps exactly 1 $NACOS_SVC (no echo from ZK)" nacos_count $NACOS_A "$NACOS_SVC" 1

# --- offline
stop_app zookeeper-provider TERM
eventually $WAIT "zk->nacos: stopped provider removed from Nacos" nacos_count $NACOS_A "$ZK_SVC" 0 zk
stop_app nacos-provider TERM
eventually $WAIT "nacos->zk: stopped provider removed from Zookeeper" zk_count nacos 0
start_app zookeeper-provider "log:is ready" "${ZK_ARGS[@]}"
start_app nacos-provider "log:is ready" "${NACOS_ARGS[@]}"
eventually $WAIT "zk->nacos: restarted provider re-synced" nacos_count $NACOS_A "$ZK_SVC" 1 zk
eventually $WAIT "nacos->zk: restarted provider re-synced" zk_count nacos 1

# --- deleting the tasks cleans up the destinations
for t in "$T_ZN" "$T_NZ" "$T_NZ2" "$T_ZN2"; do delete_task "$t"; done
eventually $WAIT "task delete: synced provider removed from Nacos" nacos_count $NACOS_A "$ZK_SVC" 0 zk
eventually $WAIT "task delete: synced provider removed from Zookeeper" zk_count nacos 0
check "task delete: native ZK provider untouched" zk_count zk 1

stop_app nacos-consumer; stop_app zookeeper-consumer; stop_app zookeeper-provider; stop_app nacos-provider
