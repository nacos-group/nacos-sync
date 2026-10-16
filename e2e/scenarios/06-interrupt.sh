#!/usr/bin/env bash
# Interruptions on a running Eureka <-> Nacos sync: nacos-sync restart, registry down/up,
# task delete/recreate and pause/resume, and a network partition between nacos-sync and the registries.
. "$(dirname "$0")/../lib.sh"
SCENARIO=interrupt
setup_clusters || exit 1

EP=spring-cloud-eureka-provider
NC=spring-cloud-nacos-consumer
start_app eureka-provider http://localhost:18032/hello || exit 1
start_app nacos-cloud-consumer http://localhost:18035/hello $(nacos_app_env) || exit 1
T_EN=$(add_task "$EUREKA_ID" "$NACOS_A_ID" $EP)
T_NE=$(add_task "$NACOS_A_ID" "$EUREKA_ID" $NC)
eventually $WAIT "baseline: eureka->nacos synced" nacos_has_port $NACOS_A $EP 18032
eventually $WAIT "baseline: nacos->eureka synced" eureka_has_port $NC 18035

# --- 1. nacos-sync restart, with source changes while it is down
compose stop nacos-sync >/dev/null 2>&1
if nacos_has_port $NACOS_A $EP 18032; then record INFO "nacos-sync down: synced instance still in Nacos"
else record INFO "nacos-sync down: synced (ephemeral) instance left Nacos with the nacos-sync connection"; fi
stop_app eureka-provider TERM
start_app "eureka-provider#2" http://localhost:18042/hello SERVER_PORT=18042
sleep 20
compose start nacos-sync >/dev/null 2>&1; wait_sync_ready
eventually 120 "restart: instance added while down is synced" nacos_has_port $NACOS_A $EP 18042
eventually 120 "restart: instance removed while down is not resurrected" nacos_lacks_port $NACOS_A $EP 18032
eventually 120 "restart: nacos->eureka restored" eureka_has_port $NC 18035
stop_app "eureka-provider#2" TERM
start_app eureka-provider http://localhost:18032/hello
eventually $WAIT "restart: back to baseline" nacos_has_port $NACOS_A $EP 18032

# --- 2a. destination/source Nacos goes down and comes back
compose stop nacos-a >/dev/null 2>&1; sleep 20
compose start nacos-a >/dev/null 2>&1
compose up -d --wait nacos-a >/dev/null 2>&1; rm -f "$OUT_DIR"/cache/token-*
eventually 180 "nacos restart: eureka->nacos instance restored in Nacos" nacos_has_port $NACOS_A $EP 18032
eventually 180 "nacos restart: native Nacos app re-registered" nacos_has_port $NACOS_A $NC 18035
eventually 180 "nacos restart: nacos->eureka instance present in Eureka" eureka_has_port $NC 18035
stays 30 "nacos restart: eureka->nacos stays synced" nacos_has_port $NACOS_A $EP 18032

# --- 2b. Eureka goes down and comes back (loses its whole registry)
compose stop eureka >/dev/null 2>&1; sleep 10
stays 20 "eureka down: Nacos keeps the last synced instance" nacos_has_port $NACOS_A $EP 18032
compose start eureka >/dev/null 2>&1; compose up -d --wait eureka >/dev/null 2>&1
eventually 180 "eureka restart: Eureka app re-registered and synced to Nacos" nacos_has_port $NACOS_A $EP 18032
eventually 180 "eureka restart: nacos-sync re-registers the Nacos instance in Eureka" eureka_has_port $NC 18035
stays 60 "eureka restart: Nacos instance stays in Eureka" eureka_has_port $NC 18035

# --- 3. task delete / recreate, pause / resume
delete_task "$T_EN"
eventually $WAIT "task delete: synced instance removed" nacos_lacks_port $NACOS_A $EP 18032
T_EN=$(add_task "$EUREKA_ID" "$NACOS_A_ID" $EP)
eventually $WAIT "task recreate: instance synced again" nacos_has_port $NACOS_A $EP 18032
set_task_status "$T_NE" DELETE
eventually $WAIT "task pause: synced instance removed from Eureka" eureka_lacks_port $NC 18035
set_task_status "$T_NE" SYNC
eventually $WAIT "task resume: instance back in Eureka" eureka_has_port $NC 18035
if sync_api DELETE "/v1/task/delete?taskId=does-not-exist" >/dev/null 2>&1; then
  record INFO "deleting an unknown task reports success"
else record INFO "deleting an unknown task reports failure (see nacos-sync log for the cause)"; fi

# --- 4. network partition between nacos-sync and every registry
NET=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(compose ps -q nacos-sync)")
docker network disconnect "$NET" "$(compose ps -q nacos-sync)"
start_app "eureka-provider#2" http://localhost:18042/hello SERVER_PORT=18042
sleep 60
docker network connect --alias nacos-sync "$NET" "$(compose ps -q nacos-sync)"
eventually 180 "partition healed: instance added during the partition is synced" nacos_has_port $NACOS_A $EP 18042
eventually 180 "partition healed: eureka->nacos original instance present" nacos_has_port $NACOS_A $EP 18032
eventually 180 "partition healed: nacos->eureka instance present" eureka_has_port $NC 18035
stop_app "eureka-provider#2" TERM
eventually $WAIT "partition healed: later removals still propagate" nacos_lacks_port $NACOS_A $EP 18042

delete_task "$T_EN"; delete_task "$T_NE"
stop_app eureka-provider; stop_app nacos-cloud-consumer
