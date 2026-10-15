#!/usr/bin/env bash
# Nacos A <-> Nacos B: single-service tasks in both directions, loop prevention, metadata updates,
# offline handling, and the serviceName=ALL task.
. "$(dirname "$0")/../lib.sh"
SCENARIO=nacos-nacos
setup_clusters || exit 1

NP=nacos-provider-cloud
start_app nacos-provider-cloud http://localhost:18088/hello $(nacos_app_env) || exit 1
np_ip() { nacos_instances $NACOS_A $NP | jq -r '.[] | select(.port == 18088) | .ip'; }

T_AB=$(add_task "$NACOS_A_ID" "$NACOS_B_ID" $NP)
eventually $WAIT "nacos->nacos: instance registered in Nacos B with sync metadata" nacos_synced_from $NACOS_B $NP 18088 "$NACOS_A_ID"
# A second instance added later must be added to B without dropping the first.
start_app "nacos-provider-cloud#2" http://localhost:18098/hello $(nacos_app_env) SERVER_PORT=18098
eventually $WAIT "nacos->nacos: second instance synced" nacos_has_port $NACOS_B $NP 18098
stays 20 "nacos->nacos: Nacos B keeps both instances" nacos_count $NACOS_B $NP 2
stop_app "nacos-provider-cloud#2" TERM
eventually $WAIT "nacos->nacos: second instance removed" nacos_lacks_port $NACOS_B $NP 18098
stays 20 "nacos->nacos: first instance kept after the second left" nacos_has_port $NACOS_B $NP 18088
T_BA=$(add_task "$NACOS_B_ID" "$NACOS_A_ID" $NP)
sleep 10
stays 30 "bidirectional: Nacos A keeps exactly 1 $NP (no echo from B)" nacos_count $NACOS_A $NP 1
check "bidirectional: Nacos B keeps exactly 1 $NP" nacos_count $NACOS_B $NP 1

nacos_set_instance $NACOS_A $NP "$(np_ip)" 18088 true '{"e2e-version":"v2","preserved.register.source":"SPRING_CLOUD"}' >/dev/null
eventually $WAIT "nacos->nacos: metadata update propagated" nacos_meta_is $NACOS_B $NP 18088 e2e-version v2

stop_app nacos-provider-cloud TERM
eventually $WAIT "nacos->nacos: stopped instance removed from Nacos B" nacos_count $NACOS_B $NP 0
start_app nacos-provider-cloud http://localhost:18088/hello $(nacos_app_env)
eventually $WAIT "nacos->nacos: restarted instance re-synced" nacos_has_port $NACOS_B $NP 18088

delete_task "$T_AB"; delete_task "$T_BA"
eventually $WAIT "task delete: synced instance removed from Nacos B" nacos_count $NACOS_B $NP 0
check "task delete: native instance in Nacos A untouched" nacos_has_port $NACOS_A $NP 18088

# --- persistent (non-ephemeral) instances, registered through the admin API
PS=e2e-persistent
nacos_register_persistent() { # <port> <ip> <instancePort>
  local tok; tok=$(nacos_token "$1")
  if nacos_is_v3 "$1"; then
    curl -s -X POST -H "accessToken: $tok" "localhost:$1/nacos/v3/admin/ns/instance" --data-urlencode "serviceName=$PS" \
      --data-urlencode "groupName=DEFAULT_GROUP" --data-urlencode "namespaceId=public" --data-urlencode "ip=$2" \
      --data-urlencode "port=$3" --data-urlencode "ephemeral=false"
  else
    curl -s -X POST "localhost:$1/nacos/v1/ns/instance" --data-urlencode "serviceName=$PS" --data-urlencode "ip=$2" \
      --data-urlencode "port=$3" --data-urlencode "ephemeral=false" --data-urlencode "accessToken=$tok"
  fi
}
nacos_register_persistent $NACOS_A 10.30.0.1 30001 >/dev/null
nacos_register_persistent $NACOS_A 10.30.0.2 30002 >/dev/null
T_PS=$(add_task "$NACOS_A_ID" "$NACOS_B_ID" $PS)
eventually $WAIT "nacos->nacos: persistent instances synced" nacos_count $NACOS_B $PS 2
delete_task "$T_PS"
eventually $WAIT "nacos->nacos: persistent synced instances removed on task delete" nacos_count $NACOS_B $PS 0

# --- serviceName=ALL copies every (non-synced) service of the source group. Created without a group, as the console
# does by default, which means DEFAULT_GROUP.
start_app nacos-cloud-consumer http://localhost:18035/hello $(nacos_app_env)
T_ALL=$(add_task "$NACOS_A_ID" "$NACOS_B_ID" ALL "")
eventually $WAIT "ALL task: $NP copied to Nacos B" nacos_has_port $NACOS_B $NP 18088
eventually $WAIT "ALL task: spring-cloud-nacos-consumer copied to Nacos B" nacos_has_port $NACOS_B spring-cloud-nacos-consumer 18035
stop_app nacos-cloud-consumer TERM
eventually $WAIT "ALL task: stopped service instance removed from Nacos B" nacos_count $NACOS_B spring-cloud-nacos-consumer 0

# A source that cannot be reached for a while says nothing about which of its services are gone, so no sub-task is
# deleted. The copies would survive here anyway, as deleting a sub-task needs the source too; they would not on a
# source that only fails to list its services.
# The check counts the warning TaskDispatcher logs for a delete that fails: keep the two in step.
delete_failures() { compose logs nacos-sync 2>/dev/null | grep -c "deleteTaskEvent delete failure"; }
failures_before=$(delete_failures)
compose pause nacos-a >/dev/null 2>&1
stays 20 "ALL task: copies stay in Nacos B while Nacos A is unreachable" nacos_has_port $NACOS_B $NP 18088
compose unpause nacos-a >/dev/null 2>&1
check "ALL task: no sub-task deleted while Nacos A was unreachable" [ "$(delete_failures)" -eq "$failures_before" ]
eventually $WAIT "ALL task: Nacos A answers again after the pause" nacos_has_port $NACOS_A $NP 18088

# Pause/resume through the status update API.
set_task_status "$T_ALL" DELETE
eventually $WAIT "ALL task paused (status DELETE): synced instances removed from Nacos B" nacos_count $NACOS_B $NP 0
set_task_status "$T_ALL" SYNC
eventually $WAIT "ALL task resumed (status SYNC): instances back in Nacos B" nacos_has_port $NACOS_B $NP 18088

delete_task "$T_ALL"
eventually $WAIT "ALL task delete: synced instances removed from Nacos B" nacos_count $NACOS_B $NP 0

stop_app nacos-provider-cloud
