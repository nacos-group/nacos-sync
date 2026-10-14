#!/usr/bin/env bash
# Consul <-> Nacos: one-way in both directions, bidirectional loop prevention, offline handling and
# the "one instance goes away, its peers stay" case for Nacos -> Consul.
. "$(dirname "$0")/../lib.sh"
SCENARIO=consul-nacos
setup_clusters || exit 1

CP=consul-provider-cloud   # registers in Consul, port 18089, /nacos-hello calls NP through Consul
NP=nacos-provider-cloud    # registers in Nacos A, port 18088, /consul-hello calls CP through Nacos
start_app consul-provider-cloud http://localhost:18089/hello || exit 1
start_app nacos-provider-cloud http://localhost:18088/hello $(nacos_app_env) || exit 1

T_CN=$(add_task "$CONSUL_ID" "$NACOS_A_ID" $CP)
T_NC=$(add_task "$NACOS_A_ID" "$CONSUL_ID" $NP)
eventually $WAIT "consul->nacos: Consul instance registered in Nacos with sync metadata" nacos_synced_from $NACOS_A $CP 18089 "$CONSUL_ID"
eventually $WAIT "nacos->consul: Nacos instance registered in Consul" consul_has_port $NP 18088
eventually 120 "call path Nacos app -> Consul app through Nacos" http_body_has http://localhost:18088/consul-hello "from Consul"
eventually 120 "call path Consul app -> Nacos app through Consul" http_body_has http://localhost:18089/nacos-hello "from Nacos"

# --- bidirectional
T_NC2=$(add_task "$NACOS_A_ID" "$CONSUL_ID" $CP)
T_CN2=$(add_task "$CONSUL_ID" "$NACOS_A_ID" $NP)
sleep 10
stays 30 "bidirectional: Consul keeps exactly 1 $CP (no echo from Nacos)" consul_count $CP 1
check "bidirectional: Nacos keeps exactly 1 $NP (no echo from Consul)" nacos_count $NACOS_A $NP 1

# --- Consul -> Nacos: several instances of one service
start_app "consul-provider-cloud#2" http://localhost:18099/hello SERVER_PORT=18099
eventually $WAIT "consul->nacos: both instances of $CP synced" nacos_count $NACOS_A $CP 2
stop_app "consul-provider-cloud#2" TERM
eventually $WAIT "consul->nacos: second instance removed, first kept" nacos_count $NACOS_A $CP 1
check "consul->nacos: remaining instance is the original one" nacos_has_port $NACOS_A $CP 18089

# --- Nacos -> Consul: a second instance goes away, the first one must stay registered
start_app "nacos-provider-cloud#2" http://localhost:18098/hello $(nacos_app_env) SERVER_PORT=18098
eventually $WAIT "nacos->consul: second Nacos instance registered in Consul" consul_count $NP 2
stop_app "nacos-provider-cloud#2" TERM
eventually $WAIT "nacos->consul: stopped instance removed from Consul" consul_lacks_port $NP 18098
stays 20 "nacos->consul: remaining instance is not deregistered with it" consul_has_port $NP 18088

# --- offline by process exit
stop_app consul-provider-cloud TERM
eventually $WAIT "consul->nacos: graceful shutdown removes instance from Nacos" nacos_lacks_port $NACOS_A $CP 18089
stop_app nacos-provider-cloud KILL
eventually $WAIT "nacos->consul: kill -9 of Nacos app removes instance from Consul" consul_lacks_port $NP 18088
start_app consul-provider-cloud http://localhost:18089/hello
start_app nacos-provider-cloud http://localhost:18088/hello $(nacos_app_env)
eventually $WAIT "consul->nacos: restarted instance re-synced" nacos_has_port $NACOS_A $CP 18089
eventually $WAIT "nacos->consul: restarted instance re-synced" consul_has_port $NP 18088

# --- deleting the tasks cleans up the destinations
for t in "$T_CN" "$T_NC" "$T_NC2" "$T_CN2"; do delete_task "$t"; done
eventually $WAIT "task delete: synced instance removed from Nacos" nacos_lacks_port $NACOS_A $CP 18089
eventually $WAIT "task delete: synced instance removed from Consul" consul_lacks_port $NP 18088
check "task delete: native Consul instance untouched" consul_has_port $CP 18089

stop_app consul-provider-cloud; stop_app nacos-provider-cloud
