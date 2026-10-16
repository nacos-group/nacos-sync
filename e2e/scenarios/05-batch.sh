#!/usr/bin/env bash
# Batch sync: many services and instances in each direction, mass offline, and batch task deletion.
. "$(dirname "$0")/../lib.sh"
SCENARIO=batch
setup_clusters || exit 1

NS=${BATCH_NACOS_SERVICES:-50}; NI=${BATCH_NACOS_INSTANCES:-10}   # Nacos A load: NS services x NI instances
ES=${BATCH_EUREKA_APPS:-20}; EI=${BATCH_EUREKA_INSTANCES:-5}      # Eureka load: ES apps x EI instances
NE=${BATCH_NACOS_TO_EUREKA:-20}                                   # how many Nacos services also go to Eureka

nacos_total() { # nacos_total <port> <prefix> <services>: total instances of prefix-0..prefix-(n-1)
  local port=$1 prefix=$2 n=$3 s total=0 c
  for ((s = 0; s < n; s++)); do
    c=$(nacos_instances "$port" "$prefix-$s" | jq length) && [ -n "$c" ] || return 1
    total=$((total + c))
  done
  echo $total
}
nacos_total_eq() { [ "$(nacos_total "$1" "$2" "$3")" -eq "$4" ]; }
eureka_total() { # eureka_total <prefix>: instances of all apps whose name starts with prefix
  curl -s -H 'Accept: application/json' "$EUREKA_URL/apps" | jq --arg p "$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')-" \
    '[.applications.application[]? | select(.name | startswith($p)) | .instance | if type == "array" then .[] else . end] | length'
}
eureka_total_eq() { [ "$(eureka_total "$1")" -eq "$2" ]; }

# --- Nacos A -> Nacos B through one ALL task
start_nacos_load nacos-load $NACOS_A nload $NS $NI || exit 1
T_ALL=$(add_task "$NACOS_A_ID" "$NACOS_B_ID" ALL)
eventually 180 "batch nacos->nacos (ALL): $NS x $NI instances in Nacos B" nacos_total_eq $NACOS_B nload $NS $((NS * NI))

# --- Nacos A -> Eureka through one task per service
T_NE=()
for ((s = 0; s < NE; s++)); do T_NE+=("$(add_task "$NACOS_A_ID" "$EUREKA_ID" "nload-$s")"); done
eventually 180 "batch nacos->eureka: $NE x $NI instances in Eureka" eureka_total_eq nload $((NE * NI))

# --- Eureka -> Nacos A through one task per app
start_eureka_load eureka-load eload $ES $EI
T_EN=()
for ((a = 0; a < ES; a++)); do T_EN+=("$(add_task "$EUREKA_ID" "$NACOS_A_ID" "eload-$a")"); done
eventually 180 "batch eureka->nacos: $ES x $EI instances in Nacos A" nacos_total_eq $NACOS_A eload $ES $((ES * EI))

# --- mass offline
stop_app nacos-load KILL
eventually 180 "batch offline: Nacos load gone -> removed from Nacos B" nacos_total_eq $NACOS_B nload $NS 0
eventually 180 "batch offline: Nacos load gone -> removed from Eureka" eureka_total_eq nload 0
stop_app eureka-load KILL
eureka_deregister_all eload $ES $EI
eventually 180 "batch offline: Eureka load deregistered -> removed from Nacos A" nacos_total_eq $NACOS_A eload $ES 0

# --- batch task deletion must clean up like single deletion
start_eureka_load eureka-load eload $ES $EI
eventually 180 "batch eureka->nacos re-synced before batch delete" nacos_total_eq $NACOS_A eload $ES $((ES * EI))
q=$(printf 'taskIds=%s&' "${T_EN[@]}")
check "deleteInBatch accepted" sync_api DELETE "/v1/task/deleteInBatch?${q%&}"
eventually $WAIT "deleteInBatch: synced instances removed from Nacos A" nacos_total_eq $NACOS_A eload $ES 0
stop_app eureka-load KILL
eureka_deregister_all eload $ES $EI

delete_task "$T_ALL"; for t in "${T_NE[@]}"; do delete_task "$t"; done
