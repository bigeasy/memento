#!/bin/bash

addendum_started_at=$(date '+%s')

pushd $(dirname $(readlink -f ${BASH_SOURCE[0]}))/.. > /dev/null
addendum_directory=$PWD
popd > /dev/null

export PATH=$PATH:$addendum_directory/node_modules/.bin

function addendum_abend () {
    local message=$1
    echo "$message" 1>&2
    exit 1
}

function addendum_k8s_get_pod () {
    namespace=$(</var/run/secrets/kubernetes.io/serviceaccount/namespace)
    token=$(</var/run/secrets/kubernetes.io/serviceaccount/token)
    authority=$KUBERNETES_SERVICE_HOST:$KUBERNETES_PORT_443_TCP_PORT
    curl \
        -sS -H "Authorization: Bearer $token" \
        --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt \
        "https://$authority/api/v1/namespaces/$namespace/pods/$NAMESPACE"
}

function addendum_wait_for_endpoint () {
    local port=$1 path=$2
    while ! curl -s "http://12.0.0.1:$port$path"; do
        sleep 1
    done
    echo "$port $path is ready"
}

function addendum_wait_for_health_endpoint () {
    local port=$1
    while ! curl -s "http://127.0.0.1:$port/health"; do
        sleep 1
    done
    echo "$port is ready"
}

function addendum_wait_for_container () {
    local container=$1 uptime=$2
    if [[ -z "$uptime" ]]; then
        uptime=0
    fi
    while true; do
        pod=$(addendum_k8s_get_pod)
        ready=$(echo "$pod" | jq --arg container -r '
            .items[0].status.containerStatuses[] |
            select(.name == $container) |
            .state.running.startedAt
        ')
        if [[ "$ready" = "true" ]]; then
            started=$(echo "$pod" | jq --arg container $container -r '
                .items[0].status.containerStatuses[] |
                select(.name == $container) |
                .state.running.startedAt
            ')
            duration=$(( $(date +%s) - $(date -d "$started" +%s) ))
            duration=$(( $duration - $addendum_clock_skew ))
            echo "$container $ready $started $duration $addendum_clock_skew"
            [[ $duration -ge "$uptime" ]] && break
        fi
        sleep 1
    done
}

echo "$addendum_started_at $k8s_started_at"
echo "$(date -d @$addendum_started_at) $k8s_started_at"
echo "$pod" | jq --arg container logger -r '
    .items[0].status.containerStatuses[] |
    select(.name == $container) |
    .state.running.startedAt
'

addendum_clock_skew=$(( $addendum_started_at - $(date -d "$k8s_started_at" +%s) ))
echo "addendum clock skew"
