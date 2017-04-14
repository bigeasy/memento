#!/bin/bash

pushd $(dirname $(readlink -f ${BASH_SOURCE[0]}))/../.. > /dev/null
addendum_directory=$PWD
popd > /dev/null

executable_started_at=$(date '+%s')
container_name=$(basename ${BASH_SOURCE[1]})

export PATH=$PATH:$addendum_directory/k8s/node_modules/.bin

function abend () {
    local message=$1
    echo "$message" 1>&2
    exit 1
}

function k8s_get_pod () {
    namespace=$(</var/run/secrets/kubernetes.io/serviceaccount/namespace)
    token=$(</var/run/secrets/kubernetes.io/serviceaccount/token)
    authority=$KUBERNETES_SERVICE_HOST:$KUBERNETES_PORT_443_TCP_PORT
    curl \
        -sS -H "Authorization: Bearer $token" \
        --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt \
        "https://$authority/api/v1/namespaces/$namespace/pods/$HOSTNAME"
}

function wait_for_endpoint () {
    local port=$1 path=$2
    while ! curl -s "http://12.0.0.1:$port$path"; do
        sleep 1
    done
    echo "$port $path is ready"
}

function wait_for_health_endpoint () {
    local port=$1
    while ! curl -s "http://127.0.0.1:$port/health"; do
        sleep 1
    done
    echo "$port is ready"
}

function wait_for_container () {
    local wait_for_container_name=$1 uptime=$2
    if [[ -z "$uptime" ]]; then
        uptime=0
    fi
    while true; do
        pod=$(k8s_get_pod)
        echo "$wait_for_container_name"
        ready=$(echo "$pod" | jq --arg container "$wait_for_container_name" -r '
            .status.containerStatuses[] |
            select(.name == $container) |
            .ready
        ')
        echo "$pod" | jq --arg container "$wait_for_container_name" -r '
            .status.containerStatuses[] |
            select(.name == $container)
        '
        if [[ "$ready" = "true" ]]; then
            echo "$pod" | jq --arg container "$container_name" '
                .status.containerStatuses[] |
                select(.name == $container)
            '
            echo "container_name => $container_name"
            container_started_at=$(echo "$pod" | jq --arg container "$container_name" -r '
                .status.containerStatuses[] |
                select(.name == $container) |
                .state.running.startedAt
            ')
            echo "k8s_started_at $k8s_started_at"
            wait_for_started_at=$(echo "$pod" | jq --arg container "$wait_for_container_name" -r '
                .status.containerStatuses[] |
                select(.name == $container) |
                .state.running.startedAt
            ')
            echo "container_started_at $container_started_at"
            echo "executable_started_at $executable_started_at"
            echo "wait_for_started_at $wait_for_started_at"
            clock_skew=$(( $executable_started_at - $(date -d "$container_started_at" +%s) ))
            duration=$(( $(date +%s) - $(date -d "$wait_for_started_at" +%s) ))
            echo "before clock skew applied $duration $clock_skew $uptime"
            duration=$(( $duration - $clock_skew ))
            echo "after clock skew applied $duration $clock_skew $uptime"
            [[ $duration -ge "$uptime" ]] && break
        fi
        sleep 1
    done
}
