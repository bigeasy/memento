eval $(minikube docker-env)

kubectl --namespace=addendum scale deployment addendum --replicas=0
kubectl --namespace=addendum scale deployment addendum --replicas=1

function kube_a () {
    kubectl --namespace=addendum "$@"
}

while
    pod=$(kube_a get pods | tail -n +2 | awk '$3 !~ /Term/ { print $1 }')
    [[ -z "$pod" ]]
do
    sleep 1
done

echo "$pod"

sleep=1
while true; do
    kube_a log "$pod" logger
    kube_a log "$pod" conduit
    kube_a get pods
    sleep $sleep
    sleep=$(( $sleep * 2 ))
done
