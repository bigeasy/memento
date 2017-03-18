source run/functions.sh

QUIET=0

run_denizen 1

while tmux_run_is_running; do
    promise=$(curl -s http://127.0.0.1:8081/health | jq -r '.government.promise')
    echo "$promise"
    [ "$promise" = "1/0" ] && break
    sleep 1
done

run_denizen 2
run_denizen 3

while tmux_run_is_running; do
    promise=$(curl -s http://127.0.0.1:8083/health | jq -r '.government.promise')
    [ "$promise" = "4/0" ] && break
    sleep 1
done

echo "------------ DONE $promise ------------"
curl -s http://127.0.0.1:8083/health | jq '.'


for i in {0..10}; do
    curl -s -X PUT -d value=$1 127.0.0.1:8083/v2/keys/key-$i
done
