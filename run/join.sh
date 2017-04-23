source run/functions.sh

QUIET=1

run_denizen 1

while tmux_run_is_running; do
    curl -sS http://127.0.0.1:8081/health | jq -r '.'
    promise=$(curl -s http://127.0.0.1:8081/health | jq -r '.government.promise')
    echo "$promise"
    [ "$promise" = "1/0" ] && break
    sleep 1
done

#curl -s http://127.0.0.1:8081/health | jq '.'

run_denizen 2

while tmux_run_is_running; do
    promise=$(curl -s http://127.0.0.1:8082/health | jq -r '.government.promise')
    [ "$promise" = "2/0" ] && break
    sleep 1
done

#curl -s http://127.0.0.1:8082/health | jq '.'

run_denizen 3

while tmux_run_is_running; do
    promise=$(curl -s http://127.0.0.1:8083/health | jq -r '.government.promise')
    [ "$promise" = "4/0" ] && break
    sleep 1
done

echo "------------ DONE $promise ------------"
curl -s http://127.0.0.1:8083/health | jq '.'
