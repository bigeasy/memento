NOISE=quiet
bash run/$NOISE.sh 1 | tee colleague-1.txt &

while tmux_run_is_running; do
    promise=$(curl -s http://127.0.0.1:8081/health | jq -r '.government.promise')
    [ "$promise" = "1/0" ] && break
    sleep 1
done

#curl -s http://127.0.0.1:8081/health | jq '.'

bash run/$NOISE.sh 2 | tee colleague-2.txt &

while tmux_run_is_running; do
    promise=$(curl -s http://127.0.0.1:8082/health | jq -r '.government.promise')
    [ "$promise" = "2/0" ] && break
    sleep 1
done

#curl -s http://127.0.0.1:8082/health | jq '.'

bash run/$NOISE.sh 3 | tee colleague-3.txt &

while tmux_run_is_running; do
    promise=$(curl -s http://127.0.0.1:8083/health | jq -r '.government.promise')
    [ "$promise" = "4/0" ] && break
    sleep 1
done

echo "------------ DONE $promise ------------"
curl -s http://127.0.0.1:8083/health | jq '.'
