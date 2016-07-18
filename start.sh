id=$1
host_ip=$(ifconfig en0 | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p')

node_modules/.bin/prolific stdio node node_modules/compassion.colleague/colleague.bin.js \
    --conduit 127.0.0.1:8486 --island memento --id $id --module \
        --timeout 3000 --ping 1000 \
        "$PWD/memento.delegate.js" \
            --bind 8080  | tee log$id.txt
