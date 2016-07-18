#!/bin/bash

node $1 node_modules/compassion.colleague/colleague.bin.js \
    --conduit 127.0.0.1:8486 --island memento --id $2 --module \
        --timeout 3000 --ping 1000 --replay log$2.txt \
        "$PWD/memento.delegate.js" \
            --bind 8081
