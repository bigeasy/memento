#node_modules/.bin/prolific stdio syslog --serializer wafer \
    node node_modules/compassion.channel/channel.bin.js --log recorded.json --island island --id $1 \
        node_modules/.bin/prolific --configuration inherit --inherit COMPASSION_COLLEAGUE_FD \
            node memento.bin.js --bind 808$1
