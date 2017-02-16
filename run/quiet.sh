    node node_modules/compassion.colleague/colleague.bin.js --chaperon http://127.0.0.1:8286 --conduit http://127.0.0.1:8486 --island island --id 1 \
        node_modules/.bin/prolific --inherit COMPASSION_COLLEAGUE_FD stdio \
            node exclusive.bin.js node t/term.js
