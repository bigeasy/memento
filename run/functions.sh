mkdir -p tmp

function run_denizen () {
    local id=$1
    if [ $QUIET -eq 1 ]; then
        bash run/quiet.sh $id &
    else
        bash run/colleague.sh $id | tee tmp/colleague-$id.txt | \
            grep --line-buffered -v '^<129>1 '
    fi
}
