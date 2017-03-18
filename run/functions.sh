mkdir -p tmp

function run_denizen () {
    local id=$1
    if [ $QUIET -eq 1 ]; then
        echo x
        bash run/quiet.sh $id &
    else
        bash run/colleague.sh $id | tee tmp/colleague-$id.txt | grep -v '^.129' &
    fi
}
