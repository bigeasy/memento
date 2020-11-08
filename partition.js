function partition (key, partition) {
    if (Array.isArray(partition)) {
        const I = partition.length
        key = key.slice(0, I)
        for (let i = 0; i < I; i++) {
            partition(key[i], partition[i])
        }
        return key
    }
    return key
}

module.exports = function (key, partition) {
    return partition(key, partition)
}
