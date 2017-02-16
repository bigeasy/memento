var Conference = require('conference')
var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('memento')

function Memento () {
    this._nodes = {}
    this._index = 0
}

Memento.prototype.join = cadence(function (async, conference) {
    async(function () {
        conference.request('store', async())
    }, function (store) {
        this._nodes = store.nodes
        this._index = store.index
    })
})

Memento.prototype.immigrate = cadence(function (async, conference) {
})

Memento.prototype.get = function (path) {
    return {
        action: 'get',
        node: this._nodes[path]
    }
}

Memento.prototype.set = cadence(function (async, set) {
    var node = this._nodes[set.path] = {
        value: set.value,
        key: set.path,
        createdIndex: this._index,
        modifiedIndex: this._index
    }
    this._index++
    return { action: 'set', node: node }
})

Memento.prototype.delete = cadence(function (async, set) {
    var node = this.nodes[set.path]
    return {
        action: 'delete',
        node: {
            createdIndex: node.createdIndex,
            key: node.key,
            modifiedIndex: this._index++
        },
        prevNode: node
    }
})

module.exports = Memento
