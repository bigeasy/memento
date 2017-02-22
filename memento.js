var Conference = require('conference')
var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('memento')
var crypto = require('crypto')

function Memento (cliffhanger, nodes) {
    this._nodes = nodes
    this._index = 0
    this._cliffhanger = cliffhanger
    this._stores = {}
}

Memento.prototype.test = cadence(function (async, conference, body) {
    console.log('GOT TEST', body)
})

Memento.prototype.join = cadence(function (async, conference) {
    async(function () {
        conference.record_(async)(function () {
            conference.request('store', { promise: conference.government.promise }, async())
        })
    }, function (store) {
        for (var path in store.nodes) {
            this._nodes[path] = store.nodes[path]
        }
        this._index = store.index
    })
})

Memento.prototype.immigrate = cadence(function (async, conference, id) {
    this._stores[conference.government.promise] = {
        nodes: JSON.parse(JSON.stringify(this._nodes)),
        index: this._index
    }
})

Memento.prototype._clearStore = function (promise) {
    delete this._stores[promise]
}

Memento.prototype.naturalize = cadence(function (async, conference, promise) {
    this._clearStore(promise)
})

Memento.prototype.exile = cadence(function (async, conference, id) {
    this._clearStore(conference.government.exile.promise)
})

Memento.prototype.store = cadence(function (async, conference, body) {
    return [ this._stores[body.promise] ]
})

Memento.prototype.set = cadence(function (async, conference, envelope) {
    console.log(envelope)
    var node = this._nodes[envelope.body.path] = {
        value: envelope.body.value,
        key: envelope.body.path,
        createdIndex: this._index,
        modifiedIndex: this._index
    }
    this._index++
    if (envelope.from == conference.id) {
        console.log('RESOLVING!')
        this._cliffhanger.resolve(envelope.cookie, [ null, { action: 'set', node: node }])
    }
})

Memento.prototype.remove = cadence(function (async, set) {
    var node = this.nodes[set.path]
    if (envelope.from == this.paxos.id) {
        var result = {
            action: 'delete',
            node: {
                createdIndex: node.createdIndex,
                key: node.key,
                modifiedIndex: this._index++
            },
            prevNode: node
        }
        this._cliffhanger.resolve(envelope.cookie, [ null, result ])
    }
})

module.exports = Memento
