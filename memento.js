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

Memento.prototype.join = cadence(function (async, conference) {
    async(function () {
        conference.record_(async)(function () {
            conference.request('store', { promise: conference.government.promise }, async())
        })
    }, function (store) {
        this._nodes = store.nodes
        this._index = store.index
    })
})

// Actually, let's just have a naturalize event. Ah, no, because we may
// naturalize or else we may exile before we naturalize.
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
    var node = this._nodes[envelope.body.path] = {
        value: envelope.body.value,
        key: envelope.body.path,
        createdIndex: this._index,
        modifiedIndex: this._index
    }
    this._index++
    if (envelope.from == this.paxos.id) {
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
