var Conference = require('conference')
var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('memento')
var crypto = require('crypto')

function Memento (cliffhanger, nodes) {
    this._nodes = nodes
    this._index = 0
    this._cliffhanger = cliffhanger
}

Memento.prototype.bootstrap = cadence(function (async, conference) {
    conference.ifNotReplaying(async)(function (recorder) {
        async(function () {
            crypto.randomBytes(16, async())
        }, function (buffer) {
            conference.invoke('catalog', buffer.toString('hex'), async())
        })
    })
})

Memento.prototype.join = cadence(function (async, conference) {
    async(function () {
        this.bootstrap(conference, async())
    }, function () {
        conference.request('store', async())
    }, function (store) {
        this._nodes = store.nodes
        this._index = store.index
    }, function (store) {
        conference.boundary()
    })
})

Memento.prototype.immigrate = cadence(function (async, conference) {
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
        this._cliffhanger.resolve(envelope.cookie, [null, { action: 'set', node: node }])
    }
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
