var Conference = require('conference')
var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('memento')
var crypto = require('crypto')
var abend = require('abend')
var assert = require('assert')
var Cubbyhole = require('cubbyhole')

function Memento (cliffhanger, nodes) {
    this._nodes = nodes
    this._index = 0
    this._cliffhanger = cliffhanger
    this._stores = {}
    this._cubbyholes = new Cubbyhole
}

Memento.prototype.test = cadence(function (async, conference, body) {
    console.log('GOT TEST', body)
})

Memento.prototype.join = cadence(function (async, conference) {
    async(function () {
        var socket = null, shifter = null
        if (!conference.replaying) {
            socket = conference.socket({ promise: conference.government.promise })
            socket.write.push(null)
            shifter = socket.read.shifter()
        }
        async(function () {
            conference.record_(async)(function () {
                shifter.dequeue(async())
            })
        }, function (envelope) {
            console.log('JOIN', envelope)
            assert(envelope.module == 'addendum' && envelope.method == 'index')
            this._index = envelope.body
        })
        var loop = async(function () {
            conference.record_(async)(function () {
                shifter.dequeue(async())
            })
        }, function (envelope) {
            if (envelope == null) {
                return [ loop.break ]
            }
            assert(envelope.module == 'addendum' && envelope.method == 'subscription')
            this._node[envelope.body.path] = envelope.body.node
        })()
    }, function () {
        console.log('----> JOINED', { index: this._index, nodes: this._nodes })
    })
})

Memento.prototype.immigrate = cadence(function (async, conference, id) {
    this._cubbyholes.set(conference.government.promise, null, {
        nodes: JSON.parse(JSON.stringify(this._nodes)),
        index: this._index
    })
})

Memento.prototype.naturalized = cadence(function (async, conference, promise) {
    delete this._cubbyholes.remove(promise)
})

Memento.prototype.exile = cadence(function (async, conference, id) {
    this._cubbys.remove(conference.government.exile.promise)
})

Memento.prototype._socket = cadence(function (async, socket, header) {
    var shifter = socket.read.shifter()
    async(function () {
        console.log('dequeuing')
        shifter.dequeue(async())
    }, function (envelope) {
        console.log('dequeued')
        assert(envelope == null, 'there should be no message body')
       this._cubbyholes.wait(header.promise, async())
    }, function (store) {
        socket.write.push({
            module: 'addendum',
            method: 'index',
            body: store.index
        })
        for (var path in store.nodes) {
            socket.write.push({
                module: 'addendum',
                method: 'index',
                body: { path: path, node: store.nodes[path] }
            })
        }
        socket.write.push(null)
    })
})

Memento.prototype.socket = function (conference, socket, header) {
    this._socket(socket, header, abend)
}

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
