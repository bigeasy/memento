var Conference = require('conference')
var cadence = require('cadence')
var logger = require('prolific.logger').createLogger('bigeasy.memento.consensus')

function Memento (colleague) {
    this.conference = this._createConference(colleague)
    this._nodes = {}
    this._index = 0
}

Memento.prototype._createConference = function (colleague) {
    var conference = new Conference(colleague, this)
    conference.join('join')
    conference.immigrate('immigrate')
    conference.receive('set', 'set')
    conference.receive('setIndex', 'setIndex')
    return conference
}

Memento.prototype._snapshot = function () {
    return []
}

Memento.prototype.immigrate = cadence(function (async, participantId, properties, promise) {
    throw new Error
    var communicator = this.conference.cancelable
    async(function () {
        communicator.pause(participantId, async())
    }, function () {
        async.forEach(function (operation) {
            communicator.send('_set', participantId, operation, async())
        })(this._snapshot())
    }, function () {
        communicator.naturalize(participantId, async())
    })
})

Memento.prototype.set = cadence(function (async, set) {
    async(function () {
// TODO Could just use broadcast, but RSM means everyone would return same
// state, so could just use local state, publish means everyone does the same
// thing when they see it.
        this.conference.publish('set', set, async())
    }, function () {
        return [ true ]
    })
})

Memento.prototype.get = function (path) {
    return {
        action: 'get',
        node: this._nodes[path]
    }
}

Memento.prototype.setIndex = cadence(function (async, index) {
    this._index = index
})

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
