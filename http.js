var cadence = require('cadence')
var Dispatcher = require('inlet/dispatcher')

function Service (consensus) {
    this._consensus = consensus
    var dispatcher = new Dispatcher(this)
    dispatcher.dispatch('GET /', 'index')
    dispatcher.dispatch('PUT /v2/keys/(.+)', 'set')
    dispatcher.dispatch('GET /v2/keys/(.+)', 'get')
    dispatcher.dispatch('DELETE /v2/keys/(.+)', 'delete')
    this.dispatcher = dispatcher
}

Service.prototype.index = cadence(function (async) {
    return 'Memento API\n'
})

Service.prototype.set = cadence(function (async, request, path) {
    var value = request.body.value
    this._consensus.conference.publish('set', {
        path: path,
        value: value
    }, async())
})

Service.prototype.get = cadence(function (async, request, path) {
    var got = this._consensus.get(path)
    if (got == null) {
        request.raise(404)
    }
    return got
})

Service.prototype.delete = cadence(function (async, request, path) {
    var value = request.body.value
    this._consensus.conference.publish('delete', { path: path }, async())
})

module.exports = Service
