var cadence = require('cadence')
var Reactor = require('reactor')

function Service (inquisitor) {
    this._inquisitor = inquisitor
    this.reactor = new Reactor(this, function (dispatcher) {
        dispatcher.dispatch('GET /', 'index')
        dispatcher.dispatch('PUT /v2/keys/(.+)', 'set')
        dispatcher.dispatch('GET /v2/keys/(.+)', 'get')
        dispatcher.dispatch('DELETE /v2/keys/(.+)', 'remove')
        dispatcher.dispatch('GET /health', 'health')
    })
}

Service.prototype.index = cadence(function (async) {
    return 'Memento API\n'
})

Service.prototype.set = cadence(function (async, request, path) {
    var value = request.body.value
    this._inquisitor.set({ path: path, value: value }, async())
})

Service.prototype.get = cadence(function (async, request, path) {
    var got = this._inquisitor.get(path)
    if (got == null) {
        throw 404
    }
    return got
})

Service.prototype.remove = cadence(function (async, request, path) {
    this._inquisitor.remove({ path: path }, async())
    return []
})

Service.prototype.health = cadence(function () {
    return { government: this._inquisitor.conference.government }
})

module.exports = Service
