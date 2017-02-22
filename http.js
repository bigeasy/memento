var cadence = require('cadence')
var Dispatcher = require('inlet/dispatcher')

function Service (inquisitor) {
    this._inquisitor = inquisitor
    var dispatcher = new Dispatcher(this)
    dispatcher.dispatch('GET /', 'index')
    dispatcher.dispatch('PUT /v2/keys/(.+)', 'set')
    dispatcher.dispatch('GET /v2/keys/(.+)', 'get')
    dispatcher.dispatch('DELETE /v2/keys/(.+)', 'remove')
    this.dispatcher = dispatcher
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
        request.raise(404)
    }
    return got
})

Service.prototype.remove = cadence(function (async, request, path) {
    console.log(request.body)
    var value = request.body.value
    this._inquisitor.remove({ path: path }, async())
})

module.exports = Service
