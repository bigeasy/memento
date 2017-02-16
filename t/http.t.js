require('proof')(1, require('cadence')(prove))

function prove (async, assert) {
    var Service = require('../http')
    var service = new Service({
        set: function (envelope, callback) {
            callback(null, { action: 'set', value: 'Hello world' } )
        }
    })
    var UserAgent = require('vizsla')
    var ua = new UserAgent(service.dispatcher.createWrappedDispatcher())
    async(function () {
        service.set({
            body: {
                path: 'message',
                value: 'Hello world'
            }
        }, 'message', async())
    }, function (body) {
        assert(body, { action: 'set', value: 'Hello world' }, 'set')
    })
}
