require('proof')(1, require('cadence')(prove))

function prove (async, assert) {
    var Service = require('../http')
    var service = new Service({
        conference: {
            publish: function (method, parameters, callback) {
                switch (method) {
                case 'set':
                    callback(null, { action: 'set', value: 'Hello world' } )
                    break
                }
            }
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
