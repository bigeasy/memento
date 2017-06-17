require('proof')(6, require('cadence')(prove))

function prove (async, assert) {
    var Service = require('../middleware')
    var service = new Service({
        set: function (envelope, callback) {
            callback(null, { action: 'set', value: 'Hello world' } )
        },
        get: function (path) {
            return path == '/path' ? 1 : null
        },
        remove: function (envelope, callback) {
            assert(envelope, { path: '/path' }, 'remove')
            callback()
        },
        conference: {
            government: 'government'
        }
    })
    var UserAgent = require('vizsla')
    var ua = new UserAgent(service.reactor.middleware)
    async(function () {
        service.index(async())
    }, function (response) {
        assert(response, 'Memento API\n', 'index')
        service.health(async())
    }, function (response) {
        assert(response, { government: 'government' }, 'health')
        service.set({
            body: {
                path: 'message',
                value: 'Hello world'
            }
        }, 'message', async())
    }, function (body) {
        assert(body, { action: 'set', value: 'Hello world' }, 'set')
    }, [function () {
        service.get({}, '/missing', async())
    }, function (error) {
        assert(error, 404, 'not found')
    }], function () {
        service.get({}, '/path', async())
    }, function (got) {
        assert(got, 1, 'get')
        service.remove({}, '/path', async())
    })
}
