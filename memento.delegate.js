var bin = require('./memento.argv')
var cadence = require('cadence')
var events = require('events')
var logger = require('prolific.logger').createLogger('bigeasy.memento.delegate')

function Delegate () {
    this._io = null
}

Delegate.prototype.initialize = function (program, colleague, callback) {
    console.log(program.argv)
    this._io = bin(program.argv, {
        params: {
            colleague: colleague,
// TODO Deleted.
            conduit: {
                events: colleague.messages,
                initialized: cadence(function () {}),
                send: function (reinstatementId, entry, callback) {
                    logger.info('send', {
                        reinstatementId: reinstatementId,
                        entry: entry
                    })
                    colleague.publish(reinstatementId, entry)
                    callback()
                }.bind(this),
                naturalized: function () {
                    colleague.kibitzer.legislator.naturalized = true
                }.bind(this)
            }
        },
        events: program
    }, callback)
}

Delegate.prototype.stop = function () {
    this._io.events.emit('SIGINT')
}

module.exports = Delegate
