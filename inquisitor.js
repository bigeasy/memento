var cadence = require('cadence')

function Inquistor (conference, cliffhanger, nodes) {
    this._conference = conference
    this._cliffhanger = cliffhanger
    this._nodes = nodes
}

Inquistor.prototype.set = cadence(function (async, set) {
    async(function () {
        this._conference.record('test', {}, async())
    }, function () {
        this._conference.broadcast('set', {
            cookie: this._cliffhanger.invoke(async()),
            from: this._conference.id,
            body: set
        })
    })
})

Inquistor.prototype.get = function (path) {
    return {
        action: 'get',
        node: this._nodes[path]
    }
}

module.exports = Inquistor
