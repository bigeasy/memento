var cadence = require('cadence')

function Inquistor (conference, cliffhanger, nodes) {
    this.conference = conference
    this._cliffhanger = cliffhanger
    this._nodes = nodes
}

Inquistor.prototype.set = cadence(function (async, set) {
    async(function () {
        this.conference.invoke('test', {}, async())
    }, function () {
        this.conference.broadcast('set', {
            cookie: this._cliffhanger.invoke(async()),
            from: this.conference.id,
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
