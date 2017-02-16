require('proof/redux')(1, prove)

function prove (assert) {
    var Participant = require('../participant')
    assert(Participant, 'require')
}
