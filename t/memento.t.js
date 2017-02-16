require('proof/redux')(1, prove)

function prove (assert) {
    var Memento = require('../memento')
    assert(Memento, 'require')
}
