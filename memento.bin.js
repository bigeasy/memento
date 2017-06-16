/*

    ___ usage ___ en_US ___
    node emissary.bin.js <options>

    options:

        --help                          display help message
        -b, --bind          <sring>     interface and port to bind to

    ___ $ ___ en_US ___

        bind is required:
            the `--bind` is a required argument

        bind is not integer:
            the `--bind` must be an integer

    ___ . ___

 */
require('arguable')(module, require('cadence')(function (async, program) {
    program.helpIf(program.ultimate.help)
    program.required('bind')

    program.validate(require('arguable/bindable'), 'bind')

    var http = require('http')
    var cadence = require('cadence')
    var abend = require('abend')

    var Shuttle = require('prolific.shuttle')

    var Colleague = require('colleague')
    var Conference = require('conference')

    var Destructible = require('destructible')

    var Memento = require('./memento')
    var Inquisitor = require('./inquisitor')
    var Service = require('./middleware')
    var Cliffhanger = require('cliffhanger')
    var Colleague = require('colleague')

    var Thereafter = require('thereafter')

    var destructible = new Destructible('memento')

    process.on('shutdown', destructible.destroy.bind(destructible))

    var logger = require('prolific.logger').createLogger('memento')
    var shuttle = Shuttle.shuttle(program, logger)
    destructible.addDestructor('shuttle', shuttle, 'close')

    var nodes = {}
    var cliffhanger = new Cliffhanger
    var memento = new Memento(cliffhanger, nodes)

    var conference = new Conference(memento, function (constructor) {
        constructor.join()
        constructor.immigrate()
        constructor.naturalized()
        constructor.exile()
        constructor.method('test')
        constructor.socket()
        constructor.receive('set')
        constructor.receive('delete', 'remove')
    })

    var colleague = new Colleague(conference)

    var inquisitor = new Inquisitor(conference, cliffhanger, nodes)

    var thereafter = new Thereafter
    destructible.addDestructor('thereafter', thereafter, 'cancel')

    thereafter.run(function (ready) {
        destructible.addDestructor('collegue', colleague, 'destroy')
        colleague.listen(program, destructible.monitor('colleague'))
        colleague.ready.wait(ready, 'unlatch')
    })

    var service = new Service(inquisitor)
    var destroyer = require('server-destroy')

    var bind = program.ultimate.bind
    var server = http.createServer(service.reactor.middleware)
    destroyer(server)

    thereafter.run(function (ready) {
        cadence(function (async) {
            async(function () {
                destructible.addDestructor('http', server, 'destroy')
                server.listen(bind.port, bind.address, async())
            }, function () {
                ready.unlatch()
                delta(async()).ee(server).on('close')
            })
        })(destructible.monitor('server'))
    })

    async(function () {
        destructible.ready.wait(async())
    }, function () {
        logger.info('started', { params: program.ultimate, argv: program.argv })
    })
}))
