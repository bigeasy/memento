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

    var Destructor = require('destructible')

    var Memento = require('./memento')
    var Inquisitor = require('./inquisitor')
    var Service = require('./middleware')
    var Cliffhanger = require('cliffhanger')
    var Colleague = require('colleague')

    var destructor = new Destructor('memento')

    process.on('shutdown', destructor.destroy.bind(destructor))

    var logger = require('prolific.logger').createLogger('memento')
    var shuttle = Shuttle.shuttle(program, logger)
    destructor.addDestructor('shuttle', shuttle, 'close')

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

    cadence(function (async) {
        destructor.stack(async, 'collegue')(function (ready) {
            destructor.addDestructor('collegue', colleague, 'destroy')
            colleague.listen(program, async())
            colleague.ready.wait(ready, 'unlatch')
        })

    })(abend)

    var service = new Service(inquisitor)
    var destroyer = require('server-destroy')

    var bind = program.ultimate.bind
    var server = http.createServer(service.reactor.middleware)
    destroyer(server)

    destructor.addDestructor('http', server, 'destroy')

    async(function () {
        destructor.ready.wait(async())
        server.listen(bind.port, bind.address, async())
    }, function () {
        logger.info('started', { params: program.ultimate, argv: program.argv })
    })
}))
