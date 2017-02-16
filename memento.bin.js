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

    var Shuttle = require('prolific.shuttle')

    var Colleague = require('colleague')
    var Conference = require('conference')

    var Destructor = require('destructible')

    var Memento = require('./memento')
    var Inquistor = require('./interface')
    var Service = require('./http')
    var Cliffhanger = require('cliffhanger')
    var Colleague = require('colleague')

    var nodes = {}
    var cliffhanger = new Cliffhanger

    var memento = new Memento(cliffhanger, nodes)

    var logger = require('prolific.logger').createLogger('memento')

    var shuttle = Shuttle.shuttle(program, logger)

    var destructor = new Destructor('memento')

    process.on('shutdown', destructor.destroy.bind(destructor))

    destructor.addDestructor('shuttle', shuttle.close.bind(shuttle))

    var conference = new Conference(memento, function (constructor) {
        constructor.join()
        constructor.immigrate()
        constructor.receive('set')
        constructor.receive('delete', 'remove')
    })

    var colleague = new Colleague

    colleague.spigot.emptyInto(conference.basin)
    conference.spigot.emptyInto(colleague.basin)

    var inquistor = new Inquistor(conference, cliffhanger, nodes)

    destructor.async(async, 'collegue')(function () {
        destructor.addDestructor('collegue', colleague.destroy.bind(colleague))
        colleague.connect(program, async())
    })

    var service = new Service(inquistor)
    var destroyer = require('server-destroy')

    var bind = program.ultimate.bind
    var server = http.createServer(service.dispatcher.createWrappedDispatcher())
    destroyer(server)

    server.listen(bind.port, bind.address, async())
    destructor.addDestructor('server', server.destroy.bind(server))

    logger.info('started', { params: program.ultimate, argv: program.argv })
}))
