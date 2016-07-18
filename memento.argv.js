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
    program.helpIf(program.command.param.help)
    program.command.required('bind')

    console.log('env', process.env)

    console.log('called')

    require('prolific').setLevel('bigeasy.paxos', 'trace')
    require('prolific').setLevel('bigeasy.compassion', 'trace')
    require('prolific').setLevel('bigeasy.kibitz', 'trace')

    var colleague = program.params.colleague

    var Service = require('./http')
    var Memento = require('./memento')
    var UserAgent = require('vizsla')
    var ua = new UserAgent
    var http = require('http')

    var logger = require('prolific.logger').createLogger('emissary.interface.bin')

    var memento = new Memento(colleague, program.command.param.advertize, service)
    var service = new Service(memento)
    var destroyer = require('server-destroy')

    var bind = program.command.bind('bind')
    var server = http.createServer(service.dispatcher.createWrappedDispatcher())
    destroyer(server)
    server.listen(bind.port, bind.address, async())
    program.on('SIGINT', server.destroy.bind(server))
    program.on('SIGINT', function () { program.disconnectIf() })
    logger.info('started', { argv: program.argv })
}))
