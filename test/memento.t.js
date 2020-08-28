require('proof')(1, async okay => {
    const fs = require('fs').promises
    const path = require('path')

    const Memento = require('../_memento')

    okay(Memento, 'require')

    const directory = path.resolve(__dirname, './tmp/memento')

    await fs.rmdir(directory, { recursive: true })
    await fs.mkdir(directory, { recursive: true })


    const memento = new Memento(directory)
    await memento.open(async (version) => {
        switch (version) {
        case 1:
            break
        }
    }, 1)
})
