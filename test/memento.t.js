require('proof')(1, async okay => {
    const fs = require('fs').promises
    const path = require('path')

    const Destructible = require('destructible')

    const Memento = require('..')

    okay(Memento, 'require')

    const directory = path.resolve(__dirname, './tmp/memento')

    await fs.rmdir(directory, { recursive: true })
    await fs.mkdir(directory, { recursive: true })

    const destructible = new Destructible('memento.t')
    const memento = new Memento(destructible.durable('memento'), directory)
    await memento.open(async (version) => {
        switch (version) {
        case 1:
            await memento.store('employee', { lastName: Memento.ASC, firstName: Memento.ASC })
            break
        }
    }, 1)

    // TODO Do we really need a snapshot as opposed to a mutator? No.
    const mutator = memento.mutator()

    mutator.set('employee', { firstName: 'George', lastName: 'Washington', state: 'VA' })

    for await (const employees of mutator.forward('employee')) {
        for (const employee of employees) {
            console.log(employee)
        }
    }

    await mutator.commit()

    memento.close()
})
