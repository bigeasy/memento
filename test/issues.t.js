require('proof')(5, async okay => {
    const path = require('path')
    const fs = require('fs').promises
    const { coalesce } = require('extant')

    const Memento = require('..')
    const Destructible = require('destructible')

    const directory = path.resolve(__dirname, './tmp/issues')


    // https://github.com/bigeasy/memento/issues/106
    {
        await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
        await fs.mkdir(directory, { recursive: true })

        const destructible = new Destructible('open-and-rename')
        destructible.ephemeral('open and rename', async () => {
            const schema = async schema => {
                switch (schema.version.target) {
                case 1:
                    await schema.store('one', { 'key': Number })
                    break
                case 2:
                    await schema.rename('one', 'two')
                    await schema.store('one', { 'key': String })
                    break
                }
            }
            {
                const memento = await Memento.open({
                    version: 1,
                    destructible: destructible.ephemeral('memento'),
                    directory: directory,
                }, schema)
                await memento.close()
            }
            {
                const memento = await Memento.open({
                    version: 2,
                    destructible: destructible.ephemeral('memento'),
                    directory: directory,
                }, schema)
                await memento.snapshot(async snapshot => {
                    okay(await snapshot.get('one', [ '1' ]), null, 'one exists')
                    okay(await snapshot.get('two', [ 1 ]), null, 'two exists')
                })
                await memento.close()
            }
            destructible.destroy()
        })

        await destructible.promise
    }
    // https://github.com/bigeasy/memento/issues/134
    {
        await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
        await fs.mkdir(directory, { recursive: true })

        const destructible = new Destructible('reverse-less-than')
        destructible.ephemeral('open and rename', async () => {
            const schema = async schema => {
                switch (schema.version.target) {
                case 1:
                    await schema.store('one', { 'key': Number })
                    break
                }
            }
            {
                const memento = await Memento.open({
                    version: 1,
                    destructible: destructible.ephemeral('memento'),
                    directory: directory,
                }, schema)
                const expected = [ 'Alfa', 'November', 'Uniform' ]
                await memento.mutator(async mutator => {
                    mutator.set('one', { key: 10 })
                    mutator.set('one', { key: 8 })
                    mutator.set('one', { key: 6 })
                    mutator.set('one', { key: 4 })
                    const gathered = []
                    for await (const items of mutator.cursor('one', [ 9 ]).reverse()) {
                        for (const item of items) {
                            gathered.push(item.key)
                        }
                    }
                    okay(gathered, [ 8, 6, 4 ], 'reverse in-memory')
                })
                await memento.snapshot(async snapshot => {
                    const gathered = []
                    for await (const items of snapshot.cursor('one', [ 9 ]).reverse()) {
                        for (const item of items) {
                            gathered.push(item.key)
                        }
                    }
                    okay(gathered, [ 8, 6, 4 ], 'reverse in staging snapshot')
                })
                await memento.mutator(async mutator => {
                    const gathered = []
                    for await (const items of mutator.cursor('one', [ 9 ]).reverse()) {
                        for (const item of items) {
                            gathered.push(item.key)
                        }
                    }
                    okay(gathered, [ 8, 6, 4 ], 'reverse in staging mutator')
                })
                await memento.close()
            }
            destructible.destroy()
        })

        await destructible.promise
    }
})
