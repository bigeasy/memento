require('proof')(17, async okay => {
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
    // https://github.com/bigeasy/memento/issues/144
    {
        await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
        await fs.mkdir(directory, { recursive: true })

        const destructible = new Destructible('compound.reverse.index')
        destructible.durable('compound reverse index', async () => {
            const memento = await Memento.open({
                version: 1,
                destructible: destructible.ephemeral('memento'),
                directory: directory,
            }, async schema => {
                switch (schema.version.target) {
                case 1:
                    await schema.store('value', { string: String, number: Number })
                    break
                }
            })
            await memento.mutator(async mutator => {
                mutator.set('value', { string: 'a', number: 1 })
                mutator.set('value', { string: 'a', number: 2 })
                mutator.set('value', { string: 'b', number: 1 })
                mutator.set('value', { string: 'c', number: 1 })
                mutator.set('value', { string: 'c', number: 2 })
            })
            await memento.snapshot(async snapshot => {
                const inclusive = await snapshot.cursor('value', [ 'b' ]).reverse().array()
                okay(inclusive, [{ string: 'b', number: 1 }, { string: 'a', number: 2 }, { string: 'a', number: 1 }], 'reversed staged inclusive')
                const exclusive = await snapshot.cursor('value', [ 'b' ]).exclusive().reverse().array()
                okay(exclusive, [{ string: 'a', number: 2 }, { string: 'a', number: 1 }], 'reversed staged exclusive')
            })
            await memento.mutator(async mutator => {
                mutator.set('value', { string: 'a', number: 0 })
                mutator.set('value', { string: 'a', number: 1, replaced: true })
                mutator.set('value', { string: 'a', number: 3 })
                mutator.set('value', { string: 'b', number: 0 })
                mutator.set('value', { string: 'b', number: 2 })
                const inclusive = await mutator.cursor('value', [ 'b' ]).reverse().array()
                okay(inclusive, [{
                    string: 'b', number: 2
                }, {
                    string: 'b', number: 1
                }, {
                    string: 'b', number: 0
                }, {
                    string: 'a', number: 3
                }, {
                    string: 'a', number: 2
                }, {
                    string: 'a', number: 1, replaced: true
                }, {
                    string: 'a', number: 0
                }], 'reversed in memory inclusive')
                const exclusive = await mutator.cursor('value', [ 'b' ]).exclusive().reverse().array()
                okay(exclusive, [{
                    string: 'a', number: 3
                }, {
                    string: 'a', number: 2
                }, {
                    string: 'a', number: 1, replaced: true
                }, {
                    string: 'a', number: 0
                }], 'reversed in memory inclusive')
            })
            await memento.close()
            destructible.destroy()
        })

        await destructible.promise
    }
    // Continuation of https://github.com/bigeasy/memento/issues/144 to ensure
    // that forward index searches work as well as reverse index searches.
    {
        await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
        await fs.mkdir(directory, { recursive: true })

        const destructible = new Destructible('compound.reverse.index')
        destructible.durable('compound reverse index', async () => {
            const memento = await Memento.open({
                version: 1,
                destructible: destructible.ephemeral('memento'),
                directory: directory,
            }, async schema => {
                switch (schema.version.target) {
                case 1:
                    await schema.store('value', { string: String, number: Number })
                    break
                }
            })
            await memento.mutator(async mutator => {
                mutator.set('value', { string: 'a', number: 1 })
                mutator.set('value', { string: 'a', number: 2 })
                mutator.set('value', { string: 'b', number: 1 })
                mutator.set('value', { string: 'c', number: 1 })
                mutator.set('value', { string: 'c', number: 2 })
            })
            await memento.snapshot(async snapshot => {
                const inclusive = await snapshot.cursor('value', [ 'b' ]).array()
                okay(inclusive, [{ string: 'b', number: 1 }, { string: 'c', number: 1 }, { string: 'c', number: 2 }], 'forward staged inclusive')
                const exclusive = await snapshot.cursor('value', [ 'b' ]).exclusive().array()
                okay(exclusive, [{ string: 'c', number: 1 }, { string: 'c', number: 2 }], 'forward staged exclusive')
            })
            await memento.mutator(async mutator => {
                mutator.set('value', { string: 'c', number: 0 })
                mutator.set('value', { string: 'c', number: 1, replaced: true })
                mutator.set('value', { string: 'c', number: 3 })
                mutator.set('value', { string: 'b', number: 0 })
                mutator.set('value', { string: 'b', number: 2 })
                const inclusive = await mutator.cursor('value', [ 'b' ]).array()
                okay(inclusive, [{
                    string: 'b', number: 0
                }, {
                    string: 'b', number: 1
                }, {
                    string: 'b', number: 2
                }, {
                    string: 'c', number: 0
                }, {
                    string: 'c', number: 1, replaced: true
                }, {
                    string: 'c', number: 2
                }, {
                    string: 'c', number: 3
                }], 'forward in memory inclusive')
                const exclusive = await mutator.cursor('value', [ 'b' ]).exclusive().array()
                okay(exclusive, [{
                    string: 'c', number: 0
                }, {
                    string: 'c', number: 1, replaced: true
                }, {
                    string: 'c', number: 2
                }, {
                    string: 'c', number: 3
                }], 'forward in memory inclusive')
            })
            await memento.close()
            destructible.destroy()
        })

        await destructible.promise
    }
    // https://github.com/bigeasy/memento/issues/148
    {
        await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
        await fs.mkdir(directory, { recursive: true })

        const destructible = new Destructible('compound.reverse.index')
        destructible.durable('compound reverse index', async () => {
            const memento = await Memento.open({
                version: 1,
                destructible: destructible.ephemeral('memento'),
                directory: directory,
            }, async schema => {
                switch (schema.version.target) {
                case 1:
                    await schema.store('value', { string: String, number: Number })
                    break
                }
            })
            await memento.mutator(async mutator => {
                mutator.set('value', { string: 'a', number: 1 })
                mutator.set('value', { string: 'a', number: 2 })
                mutator.set('value', { string: 'b', number: 1 })
                mutator.set('value', { string: 'c', number: 1 })
                mutator.set('value', { string: 'c', number: 2 })
            })
            await memento.mutator(async mutator => {
                mutator.set('value', { string: 'c', number: 1, replaced: 'once' })
                mutator.set('value', { string: 'c', number: 1, replaced: 'twice' })
                mutator.set('value', { string: 'a', number: 2, replaced: 'once' })
                mutator.set('value', { string: 'a', number: 2, replaced: 'twice' })
                {
                    const inclusive = await mutator.cursor('value', [ 'b' ]).array()
                    okay(inclusive, [{
                        string: 'b', number: 1
                    }, {
                        string: 'c', number: 1, replaced: 'twice'
                    }, {
                        string: 'c', number: 2
                    }], 'double edit forward in memory inclusive')
                    const exclusive = await mutator.cursor('value', [ 'b' ]).exclusive().array()
                    okay(exclusive, [{
                        string: 'c', number: 1, replaced: 'twice'
                    }, {
                        string: 'c', number: 2
                    }], 'double edit forward in memory exclusive')
                }
                {
                    const inclusive = await mutator.cursor('value', [ 'b' ]).reverse().array()
                    okay(inclusive, [{
                        string: 'b', number: 1
                    }, {
                        string: 'a', number: 2, replaced: 'twice'
                    }, {
                        string: 'a', number: 1
                    }], 'double edit reverse in memory inclusive')
                    const exclusive = await mutator.cursor('value', [ 'b' ]).reverse().exclusive().array()
                    okay(exclusive, [{
                        string: 'a', number: 2, replaced: 'twice'
                    }, {
                        string: 'a', number: 1
                    }], 'dobule edit reversein memory inclusive')
                }
            })
            await memento.close()
            destructible.destroy()
        })

        await destructible.promise
    }
})
