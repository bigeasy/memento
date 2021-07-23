require('proof')(2, async okay => {
    const path = require('path')
    const fs = require('fs').promises
    const { coalesce } = require('extant')

    const Memento = require('..')
    const Destructible = require('destructible')

    const directory = path.resolve(__dirname, './tmp/issues')

    await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
    await fs.mkdir(directory, { recursive: true })

    // https://github.com/bigeasy/memento/issues/106
    {
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
})
