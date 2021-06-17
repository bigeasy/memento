require('proof')(60, async okay => {
    const assert = require('assert')

    const { Future } = require('perhaps')
    const Interrupt = require('interrupt')

    const fs = require('fs').promises
    const path = require('path')

    const Destructible = require('destructible')

    const Memento = require('..')

    const destructible = new Destructible(1000, 'iterate-over-unset.t')

    const directory = path.resolve(__dirname, './tmp/memento')

    await fs.rmdir(directory, { recursive: true })
    await fs.mkdir(directory, { recursive: true })

    destructible.ephemeral('test', async function () {
        const memento = await Memento.open({
            version: 1,
            destructible: destructible.ephemeral('memento'),
            directory: directory,
            comparators: {
                text: (left, right) => (left > right) - (left < right)
            }
        }, async (schema) => {
            switch (schema.version.target) {
            case 1:
                await schema.store('store', { 'key': Number })
                break
            }
        })

        const array = []
        for (let i = 0; i < 10; i++) {
            array[i] = { key: i }
        }

        await memento.mutator(async mutator => {
            for (const i of array) {
                mutator.set('store', { key: i.key })
            }
        })

        await memento.snapshot(async snapshot => {
            const gather = await snapshot.cursor('store').array()
            okay(gather, array, 'stored')
        })

        await memento.mutator(async mutator => {
            mutator.unset('store', [ 2 ])
            const gather = await mutator.cursor('store').array()
            okay(gather, array, 'store')
        })

        destructible.destroy()
    })

    await destructible.promise
})
