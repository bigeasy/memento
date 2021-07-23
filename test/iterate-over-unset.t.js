require('proof')(5, async okay => {
    const assert = require('assert')

    const { Future } = require('perhaps')
    const { coalesce } = require('extant')
    const Interrupt = require('interrupt')

    const fs = require('fs').promises
    const path = require('path')

    const Destructible = require('destructible')

    const Memento = require('..')

    const destructible = new Destructible(1000, 'iterate-over-unset.t')

    const directory = path.resolve(__dirname, './tmp/memento')

    await coalesce(fs.rm, fs.rmdir).call(fs, directory, { recursive: true })
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
            array[i] = { key: i, value: i }
        }

        await memento.mutator(async mutator => {
            for (const i of array) {
                mutator.set('store', { key: i.key, value: i.key })
            }
        })

        await memento.snapshot(async snapshot => {
            const gather = await snapshot.cursor('store').array()
            okay(gather, array, 'stored')
        })

        await memento.mutator(async mutator => {
            mutator.set('store', { key: 2, value: 'x' })
            array[2].value = 'x'
            const gather = await mutator.cursor('store').array()
            console.log(gather)
            okay(gather, array, 'store')
        })

        await memento.mutator(async mutator => {
            mutator.set('store', { key: 2, value: 'x' })
            array[2].value = 'x'
            const gather = await mutator.cursor('store').array()
            console.log(gather)
            okay(gather, array, 'store')
        })

        await memento.mutator(async mutator => {
            mutator.set('store', { key: 2, value: 'x' })
            array[2].value = 'x'
            const gather = await mutator.cursor('store').array()
            console.log(gather)
            okay(gather, array, 'store')
        })

        await memento.mutator(async mutator => {
            mutator.set('store', { key: 4, value: 'x' })
            array[4].value = 'x'
            const gather = await mutator.cursor('store', [ 4 ]).array()
            okay(gather, array.slice(4), 'store')
        })

        destructible.destroy()
    })

    await destructible.promise
})
