const path = require('path')
const fs = require('fs').promises

const Trampoline = require('skip')
const Interrupt = require('interrupt')

const Keyify = require('keyify')

const assert = require('assert')

const Strata = require('b-tree')
const Cache = require('b-tree/cache')

const Locker = require('amalgamate/locker')
const Amalgamator = require('amalgamate')
const Journalist = require('journalist')

const rescue = require('rescue')

const coalesce = require('extant')

const ascension = require('ascension')

const ROLLBACK = Symbol('rollback')


function find (comparator, array, key, low, high) {
    let mid

    while (low <= high) {
        mid = low + ((high - low) >>> 1)
        const compare = comparator(key, array[mid].key)
        if (compare < 0) high = mid - 1
        else if (compare > 0) low = mid + 1
        else return { index: mid, found: true }
    }

    return { index: low, found: false }
}

class InnerIterator {
    constructor (outer, items) {
        this._outer = outer
        this._series = outer._mutation.series
        this._compare = outer._mutation.amalgamator._comparator.stage
        this._items = items == null ? null : { array: items, index: 0 }
        this._direction = this._outer._direction == 'reverse' ? -1 : 1
    }

    [Symbol.iterator] () {
        return this
    }

    get reversed () {
        return this._outer._direction == 'reverse'
    }

    set reversed (value) {
        const direction = value ? 'reverse' : 'forward'
        if (direction != this._outer._direction) {
            this._outer._direction = direction
            this._outer._series = this._series = 0
            this._outer._done = false
        }
    }

    // The problem with advancing over our in-memory or file backed stage is
    // that there may be writes to the stage that are greater than the last
    // value returned, but less than any of the values that have been sliced
    // into memory. Advacing indexes into the in-memory stage for inserts
    // doesn't help. If the last value returned from our primary tree is 'a',
    // and the index of the in-memory stage is pointing at 'z', then if we've
    // inserted 'b' since our last `next()` we are not going to see it, we will
    // continue from 'z' if we've been adjusting our index for the inserts.
    //
    // What if we don't advance the index? We'll let's say the amalgamaged tree
    // is at 'z' and our in-memory store is at 'b' so we reutrn 'b'. Now we
    // insert 'a' and we point at a. Really, advancing the index is not about
    // the index but about a comparison with the value at the insert spot. Seems
    // like we may as well just do a binary search each time.
    //
    // This problem also exists for staging. I've worked through a number of
    // goofy ideas to keep the active staging tree up-to-date, but the best
    // thing to do, for now, is to just scrap the entire existing iterator and
    // recreate it.
    //
    // Thought on these nested iterators, they should all always return
    // something, a non-empty set, shouldn't they? Why force that logic on the
    // caller? Yet something like dilute might do this and I've been looking at
    // making dilute synchronous.

    //
    next () {
        // TODO Here is the inner series check, so all we need is one for the
        // outer iterator and we're good.
        if (this._outer._mutation.series != this._series) {
            this._series = this._outer._mutation.series
            return { done: true, value: null }
        }
        const candidates = []
        if (this._items != null) {
            if (this._items.array.length == this._items.index) {
                return { done: true, value: null }
            } else {
                candidates.push(this._items)
            }
        }
        const array = this._outer._mutation.appends[0]
        const comparator = this._outer._mutation.amalgamator._comparator.stage
        let { index, found } = this._outer._previous.key == null
            ? { index: this._direction == 1 ? 0 : array.length, found: false }
            : find(comparator, array, this._outer._previous.key, 0, array.length - 1)
        if (found || this._direction == -1) {
            index += this._direction
        }
        if (0 <= index && index < array.length) {
            candidates.push({ array, index })
        }
        if (candidates.length == 0) {
            this._outer._done = true
            return { done: true, value: null }
        }
        candidates.sort((left, right) => {
            return comparator(left.array[left.index].key, right.array[right.index].key) * this._direction
        })
        const candidate = candidates.shift()
        // We always increment the index because Strata iterators return the
        // values reversed but we search our in-memory stage each time we
        // descend.
        this._outer._previous = candidate.array[candidate.index++]
        return { done: false, value: this._outer._previous.value }
    }
}

class OuterIterator {
    constructor ({
        transaction, mutation, direction,
        key = null, inclusive = true,
        converter = (trampoline, items, consume) => {
            consume(items.map(item => {
                return { key: item.key, parts: item.parts, value: item.parts[1] }
            }))
        }
    }) {
        this._transaction = transaction
        this._direction = direction
        this._previous = { key }
        this._mutation = mutation
        this._converter = converter
        this._series = 0
        this._inclusive = inclusive
        this._done = false
    }

    [Symbol.asyncIterator] () {
        return this
    }

    _search () {
        const {
            _mutation: { amalgamator },
            _mutation: { appends },
            _transaction: transaction,
            _direction: direction,
            _previous: { key },
            _inclusive: inclusive
        } = this
        const additional = []
        if (appends.length == 2) {
            additional.push(advance.forward([ appends[1] ]))
        }
        this._iterator = amalgamator.iterator(transaction, direction, key, inclusive, additional)
    }

    async next () {
        if (this._done) {
            return { done: true, value: null }
        }
        if (this._series != this._mutation.series) {
            this._series = this._mutation.series
            this._search()
        }
        const trampoline = new Trampoline, scope = { items: null, converted: null }
        this._iterator.next(trampoline, items => scope.items = items)
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        if (scope.items != null) {
            this._converter(trampoline, scope.items, converted => scope.converted = converted)
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        return { done: false, value: new InnerIterator(this, scope.converted) }
    }
}


class Snapshot {
    constructor (memento, transaction) {
        this._memento = memento
        this._transaction = transaction
    }
}

class Mutator extends Snapshot {
    static instance = 0

    constructor (memento) {
        super(memento, memento._locker.mutator())
        this._destructible = memento._destructible.mutators.ephemeral([ 'mutation', Mutator.instance++ ])
        this._destructible.increment()
        this._mutations = {}
        this._index = 0
        this._references = []
    }

    _mutation (name) {
        const mutation = this._mutations[name]
        if (mutation == null) {
            const store = this._memento._stores[name]
            const indices = {}
            for (const index in this._memento._stores[name].indices) {
                indices[index] = {
                    series: 1,
                    appends: [[]],
                    index: store.indices[index],
                    amalgamator: store.indices[index].amalgamator,
                    qualifier: [ name, index ]
                }
            }
            return this._mutations[name] = {
                series: 1,
                store: store,
                amalgamator: store.amalgamator,
                appends: [[]],
                qualifier: [ name ],
                indices: indices
            }
        }
        return mutation
    }

    // TODO Okay, so how do we say that any iterators should recalculate with a
    // new `Amalgamate.iterator()`? Use a count.
    async _merge (mutation) {
        await mutation.amalgamator.merge(this._transaction, mutation.appends[1])
        mutation.series++
        mutation.appends.pop()
    }

    _maybeMerge (mutation, max) {
        const { appends } = mutation
        if (appends[0].length >= max && appends.length == 1) {
            mutation.appends.unshift([])
            // TODO Really seems like a queue is appropriate.
            return this._destructible.ephemeral([ 'merge'].concat(mutation.qualifier), this._merge(mutation))
        }
        return null
    }

    _append (mutation, method, key, record, value) {
        const compound = [ key, Number.MAX_SAFE_INTEGER, this._index++ ]
        const array = mutation.appends[0]
        const parts = [{
            method: method,
            version: compound[1],
            order: compound[2]
        }, record ]
        const comparator = mutation.amalgamator._comparator.stage
        const { index, found } = find(comparator, array, compound, 0, array.length - 1)
        array.splice(index, 0, { key: compound, parts, value })
        this._maybeMerge(mutation, 1024)
    }

    set (name, record) {
        const mutation = this._mutation(name)
        const key = mutation.amalgamator.strata.extract([ record ])
        this._append(mutation, 'insert', key, record, record)
        for (const name in mutation.indices) {
            const index = mutation.indices[name]
            const key = index.index.extractor([ record ])
            this._append(index, 'insert', key, key, record)
        }
    }

    unset (name, key) {
        this._append({
            value: store.strata.extract([ record ]),
            version: this._version,
            order: this._index++
        }, [{
            header: { method: 'remove', order: this._index },
            version: this._version
        }, key ])
    }

    _iterator (name, vargs, direction) {
        if (Array.isArray(name)) {
            const mutation = this._mutation(name[0])
            const index = mutation.indices[name[1]]
            return new OuterIterator({
                transaction: this._transaction,
                mutation: index,
                direction: direction,
                key: null,
                converter: (trampoline, items, consume) => {
                    const converted = []
                    let i = 0
                    const get = () => {
                        if (i == items.length) {
                            consume(converted)
                        } else {
                            const key = items[i].key[0].slice(index.index.keyLength)
                            this._get(name[0], trampoline, key, item => {
                                assert(item != null)
                                converted[i] = {
                                    key: items[i].key,
                                    parts: items[i].parts,
                                    value: item.parts[1]
                                }
                                i++
                                trampoline.push(() => get())
                            })
                        }
                    }
                    get()
                }
            })
        }
        return new OuterIterator({
            transaction: this._transaction,
            mutation: this._mutation(name),
            direction: direction
        })
    }

    forward (name, ...vargs) {
        return this._iterator(name, vargs, 'forward')
    }

    reverse (name, ...vargs) {
        return this._iterator(name, vargs, 'reverse')
    }

    _get (name, trampoline, key, consume) {
        const mutation = this._mutation(name)
        // TODO Expose comparators in Amalgamate.
        const comparators = mutation.amalgamator._comparator
        for (const array of mutation.appends) {
            const { index, found } = find(comparators.stage, array, [ key ], 0, array.length - 1)
            if (index < array.length) {
                const item = array[index]
                if (comparators.primary(item.key[0], key) == 0) {
                    consume(item.parts[0].method == 'remove' ? null : item)
                    return
                }
            }
        }
        mutation.amalgamator.get(this._transaction, trampoline, key, consume)
    }

    async get (name, key) {
        const trampoline = new Trampoline, scope = { item: null }
        this._get(name, trampoline, key, item => scope.item = item)
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        return scope.item == null ? null : scope.item.parts[1]
    }

    async commit () {
        const mutations = Object.keys(this._mutations).map(name => this._mutations[name])
        do {
            await this._destructible.drain()
            for (const mutation of mutations) {
                this._maybeMerge(mutation, 1)
                for (const name in mutation.indices) {
                    this._maybeMerge(mutation.indices[name], 1)
                }
            }
        } while (this._destructible.ephemerals != 0)
        if (mutations.some(mutation => mutation.conflicted)) {
            for (const mutation of mutations) {
                mutation.mutator.rollback()
            }
            return false
        }
        // TODO Here goes your commit write *before* you call in-memory commit.
        this._memento._locker.commit(this._transaction)
        this._destructible.decrement()
        this._destructible.decrement()
        return true
    }

    rollback () {
        throw ROLLBACK
    }

    async _rollback () {
        const mutations = Object.keys(this._mutations).map(name => this._mutations[name])
        do {
            await this._destructible.drain()
            for (const mutation of mutations) {
                this._maybeMerge(mutation, 1)
            }
        } while (this._destructible.ephemerals != 0)
        this._memento._locker.rollback(this._transaction)
        this._destructible.decrement()
        this._destructible.decrement()
    }
}

const ASCENSION_TYPE = [ String, Number, BigInt ]

class Schema extends Mutator {
    constructor (journalist, memento, version) {
        super(memento)
        this.version = version
        this._journalist = journalist
    }

    _comparisons (extraction) {
        const comparisons = []

        for (const path in extraction) {
            const parts = path.split('.')
            const properties = Array.isArray(extraction[path])
                ? extraction[path]
                : [ extraction[path] ]
            let type = ASCENSION_TYPE.indexOf(String), direction = 1
            for (const property of properties) {
                switch (typeof property) {
                case 'number':
                    direction = part < 0 ? -1 : 1
                    break
                case 'string':
                    type = property
                    break
                case 'function':
                    type = ASCENSION_TYPE.indexOf(property)
                    break
                }
            }
            comparisons.push({
                type: type,
                direction: direction,
                parts: path.split('.')
            })
        }

        return comparisons
    }

    // TODO Need a rollback interface.
    async store (name, extraction) {
        Memento.Error.assert(this._memento._stores[name] == null, 'store already exists')
        const directory = (await this._journalist.mkdir(path.join('stores', name))).absolute
        const comparisons = this._comparisons(extraction)
        await fs.mkdir(path.join(directory, 'store'))
        await fs.writeFile(path.join(directory, 'key.json'), JSON.stringify(comparisons))
        await this._memento._store(name, directory, true)
    }

    async index (name, extraction, options = {}) {
        Memento.Error.assert(this._memento._stores[name[0]] != null, 'store does not exist')
        Memento.Error.assert(this._memento._stores[name[0]].indices[name[1]] == null, 'index already exists')
        const directory = (await this._journalist.mkdir(path.join('indices', name[0], name[1]))).absolute
        const comparisons = this._comparisons(extraction)
        await fs.mkdir(path.join(directory, 'store'))
        await fs.writeFile(path.join(directory, 'key.json'), JSON.stringify({ comparisons, options }))
        await this._memento._index(name, directory, true)
    }

    // TODO Would need to close completely, then rename and reopen.
    async rename (from, to) {
        if (Array.isArray(from)) {
            Memento.Error.assert(from[0] == to[0], 'the stores for an index move must be the same')
            Memento.Error.assert(this._memento._stores[from[0]] != null, 'store does not exist')
            Memento.Error.assert(this._memento._stores[from[0]].indices[from[1]] != null, 'index does not exist')
            Memento.Error.assert(this._memento._stores[to[0]].indices[to[1]] == null, 'index already exists')
            const store = this._memento._stores[to[0]].indices[to[1]] =
                this._memento._stores[from[0]].indices[from[1]]
            delete this._memento._stores[from[0]].indices[from[1]]
            await this._journalist.rename(path.join('indices', from[0], from[1]), path.join('indices', to[0], to[1]))
        } else {
            Memento.Error.assert(this._memento._stores[from] != null, 'store does not exist')
            Memento.Error.assert(this._memento._stores[to] == null, 'store already exists')
            const store = this._memento._stores[to] = this._memento._stores[from]
            delete this._memento._stores[from]
            await this._journalist.rename(path.join('stores', from), path.join('stores', to))
            for (const name in store.indices) {
                await this._journalist.rename(path.join('indices', from, name), path.join('indices', to, name))
            }
        }
    }

    async remove (name) {
    }
}

class Memento {
    static ASC = Symbol('ascending')

    static DSC = Symbol('decending')

    static Error = Interrupt.create('Memento.Error', {
        ALREADY_EXISTS: '%s already exists',
        DOES_NOT_EXIST: '%s does not exist',
        INVALID_RENAME: 'the stores for an index rename must be the same',
        ROLLBACK: 'transaction rolled back'
    })

    constructor (options) {
        this.destructible = options.destructible
        this._destructible = null
        this._stores = {}
        this._cache = new Cache
        this._versions = { '0': true }
        this.directory = options.directory
        this._locker = new Locker({ heft: coalesce(options.heft, 1024 * 1024) })
        const primary = coalesce(options.primary, {})
        const stage = coalesce(options.stage, {})
        const leaf = { stage: coalesce(stage.leaf, {}), primary: coalesce(primary.leaf, {}) }
        const branch = { stage: coalesce(stage.branch, {}), primary: coalesce(primary.branch, {}) }
        this._comparators = coalesce(options.comparators, {})
        this._strata = {
            stage: {
                leaf: {
                    split: coalesce(leaf.stage.split, 4096),
                    merge: coalesce(leaf.stage.merge, 2048)
                },
                branch: {
                    split: coalesce(branch.stage.split, 4096),
                    merge: coalesce(branch.stage.merge, 2048)
                }
            },
            primary: {
                leaf: {
                    split: coalesce(leaf.primary.split, 4096),
                    merge: coalesce(leaf.primary.merge, 2048)
                },
                branch: {
                    split: coalesce(branch.primary.split, 4096),
                    merge: coalesce(branch.primary.merge, 2048)
                }
            }
        }
    }

    static async open ({
        destructible = new Destructible('memento'),
        directory,
        version = 1,
        comparators = {}
    } = {}, upgrade) {
        const journalist = await Journalist.create(directory)
        await Journalist.prepare(journalist)
        await Journalist.commit(journalist)
        await journalist.dispose()
        const memento = new Memento({ destructible, directory, comparators })
        const open = destructible.ephemeral('open')
        memento._destructible = {
            open: open,
            amalgamators: open.durable('amalgamators'),
            mutators: open.durable('mutators')
        }
        open.destruct(() => {
            // Destroying the amalgamators and mutators Destructible will
            // prevent new stores, indices or mutations from being created.
            memento._destructible.amalgamators.destroy()
            memento._destructible.mutators.destroy()
            // Wait for the mutations to drain, make a final rotation of the
            // amagamators, then destroy all the amalgamators.
            open.ephemeral('shutdown', async () => {
                await memento._destructible.mutators.drain()
                await memento._locker.drain()
                await memento._locker.rotate()
                for (const store in memento._stores) {
                    memento._stores[store].destructible.decrement()
                    for (const index in memento._stores[store].indices) {
                        memento._stores[store].indices[index].destructible.decrement()
                    }
                }
            })
        })
        const list = async () => {
            try {
                return await fs.readdir(memento.directory)
            } catch (error) {
                rescue(error, [{ code: 'ENOENT' }])
                await fs.mdkir(memento.directory, { recursive: true })
                return await list()
            }
        }
        const subdirs = [ 'versions', 'stores', 'indices' ].sort()
        const dirs = await list()
        if (dirs.length == 0) {
            for (const dir of subdirs) {
                await fs.mkdir(path.resolve(memento.directory, dir))
            }
            await fs.mkdir(path.resolve(memento.directory, './versions/0'))
        } else {
            for (const store of (await fs.readdir(path.join(directory, 'stores')))) {
                await memento._store(store, path.join(directory, 'stores', store))
            }
            for (const store of (await fs.readdir(path.join(directory, 'indices')))) {
                for (const index of (await fs.readdir(path.join(directory, 'indices', store)))) {
                    await memento._index([ store, index ], path.join(directory, 'indices', store, index))
                }
            }
        }
        const versions = await fs.readdir(path.resolve(memento.directory, 'versions'))
        const latest = versions.sort((left, right) => +left - +right).pop()
        if (latest < version) {
        }
        if (latest < version && upgrade != null) {
            const journalist = await Journalist.create(memento.directory)
            const schema = new Schema(journalist, memento, version)
            try {
                await upgrade(schema)
                await schema.commit()
                await journalist.mkdir(path.join('versions', String(version)))
                await journalist.write()
                await Journalist.prepare(journalist)
                await Journalist.commit(journalist)
                await journalist.dispose()
                await memento._destructible.open.destroy().rejected
                return await Memento.open({
                    destructible,
                    directory,
                    version,
                    comparators
                }, upgrade)
            } catch (error) {
                await schema._rollback()
                if (error === ROLLBACK) {
                    throw new Memento.Error('rollback')
                }
                throw error
            }
        }
        return memento
    }

    get _version () {
        throw new Error
    }

    async _open (upgrade, version) {
    }

    async _store (name, directory, create = false) {
        const comparisons = JSON.parse(await fs.readFile(path.join(directory, 'key.json'), 'utf8'))

        const extractors = comparisons.map(part => {
            return function (object) {
                const parts = part.parts.slice()
                while (object != null && parts.length != 0) {
                    object = object[parts.shift()]
                }
                return object
            }
        })

        const extractor = function (parts) {
            return extractors.map(extractor => extractor(parts[0]))
        }

        const comparator = ascension(comparisons.map(part => {
            return [
                typeof part.type == 'string'
                    ? this._comparators[part.type]
                    : ASCENSION_TYPE[part.type],
                part.direction
            ]
        }), function (object) {
            return object
        })

        const destructible = this._destructible.amalgamators.ephemeral([ 'store', name ])
        destructible.increment()

        const amalgamator = new Amalgamator(destructible, {
            locker: this._locker,
            directory: path.join(directory, 'store'),
            cache: this._cache,
            key: {
                extract: extractor,
                compare: comparator,
                serialize: function (key) {
                    return [ Buffer.from(JSON.stringify(key)) ]
                },
                deserialize: function (parts) {
                    return JSON.parse(parts[0].toString())
                }
            },
            parts: {
                serialize: function (parts) {
                    return [ Buffer.from(JSON.stringify(parts)) ]
                },
                deserialize: function (parts) {
                    return JSON.parse(parts[0].toString())
                }
            },
            transformer: function (operation) {
                if (operation.parts[0].method == 'insert') {
                    return {
                        method: 'insert',
                        key: operation.key[0],
                        parts: [ operation.parts[1] ]
                    }
                }
                return {
                    method: 'remove',
                    key: operation.key[0]
                }
            },
            createIfMissing: create,
            errorIfExists: create
        })

        await amalgamator.ready

        this._stores[name] = { destructible, amalgamator, indices: {}, comparisons }
    }

    async _index ([ storeName, name ], directory, create = false) {
        const key = JSON.parse(await fs.readFile(path.join(directory, 'key.json'), 'utf8'))

        const store = this._stores[storeName]

        const comparisons = key.comparisons.concat(store.comparisons)

        const extractors = comparisons.map(part => {
            return function (object) {
                const parts = part.parts.slice()
                while (object != null && parts.length != 0) {
                    object = object[parts.shift()]
                }
                return object
            }
        })

        const extractor = function (parts) {
            return extractors.map(extractor => extractor(parts[0]))
        }

        const comparator = ascension(comparisons.map(part => {
            return [
                typeof part.type == 'string'
                    ? this._comparators[part.type]
                    : ASCENSION_TYPE[part.type],
                part.direction
            ]
        }), function (object) {
            return object
        })

        const destructible = this._destructible.amalgamators.ephemeral([ 'store', name ])
        destructible.increment()

        const amalgamator = new Amalgamator(destructible, {
            locker: this._locker,
            directory: path.join(directory, 'store'),
            cache: this._cache,
            key: {
                compare: comparator,
                serialize: function (key) {
                    return [ Buffer.from(JSON.stringify(key)) ]
                },
                deserialize: function (parts) {
                    return JSON.parse(parts[0].toString())
                }
            },
            parts: {
                serialize: function (parts) {
                    return [ Buffer.from(JSON.stringify(parts)) ]
                },
                deserialize: function (parts) {
                    return JSON.parse(parts[0].toString())
                }
            },
            transformer: function (operation) {
                if (operation.parts[0].method == 'insert') {
                    return {
                        method: 'insert',
                        key: operation.key[0],
                        parts: [ operation.parts[1] ]
                    }
                }
                return {
                    method: 'remove',
                    key: operation.key[0]
                }
            },
            createIfMissing: create,
            errorIfExists: create
        })

        await amalgamator.ready

        store.indices[name] = {
            destructible, amalgamator, extractor, keyLength: key.comparisons.length
        }
    }

    async mutator (block) {
        const mutator = new Mutator(this)
        do {
            try {
                await block(mutator)
            } catch (error) {
                await mutator._rollback()
                if (error === ROLLBACK) {
                    return
                }
                throw error
            }
        } while (! await mutator.commit())
    }

    async close () {
        await this.destructible.destroy().rejected
    }
}

module.exports = Memento
