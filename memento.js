const path = require('path')
const fs = require('fs').promises

const Interrupt = require('interrupt')

const assert = require('assert')

const Strata = require('b-tree')
const Cache = require('b-tree/cache')

const Locker = require('amalgamate/locker')
const Amalgamator = require('amalgamate')

const rescue = require('rescue')

const coalesce = require('extant')

const ascension = require('ascension')


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
    constructor (outer, next) {
        this._outer = outer
        this._series = outer._mutation.series
        this._items = next.done ? null : { array: next.value, index: 0 }
    }

    [Symbol.iterator] () {
        return this
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
                if (!this._done) {
                    return { done: true, value: null }
                }
            } else {
                candidates.push(this._items)
            }
        }
        const array = this._outer._mutation.appends[0]
        let { index, found } = this._outer._previous.key == null
            ? { index: 0, found: false }
            : this._outer._find(this._outer._previous, false)
        if (found) {
            index++
        }
        if (index < array.length) {
            candidates.push({ array, index })
        }
        if (candidates.length == 0) {
            if (this._items == null) {
                this._outer._done = true
            }
            return { done: true, value: null }
        }
        candidates.sort((left, right) => this._compare(left, right))
        const candidate = candidates.pop()
        this._outer._previous = candidate.array[candidate.index++]
        return { done: false, value: this._outer._previous.parts[1] }
    }
}

class OuterIterator {
    constructor (versions, mutation, direction, key, inclusive = true) {
        this._versions = versions
        this._direction = direction
        this._previous = { key }
        this._mutation = mutation
        this._series = 0
        this._inclusive = inclusive
        this._done = false
        this._search()
    }

    [Symbol.asyncIterator] () {
        return this
    }

    _search () {
        const {
            _mutation: { amalgamator },
            _mutation: { appends },
            _versions: versions,
            _direction: direction,
            _previous: { key },
            _inclusive: inclusive
        } = this
        const additional = []
        if (appends.length == 2) {
            additional.push(advance.forward([ appends[1] ]))
        }
        const iterator = amalgamator.iterator(versions, direction, key, inclusive, additional)
        // TODO LOL. Overwriting this very function.
        this._iterator = iterator[Symbol.asyncIterator]()
    }

    _find (value) {
        if (value == null) {
            return { index: 0, found: false }
        }
        const comparator = this._mutation.amalgamator._comparator.stage
        const array = this._mutation.appends[0]
        return find(comparator, array, value.key, 0, array.length - 1)
    }

    async next () {
        if (this._done) {
            return { done: true, value: null }
        }
        if (this._series != this._mutation.series) {
            this._series = this._mutation.series
            this._search()
        }
        const next = await this._iterator.next()
        return { done: false, value: new InnerIterator(this, next) }
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

    constructor (memento, version) {
        super(memento, memento._locker.mutator())
        this._destructible = memento._destructible.opened.ephemeral([ 'mutation', Mutator.instance++ ])
        this._mutations = {}
        this._version = version
        this._index = 0
        this._references = []
    }

    _mutation (name) {
        const mutation = this._mutations[name]
        if (mutation == null) {
            return this._mutations[name] = {
                series: 1,
                amalgamator: this._memento._stores[name].amalgamator,
                appends: [[]]
            }
        }
        return mutation
    }

    // TODO Okay, so how do we say that any iterators should recalculate with a
    // new `Amalgamate.iterator()`? Use a count.
    async _merge (mutation) {
        await mutation.amalgamator.merge(this._transaction, mutation.appends[1])
        mutation.appends.pop()
    }

    _maybeMerge (mutation, max) {
        if (mutation.appends[0].length >= max && mutation.appends.length == 1) {
            mutation.merge++
            mutation.appends.unshift([])
            // TODO Really seems like a queue is appropriate.
            return this._destructible.ephemeral([ 'merge', mutation.name ], this._merge(mutation))
        }
        return null
    }

    _append (mutation, key, parts) {
        const array = mutation.appends[0]
        const { index, found } = find(this._comparator, array, key, 0, array.length - 1)
        array.splice(index, 0, { key: key, parts: parts })
        this._maybeMerge(mutation, 1024)
    }

    set (name, record) {
        const mutation = this._mutation(name)
        this._append(mutation, {
            value: mutation.amalgamator.strata.extract([ record ]),
            version: this._version,
            order: this._index++
        }, [{
            method: 'insert',
            order: this._index,
            version: this._version
        }, record ])
    }

    unset (name, key) {
        this._append({
            value: store.strata.extract(record),
            version: this._version,
            order: this._index++
        }, [{
            header: { method: 'remove', order: this._index },
            version: this._version
        }, key ])
    }

    forward (name) {
        return new OuterIterator(this._transaction, this._mutation(name), 'forward', null)
    }

    async commit () {
        const mutations = Object.keys(this._mutations).map(name => this._mutations[name])
        do {
            await this._destructible.drain()
            for (const mutation of mutations) {
                this._maybeMerge(mutation, 1)
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
        return true
    }

    async rollback () {
        const mutations = Object.keys(this._mutations).map(name => this._mutations[name])
        do {
            await this._destructible.drain()
            for (const mutation of mutations) {
                this._maybeMerge(mutation, 1)
            }
        } while (this._destructible.ephemerals != 0)
        this._memento._locker.rollback(this._transaction)
    }
}

const ASCENSION_TYPE = [ String, Number, BigInt ]

class Memento {
    static ASC = Symbol('ascending')
    static DSC = Symbol('decending')
    static Error = Interrupt.create('Memento.Error')

    constructor (destructible, directory, options = {}) {
        this.destructible = destructible
        this.destructible.operative++
        this.destructible.destruct(() => {
            this.destructible.ephemeral('shutdown', async () => {
                await this._locker.drain()
                await this._locker.rotate()
                this.destructible.operative--
            })
        })
        this._destructible = { opened: null }
        this._stores = {}
        this._cache = new Cache
        this._version = 1
        this._versions = { '0': true }
        this.directory = directory
        this._locker = new Locker({ heft: coalesce(options.heft, 1024 * 1024) })
        const primary = coalesce(options.primary, {})
        const stage = coalesce(options.stage, {})
        const leaf = { stage: coalesce(stage.leaf, {}), primary: coalesce(primary.leaf, {}) }
        const branch = { stage: coalesce(stage.branch, {}), primary: coalesce(primary.branch, {}) }
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

    async open (upgrade = null, version = 1) {
        this._destructible.opened = this.destructible.ephemeral('opened')
        const list = async () => {
            try {
                return await fs.readdir(this.directory)
            } catch (error) {
                rescue(error, [{ code: 'ENOENT' }])
                await fs.mdkir(this.directory, { recursive: true })
                return await list()
            }
        }
        const subdirs = [ 'versions', 'stores' ].sort()
        const dirs = await list()
        if (dirs.length == 0) {
            for (const dir of subdirs) {
                await fs.mkdir(path.resolve(this.directory, dir))
            }
            await fs.mkdir(path.resolve(this.directory, './versions/0'))
        } else {
            for (const dir of (await list()).sort()) {
            }
        }
        const versions = await fs.readdir(path.resolve(this.directory, 'versions'))
        const latest = versions.map(version => +version).sort().pop()
        if (latest < version) {
        }
        if (latest < version && upgrade != null) {
            await upgrade(version)
        }
    }

    async _store (name, create = false) {
        const directory = path.join(this.directory, 'stores', name)
        const key = JSON.parse(await fs.readFile(path.join(directory, 'key.json'), 'utf8'))
        await fs.mkdir(path.join(directory, 'store'))

        const extractors = key.map(part => {
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

        const comparator = ascension(key.map(part => {
            return [ ASCENSION_TYPE[part.type], part.direction ]
        }), function (object) {
            return object
        })

        const destructible = this._destructible.opened.durable([ 'store', name ])

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
                        order: operation.key.order,
                        method: 'insert',
                        key: operation.key.value,
                        parts: [ operation.parts[1] ]
                    }
                }
                return {
                    order: operation.key.order,
                    method: 'remove',
                    key: operation.key.value
                }
            },
            createIfMissing: create,
            errorIfExists: create
        })

        await amalgamator.ready

        const store = this._stores[name] = { destructible, amalgamator }
    }

    async store (name, extraction) {
        const comparisons = []

        for (const path in extraction) {
            const parts = path.split('.')
            const properties = Array.isArray(extraction[path])
                ? extraction[path]
                : [ extraction[path] ]
            let type = ASCENSION_TYPE.indexOf(String), direction = 1
            for (const property of properties) {
                if (property === Memento.ASC) {
                    direction = 1
                } else if (property === Memento.DSC) {
                    direction = -1
                } else {
                    type = property
                }
            }
            comparisons.push({
                type: type,
                direction: direction,
                parts: path.split('.')
            })
        }

        const directory = path.join(this.directory, 'stores', name)
        await fs.mkdir(directory, { recursive: true })
        await fs.writeFile(path.join(directory, 'key.json'), JSON.stringify(comparisons))

        await this._store(name, true)
    }

    mutator () {
        Memento.Error.assert(!this.destructible.destroyed, 'destroyed')
        return new Mutator(this, this._version++)
    }
}

module.exports = Memento
