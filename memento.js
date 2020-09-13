const path = require('path')
const fs = require('fs').promises

const assert = require('assert')

const Strata = require('b-tree')
const Cache = require('b-tree/cache')

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
        let { index, found } = this._previous == null
            ? { index: 0, found: false }
            : this._outer._find(this._previous, false)
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
        this._previous = candidate.array[candidate.index++]
        return { done: false, value: this._previous.parts[1] }
    }
}

class OuterIterator {
    constructor (versions, mutation, direction, key, inclusive = true) {
        this._versions = versions
        this._direction = direction
        this._previous = key
        this._mutation = mutation
        this._series = mutation.series
        this._inclusive = inclusive
        this._done = false
        this._iterator()
    }

    [Symbol.asyncIterator] () {
        return this
    }

    _iterator () {
        const {
            _mutation: { amalgamator },
            _mutation: { appends },
            _versions: versions,
            _direction: direction,
            _previous: key,
            _inclusive: inclusive
        } = this
        const additional = []
        if (appends.length == 2) {
            additional.push(advance.forward([ appends[1] ]))
        }
        const iterator = amalgamator.iterator(versions, direction, key, inclusive, additional)
        this._iterator = iterator[Symbol.asyncIterator]()
    }

    _find (value) {
        if (value == null) {
            return { index: 0, found: false }
        }
        const comparator = this._mutation.amalgamator._comparator.stage
        const array = this._mutation.appends[0]
        return find(comparator, array, value.key, 0, array.length)
    }

    async next () {
        if (this._done) {
            return { done: true, value: null }
        }
        const next = await this._iterator.next()
        return { done: false, value: new InnerIterator(this, next) }
    }
}

class Snapshot {
    constructor (memento, snapshot) {
        this._memento = memento
        this._snapshot = snapshot
    }
}

class Mutator extends Snapshot {
    constructor (memento, snapshot, version) {
        super(memento, snapshot)
        this._snapshot[version] = true
        this._version = version
        this._index = 0
        this._cursors = []
        this._notebooks = []
    }

    async _assimilate () {
        do {
            if (this._queues.size == 0) {
                await this._latch.promise
            }
            for (const [ name, value ] of this._queues.entries()) {
                const stage = this._stage(name)
            }
        } while (!this.destroyed)
    }

    _notebook (name) {
        const notebook = this._notebooks[name]
        if (notebook == null) {
            return this._notebooks[name] = new Notebook
        }
        return notebook
    }

    _mutation (name) {
        const mutation = this._mutation[name]
        if (mutation == null) {
            return this._mutation[name] = {
                series: 0,
                amalgamator: this._memento._stores[name].amalgamator,
                mutator: this._memento._stores[name].amalgamator.mutator(this._version),
                appends: [[]]
            }
        }
        return mutation
    }

    _append (mutation, key, parts) {
        const array = mutation.appends[0]
        const { index, found } = find(this._comparator, array, key, 0, array.length - 1)
        array.splice(index, 0, { key: key, parts: parts })
    }

    set (name, record) {
        const mutation = this._mutation(name)
        this._append(mutation, {
            value: mutation.amalgamator.strata.extract([ record ]),
            version: this._version,
            index: this._index++
        }, [{
            method: 'insert',
            index: this._index,
            version: this._version
        }, record ])
    }

    unset (name, key) {
        this._append({
            value: store.strata.extract(record),
            version: this._version,
            index: this._index++
        }, [{
            header: { method: 'remove', index: this._index },
            version: this._version
        }, key ])
    }

    forward (name) {
        return new OuterIterator(this._snapshot, this._mutation(name), 'forward', null)
    }

    async commit () {
    }

    async rollback () {
    }
}

const ASCENSION_TYPE = [ String, Number, BigInt ]

class Memento {
    static ASC = Symbol('ascending')
    static DSC = Symbol('decending')

    constructor (destructible, directory, options = {}) {
        this._destructible = { root: destructible, opened: null }
        this._stores = {}
        this._cache = new Cache
        this._version = 1n
        this.directory = directory
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
        this._destructible.opened = this._destructible.root.ephemeral('opened')
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
            directory: path.join(directory, 'store'),
            cache: this._cache,
            extractor: extractor,
            key: {
                compare: comparator,
                serialize: function (key) {
                    return [ Buffer.from(JSON.serialize(key)) ]
                },
                deserialize: function (parts) {
                    return JSON.parse(parts[0].toString())
                }
            },
            parts: {
                serialize: function (parts) {
                    return [ Buffer.from(JSON.serialize(parts)) ]
                },
                deserialize: function (parts) {
                    return JSON.parse(parts[0].toString())
                }
            },
            header: {
                compose: function (version, method, index) {
                    return { header: { method, index }, version }
                },
                serialize: function (header) {
                    return Buffer.from(JSON.stringify({
                        header: {
                            method: header.header.method,
                            index: header.header.index
                        },
                        version: header.version.toString()
                    }))
                },
                deserialize: function (buffer) {
                    const header = JSON.parse(buffer.toString())
                    header.version = BigInt(header.version)
                    return header
                },
            },
            transformer: function (operation) {
                return {
                    method: operation.method,
                    key: key,
                    value: ('value' in operation) ? operation.value : null
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

    _snapshot () {
        return { '0': true }
    }

    mutator () {
        return new Mutator(this, this._snapshot(), this._version++)
    }

    async close () {
    }
}

module.exports = Memento
