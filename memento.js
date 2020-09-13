const path = require('path')
const fs = require('fs').promises

const assert = require('assert')

const Strata = require('b-tree')
const Cache = require('b-tree/cache')

const Amalgamator = require('amalgamate')

const rescue = require('rescue')

const coalesce = require('extant')

const ascension = require('ascension')

//const Queue = require('avenue')

// TODO Offhand, it seems like Riffle going backwards will not miss. If we merge
// the page we land in the new page. If we resplit before the old key we land in
// the new page, after the new key we land in the old page.

// Riffle going forward can miss easily, because we're searching by key.
// Wherever we land, something could have been inserted before us. We search by
// the previous key because we'll never advance to the next page in that way. We
// could search by the next key, then scan backwards for the spot for the old
// key, where it would be not for it specifically since it may no longer exist.
//
// If we can't find the old key in the exiting page we hold onto the least value
// in the backwards scan and descend again to check the previous page. Kind of
// repeat the process since we may have added a full page of items since then.
//
// If the old key is at the very end of which ever previous page we continue
// with page we held onto, if not we discard it and continue with the previous
// page. This will have to go into Riffle as some sort of super Riffle. Maybe it
// is the default Riffle, if pages are long and slices are short relative to the
// length this double descent isn't to terribly expensive.
//
// The queue is in memory. Strata is fastest when you're able to insert a sorted
// array of objects into a tree. Memento will simply build that array for you,
// by default. Makes memento slow, I suppose but we could add a direct set and
// unset to Memento itself if doing so can reduce the fuss.
//
// But, with our mutator, we want to be able to have an isolate transaction
// where the user can insert records, then reference them in queries. For this
// we keep an in-memory queue that is serialized by a background strand. The
// queue an array for each store, sorted, so that it can be merged sorted. This
// will allow for performant bulk inserts, which are going to be a common case
// and a headache otherwise.
//
// The queue is sorted so we can use ordinary homogenize. Not even certain that
// super-Riffle is needed, but we have it figured out if it is needed. A cursor
// will move through the queue as well as through homogenize. Oof...
//
// The problem with this is that all the interators take slices of the
// underlying trees. No way to update that slice. We have to keep a queue for as
// long as it takes for each Riffle to fetch a new batch of records. Ugh.
//
// Okay, so that suggests and array of queues. The first queue is the one that
// has the dynamic index problem. The iterator will have an index into the
// queue, but a call to set can throw those indexes off. So a call to set should
// increment any index that is less than or equal to the insert index of the new
// record in the queue. Simple enough.
//
// If the queue is empty? I think `-1` for forward iteration, `0` for reverse.
// Come back to that.
//
// Thought the worker strand could just shift entries off the queue and
// decrement these indexes, but now I see that we can't count on super-Riffle at
// all since Riffle returns slices. So we have to keep the old queues until
//
// Convinced myself that a super-Riffle and

//
class SyncCursor {
    constructor (queue) {
        this._queue = { array: queue, index: -1 }
    }

    next () {
        if (this._items.length == this._index && !this._done) {
            return false
        }
        const stored = this._items[this._index]
    }

    set () {
        const key = this.key
    }
}

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

class Notes {
    constructor (scribbles, notes) {
        this.notes = notes.map(note => {
            return {
                array: note.array.slice(),
                scribbles: note.scribbles.shifter(),
                index: 0
            }
        })
        this._scribbles = scribbles.shifter().sync
        this.resumable = true
    }

    copy () {
        return new Notes(this._scribbles, this.notes)
    }

    play () {
        for (const scribble of this._scribbles.iterator()) {
            switch (scribble.method) {
            case 'unshift':
                this.notes.unshift({
                    array: [],
                    scribbles: scribble.scribbles.shifter().sync
                })
                break
            }
        }
        let i = 0
        NOTES: while (i != this.notes.length) {
            const note = this.notes[i]
            for (const scribble of note.scribbles.sync.iterator()) {
                switch (scribble.method) {
                case 'splice': {
                        note.array.splice(scribble.index, 0, scribble.record)
                        if (scribble.index <= note.index) {
                            note.index++
                        }
                    }
                    break
                case 'delete': {
                        this._notes.splice(i, 1)
                    }
                    continue NOTES
                }
            }
            i++
        }
    }

    resume (previous) {
        this._previous = previous
    }

    async next () {
        const filtered = this._notes.filter(node => note.index < note.items.length)
        if (filtered.length == 0) {
            return { done: true, value: null }
        }
        for (;;) {
            filter.sort((left, right) => {
                return this._comparator(left.array[left.index], right.array[right.index])
            })
            const candidate = filtered[0].array[filtered[0].index]
            if (previous != null && this._comparator(candidate, this._previous) <= 0) {
                filtered[0].index++
                continue
            }
            return { done: false, value: [ candidate ] }
        }
    }
}

class Notebook {
    constructor (mutator, comparator) {
        this._comparator = comparator
        this._mutator = mutator
        this._scribbles = { notebook: new Queue, note: [] }
        this.notes = new Notes(this._scribbles.notebook, [])
        this._inners = new Set
        this._unshift()
    }

    _unshift () {
        const scribbles = new Queue
        this._scribbles.notebook.push({
            method: 'unshift',
            scribbles: scribbles
        })
        this._scribbles.note.unshift(scribbles)
        this.notes.play()
    }

    append (key, parts) {
        const scribbles = this._scribbles[1]
        const array = this.notes.notes[0].array
        const { index } = find(this._comparator, array, key, 0, array.length - 1)
        this._scribbles.note[0].push({
            method: 'splice',
            index: index,
            record: { key: key, parts: parts }
        })
        this.notes.play()
    }

    _check (queue, i) {
        if (queue.every(countdown => countdown.size == queue.references)) {
            this._notebooks.splice(i, 1)
            return 0
        }
        return 1
    }

    inner (inner) {
        this._inners.add(inner)
    }

    uninner (inner) {
        this._inners.delete(inner)
    }

    outer (cursor) {
        for (const queue of this._notebooks) {
            queue.referneces++
            if (queue.countdown != null) {
                for (const countdown of queue.countdown) {
                    countdown.set(cursor, true)
                }
            }
        }
    }

    riffled (cursor) {
        let i = 0
        while (i != this._notebooks.length) {
            const queue = this._notebooks[i]
            if (queue.countdown != null) {
                for (const countdown of queue.countdown) {
                    if (!queue.countdown.get(cursor)) {
                        queue.countdown.set(cursor, true)
                        break
                    }
                }
            }
            i -= this._check(queue)
        }
    }

    unouter (cursor) {
        let i = 0
        while (i != this._notebooks.length) {
            const queue = this._notebooks[i]
            if (queue.countdown != null) {
                for (const countdown of queue.countdown) {
                    countdown.delete(cursor)
                }
            }
            queue.references--
            i -= this._check(queue)
        }
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
