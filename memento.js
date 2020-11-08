const path = require('path')
const fs = require('fs').promises

const Destructible = require('destructible')
const Trampoline = require('reciprocate')
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

const mvcc = {
    satiate: require('satiate'),
    constrain: require('constrain/iterator'),
    riffle: require('riffle'),
    whittle: require('./partition')
}

function find (comparator, array, key, low, high) {
    let mid

    while (low <= high) {
        mid = low + ((high - low) >>> 1)
        const compare = comparator(key, [ array[mid].key[0] ])
        if (compare < 0) high = mid - 1
        else if (compare > 0) low = mid + 1
        else return { index: mid, found: true }
    }

    return { index: low, found: false }
}

class InnerIterator {
    constructor (iterator) {
        this._iterator = iterator
    }

    [Symbol.iterator] () {
        return this
    }

    get reversed () {
        return this._iterator.reversed
    }

    set reversed (value) {
        this._iterator.reversed = value
    }

    next () {
        return this._iterator.inner()
    }
}

class OuterIterator {
    constructor (iterator) {
        this._iterator = iterator
    }

    next () {
        return this._iterator.outer()
    }
}

class AmalgamatorIterator {
    constructor({
        transaction, manipulation, direction, key, inclusive, converter, joins = []
    }) {
        this.transaction = transaction
        this.key = key
        this.direction = direction
        this.inclusive = inclusive
        this.manipulation = manipulation
        this.converter = converter
        this.inclusive = inclusive
        this.done = false
        this.joins = joins
        this.joined = null
        this.comparator = manipulation.store.amalgamator.comparator.stage
        this.trampoline = new Trampoline
    }

    _search () {
        const {
            transaction: { _transaction: transaction },
            manipulation: { store: { amalgamator }, appends },
            inclusive, key, direction
        } = this
        const additional = []
        if (appends.length == 2) {
            additional.push(advance.forward([ appends[1] ]))
        }
        this._iterator = amalgamator.iterator(transaction, direction, key, inclusive, additional)
    }

    get reversed () {
        return this._iterator.direction == 'reverse'
    }

    set reversed (value) {
        const direction = value ? 'reverse' : 'forward'
        if (direction != this.direction) {
            this.direction = direction
            this.series = 0
            this.done = false
        }
    }

    async outer () {
        const { trampoline } = this, scope = { items: null, converted: null }
        if (trampoline.seek()) {
            return { done: false, value: new InnerIterator(this) }
        }
        if (this.done) {
            return { done: true, value: null }
        }
        if (this.series != this.manipulation.series) {
            this.series = this.manipulation.series
            this._search()
        }
        this._iterator.next(trampoline, items => scope.items = items)
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        if (scope.items != null) {
            this.converter(trampoline, scope.items, converted => scope.converted = converted)
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        if (scope.converted != null && this.joins.length != 0) {
            const traverse = (input, i) => {
                if (i < input.length) {
                    this._join(input[i].value, (values, keys) => {
                        input[i].join = { values, keys }
                        trampoline.sync(() => traverse(input, i + 1))
                    })
                }
            }
            traverse(scope.converted, 0)
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        this.joined = new Map
        const { manipulation: { appends: [ array ] } } = this
        if (this.joins.length != 0 && array.length != 0) {
            const { key, comparator } = this
            const start = function () {
                if (key == null) {
                    return 0
                }
                const { index, found } = find(comparator, array, [ key ], 0, array.length - 1)
                return found ? index + 1 : index
            } ()
            const end = function () {
                if (scope.converted != null) {
                    const key = scope.converted[scope.converted.length - 1].key[0]
                    const { index, found } =  find(comparator, array, [ key ], 0, array.length - 1)
                    return found ? index + 1 : index
                }
                return array.length
            } ()
            const traverse = (i) => {
                if (i < end) {
                    this._join(array[i].value, (values, keys) => {
                        this.joined.set(array[i].key[2], { values, keys })
                        trampoline.sync(() => traverse(i + 1))
                    })
                }
            }
            traverse(0)
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        if (scope.converted == null) {
            if (array.length == 0) {
                this.done = true
                return { done: true, value: null }
            }
            this._items = null
        } else {
            this._items = { array: scope.converted, index: 0 }
        }
        return { done: false, value: new InnerIterator(this) }
    }
}

// DRY. Unless you really want to see what the code would look like without the
// added complexity of the in-memory stage. It would look like this, but I
// repeat myself.

//
class SnapshotIterator extends AmalgamatorIterator {
    constructor (options) {
        super(options)
    }

    get reversed () {
        return this._iterator.direction == 'reverse'
    }

    set reversed (value) {
        const direction = value ? 'reverse' : 'forward'
        if (direction != this.direction) {
            this.direction = direction
            this.series = 0
            this.done = false
        }
    }

    inner () {
        // TODO Here is the inner series check, so all we need is one for the
        // outer iterator and we're good.
        for (;;) {
            if (this.manipulation.series != this.series) {
                return { done: true, value: null }
            }
            if (this._items.array.length == this._items.index) {
                return { done: true, value: null }
            }
            // We always increment the index because Strata iterators return the
            // values reversed but we search our in-memory stage each time we
            // descend.
            const item = this._items.array[this._items.index++]
            const result = this._filter(item)
            if (result == null || !result.done) {
                this.key = item.key[0]
                this.inclusive = false
            }
            if (result != null) {
                return result
            }
        }
    }

    _filter (item) {
        if (this.joins.length != 0) {
            for (let i = 0; i < this.joins.length; i++) {
                if (this.joins[i].inner && item.join.values[i + 1] == null) {
                    return null
                }
            }
            return { done: false, value: item.join.values }
        } else {
            return { done: false, value: item.value }
        }
    }
}

// When we join in a snapshot, we can easily use the trampoline `get` to do our
// join in the outer iterator and return an array of joined values.

// When we join in a mutator we have two problems. The user may perform a `set`
// or `unset` of a joined item changing the join value. We do a lookup against
// stored values for each of the entries we will encounter in the in-memory
// array in the next synchronous iterator. That will be a range determined by
// the last key used and the max value of the array returned from storage. We
// look up the values and put them in a map indexed by the insert order. We use
// a map because we don't have any other place to put it, the in memory stage is
// common to all iterators across all mutations.

// When we visit that item we check the map for the join values extracted from
// storage and then we check in memory stage for each value which is
// a synchornous action. The key is that we do it right at the time we're
// returning the join from the inner iterator.

// Note that we'll use the ordinary get to look up the join value. This will
// perform an lookup against the in memory stage of the joined store first, then
// a possibly async lookup against the amalgamator of the joined store. We'll
// duplicate the in memory lookup in the inner iterator. This is fine since the
// lookup on the in memory store is a synchronous operation and therefore
// cheaper than an async operation if the value is in the in memory store.

// Worse, the user may perform a set or unset of a value into the collection we're
// iterating. This will be a new entry and we won't have a value for its insert
// index in your join map. We will not have done the trampoline join to lookup
// the stored value for that new record and we need an `async` function in which
// to run the trampoline.

// What we do is we look up the value with a trampoline, but we do not run the
// trampoline. If the values are in the in memory stage of the joined store or
// are hot in the cache of the amalgamator of the joined store, our get will
// run synchronously and we can return the result.

// If our get does not run synchronosly, we'll know. When we call the trampoline
// seek we will get a true value and that means we have an async operation in
// the trampoline, so we return done for the inner iterator. When the outer
// iterator is called again, we'll check seek on the same trampoline and run it,
// then return a new inner iterator based on the current state of the iteration
// machine. So, the trampoline call should endeavor to simply put the join into
// the map and if successful we just run the filter again.

// We'll start with the problem of doing a join against kkk
// To do a join normally, we take the array we've created and add values to the
// join array in each object in the array. We return the value or skip it

//
class MutatorIterator extends AmalgamatorIterator {
    constructor (options) {
        super(options)
        this.comparator = options.manipulation.store.amalgamator.comparator.stage
        this.trampoline = new Trampoline
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
    inner () {
        const direction = this.direction == 'reverse' ? -1 : 1
        for (;;) {
            if (this.manipulation.series != this.series) {
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
            const array = this.manipulation.appends[0]
            const comparator = this.manipulation.store.amalgamator.comparator.stage
            let { index, found } = this.key == null
                ? { index: direction == 1 ? 0 : array.length, found: false }
                : find(comparator, array, [ this.key ], 0, array.length - 1)
            if (found || direction == -1) {
                index += direction
            }
            if (0 <= index && index < array.length) {
                candidates.push({ array, index })
            }
            if (candidates.length == 0) {
                this.done = true
                return { done: true, value: null }
            }
            candidates.sort((left, right) => {
                return comparator(left.array[left.index].key, right.array[right.index].key) * direction
            })
            const candidate = candidates.shift()
            // We always increment the index because Strata iterators return the
            // values reversed but we search our in-memory stage each time we
            // descend.
            const item = candidate.array[candidate.index++]
            const result = this._filter(item)
            if (result == null || !result.done) {
                this.key = item.key[0]
                this.inclusive = false
            }
            if (result != null) {
                return result
            }
        }
    }

    _filter (item) {
        if (this.joins.length != 0) {
            let join = item.join
            if (join == null) {
                join = this.joined.get(item.key[2])
                if (join == null) {
                    this._join(item, (values, keys) => {
                        this.joined.set(item.key[2], { values, keys })
                    })
                    if (this.trampoline.seek()) {
                        return { done: true, value: null }
                    }
                    return this._filter(item)
                } else {
                    join.values[0] = item.value
                }
            }
            for (let i = 0; i < this.joins.length; i++) {
                const {
                    store: { amalgamator: { comparator: { stage: comparator } } },
                    appends
                } = this.transaction._mutator(this.joins[0].name)
                for (const array of appends) {
                    const { index, found } = find(comparator, array, [ join.keys[i] ], 0, array.length - 1)
                    if (found) {
                        const hit = array[index]
                        join.values[i + 1] = hit.method == 'remove' ? null : hit.parts[1]
                        break
                    }
                }
                if (this.joins[i].inner && join.values[i + 1] == null) {
                    return null
                }
            }
            return { done: false, value: join.values }
        } else {
            return { done: false, value: item.value }
        }
    }

    _join (value, next) {
        const values = [ value ], keys = [], { trampoline } = this
        const join = (i) => {
            if (i == this.joins.length) {
                trampoline.sync(() => next(values, keys))
            } else {
                const { name, using } = this.joins[i]
                const key = using(values)
                keys.push(key)
                this.transaction._get(name, trampoline, key, item => {
                    values.push(item == null ? null : item.parts[1])
                    trampoline.sync(() => join(i + 1))
                })
            }
        }
        join(0)
    }
}

class IteratorBuilder {
    constructor (options) {
        this._options = options
        this._options.joins = []
    }

    join (name, using) {
        this._options.joins.push({ name, using, inner: true })
        return this
    }

    outer (name, using) {
        this._options.joins.push({ name, using, inner: false })
        return this
    }

    [Symbol.asyncIterator] () {
        const options = this._options
        this._options = null
        return new OuterIterator(new (options.Iterator)(options))
    }
}

class MapInnerIterator {
}

class MapIterator {
    constructor (options) {
        this._options = options
    }

    [Symbol.asyncIterator]() {
        return this
    }

    _inner (items) {
        return { done: false, value: items }
    }
}

class SnapshotMapIterator extends MapIterator {
    constructor (options) {
        super(options)
    }

    async next () {
        const trampoline = new Trampoline, scope = { items: null }
        const { converter, iterator } = this._options
        iterator.next(trampoline, items => {
            converter(trampoline, items, items => scope.items = items)
        })
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        if (iterator.done) {
            return { done: true, value: null }
        }
        return this._inner(scope.items)
    }
}

class Transaction {
    constructor (Iterator, memento, transaction) {
        this._Iterator = Iterator
        this._memento = memento
        this._transaction = transaction
    }

    _iterator (name, vargs, direction) {
        if (Array.isArray(name)) {
            const manipulation = this._manipulation(name)
            return new IteratorBuilder({
                Iterator: this._Iterator,
                transaction: this,
                manipulation: manipulation,
                direction: direction,
                key: vargs.length == 0 ? null : vargs.shift(),
                incluslive: vargs.length == 0 ? true : vargs.shift(),
                converter: (trampoline, items, consume) => {
                    const converted = []
                    let i = 0
                    const get = () => {
                        if (i == items.length) {
                            consume(converted)
                        } else {
                            const key = items[i].key[0].slice(manipulation.store.keyLength)
                            this._get(name[0], trampoline, key, item => {
                                assert(item != null)
                                converted[i] = {
                                    key: items[i].key, value: item.parts[1], join: []
                                }
                                i++
                                trampoline.sync(() => get())
                            })
                        }
                    }
                    get()
                }
            })
        }
        return new IteratorBuilder({
            Iterator: this._Iterator,
            transaction: this,
            manipulation: this._manipulation(name),
            direction: direction,
            key: null,
            incluslive: true,
            converter: (trampoline, items, consume) => {
                consume(items.map(item => {
                    return { key: item.key, value: item.parts[1], join: [] }
                }))
            }
        })
    }

    map (name, set, { extractor = $ => $ } = {}) {
        const manipulation = this._manipulation(name)
        const additional = manipulation.appends[0] || []
        const { store: { amalgamator } } = this._manipulation(name)
        const iterator = Array.isArray(name)
            ? amalgamator.map(this._transaction, set, {
                extractor, additional,
                group: (sought, key) => {
                    const partial = key.slice(0, sought.length)
                    const matched = manipulation.store.amalgamator.comparator.primary(sought, partial) == 0
                    return matched
                }
            })
            : amalgamator.map(this._transaction, set, { extractor, additional })
        return new SnapshotMapIterator({
            transaction: this,
            iterator: iterator,
            manipulation: manipulation,
            converter: Array.isArray(name)
                ? (trampoline, items, consume) => {
                    const converted = []
                    let i = 0, j = 0, entry
                    const get = () => {
                        if (i == items.length) {
                            consume(converted)
                        } else {
                            if (j == 0) {
                                converted.push(entry = {
                                    key: items[i].key,
                                    value: items[i].value,
                                    items: []
                                })
                            }
                            if (items[i].items.length == j) {
                                j = 0
                                i++
                                trampoline.sync(() => get())
                            } else {
                                const key = {
                                    domestic: items[i].items[j].key[0].slice(0, manipulation.store.keyLength),
                                    foreign: items[i].items[j].key[0].slice(manipulation.store.keyLength)
                                }
                                if (items[i].items[j].parts[0].method == 'remove') {
                                    j++
                                    trampoline.sync(() => get())
                                } else {
                                    this._get(name[0], trampoline, key.foreign, foreign => {
                                        assert(foreign.parts[0].method == 'insert')
                                        entry.items.push({
                                            key: items[i].items[j].key[0],
                                            value: foreign.parts[1]
                                        })
                                        j++
                                        trampoline.sync(() => get())
                                    })
                                }
                            }
                        }
                    }
                    get()
                }
                : (trampoline, items, consume) => {
                    consume(items.map(item => {
                        return {
                            key: item.key,
                            value: item.value,
                            items: item.items.filter(item => {
                                return item.parts[0].method != 'remove'
                            }).map(item => {
                                return { key: item.key[0], value: item.parts[1] }
                            })
                        }
                    }))
                }
        })
    }

    forward (name, ...vargs) {
        return this._iterator(name, vargs, 'forward')
    }

    reverse (name, ...vargs) {
        return this._iterator(name, vargs, 'reverse')
    }

    async get (name, key) {
        const trampoline = new Trampoline, scope = { item: null }
        this._get(name, trampoline, key, item => scope.item = item)
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        return scope.item == null ? null : scope.item.parts[1]
    }
}

class Snapshot extends Transaction {
    constructor (memento) {
        super(SnapshotIterator, memento, memento._locker.snapshot())
    }

    _manipulation (name) {
        if (Array.isArray(name)) {
            return {
                series: 1,
                appends: [[]],
                store: this._memento._stores[name[0]].indices[name[1]],
                qualifier: name,
                index: true
            }
        }
        return {
            series: 1,
            appends: [[]],
            store: this._memento._stores[name],
            qualifier: name,
            index: false
        }
    }

    _get (name, trampoline, key, consume) {
        if (Array.isArray(name)) {
            const { amalgamator, keyLength, comparator }  = this._memento._stores[name[0]].indices[name[1]]
            const iterator = mvcc.satiate(mvcc.constrain(amalgamator.iterator(this._transaction, 'forward', key, true), item => {
                return comparator(item.key.slice(0, keyLength), key) == 0
            }), 1)
            let got = false
            iterator.next(trampoline, items => {
                got = true
                const key = items[0].key.slice(keyLength)
                const { amalgamator } = this._memento._stores[name[0]]
                amalgamator.get(this._transaction, trampoline, key, consume)
            }, {
                set done (value) {
                    if (!got) {
                        consume(null)
                    }
                }
            })
        } else {
            const amalgamator = this._memento._stores[name].amalgamator
            amalgamator.get(this._transaction, trampoline, key, consume)
        }
    }

    release () {
        this._memento._locker.release(this._transaction)
    }
}

class Mutator extends Transaction {
    static instance = 0

    constructor (memento) {
        super(MutatorIterator, memento, memento._locker.mutator())
        this._destructible = memento._destructible.mutators.ephemeral([ 'mutation', Mutator.instance++ ])
        this._destructible.increment()
        this._mutations = {}
        this._index = 0
        this._references = []
    }

    _mutator (name) {
        const mutation = this._mutations[name]
        if (mutation == null) {
            const store = this._memento._stores[name]
            const indices = {}
            for (const index in this._memento._stores[name].indices) {
                indices[index] = {
                    series: 1,
                    appends: [[]],
                    store: store.indices[index],
                    qualifier: [ name, index ]
                }
            }
            // TODO No, get them as you need them, the index and such, do not
            // stuff them here.
            return this._mutations[name] = {
                series: 1,
                store: store,
                appends: [[]],
                qualifier: [ name ],
                indices: indices
            }
        }
        return mutation
    }

    _manipulation (name) {
        if (Array.isArray(name)) {
            return this._mutator(name[0]).indices[name[1]]
        }
        return this._mutator(name)
    }

    // TODO Okay, so how do we say that any iterators should recalculate with a
    // new `Amalgamate.iterator()`? Use a count.
    async _merge (mutation) {
        await mutation.store.amalgamator.merge(this._transaction, mutation.appends[1])
        mutation.series++
        mutation.appends.pop()
    }

    _maybeMerge (mutation, max) {
        const { appends } = mutation
        if (appends[0].length >= max && appends.length == 1) {
            mutation.appends.unshift([])
            // TODO Really seems like a queue is appropriate.
            this._destructible.ephemeral([ 'merge'].concat(mutation.qualifier), this._merge(mutation))
        }
        return null
    }

    // Remember that the `_index` is still needed even though we are replacing
    // the item in the in-memory on updates because you have two in-memory
    // arrays and your iteration strategy depends on the sort.

    //
    _append (mutation, method, key, record, value) {
        const compound = [ key, Number.MAX_SAFE_INTEGER, this._index++ ]
        const array = mutation.appends[0]
        const parts = [{
            method: method,
            version: compound[1],
            order: compound[2]
        }, record ]
        const comparator = mutation.store.amalgamator.comparator.stage
        const { index, found } = find(comparator, array, compound, 0, array.length - 1)
        if (found) {
            array[index] = { key: compound, parts, value, join: null }
        } else {
            array.splice(index, 0, { key: compound, parts, value, join: null })
        }
        this._maybeMerge(mutation, 1024)
    }

    set (name, record) {
        const manipulation = this._mutator(name)
        const key = manipulation.store.amalgamator.strata.extract([ record ])
        this._append(manipulation, 'insert', key, record, record)
        for (const name in manipulation.indices) {
            const index = manipulation.indices[name]
            const key = index.store.extractor([ record ])
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

    _get (name, trampoline, key, consume) {
        const mutation = this._mutator(name)
        // TODO Expose comparators in Amalgamate.
        const comparator = mutation.store.amalgamator.comparator.stage
        for (const array of mutation.appends) {
            const { index, found } = find(comparator, array, [ key ], 0, array.length - 1)
            if (found) {
                const item = array[index]
                consume(item.method == 'remove' ? null : item)
                return
            }
        }
        mutation.store.amalgamator.get(this._transaction, trampoline, key, consume)
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
        const trampoline = new Trampoline
        const writes = {}
        const version = this._transaction.mutation.version
        this._memento._commits.search(trampoline, version, cursor => {
            cursor.insert(cursor.index, version, [ version ], writes)
        })
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        await Strata.flush(writes)
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
        Memento.Error.assert(this._memento._stores[name] == null, [ 'ALREADY_EXISTS', 'store' ])
        const directory = (await this._journalist.mkdir(path.join('stores', name))).absolute
        const comparisons = this._comparisons(extraction)
        await fs.mkdir(path.join(directory, 'store'))
        await fs.writeFile(path.join(directory, 'key.json'), JSON.stringify(comparisons))
        await this._memento._store(new Set, name, directory, true)
    }

    async index (name, extraction, options = {}) {
        Memento.Error.assert(this._memento._stores[name[0]] != null, [ 'DOES_NOT_EXIST', 'store' ])
        Memento.Error.assert(this._memento._stores[name[0]].indices[name[1]] == null, [ 'ALREADY_EXISTS', 'index' ])
        const directory = (await this._journalist.mkdir(path.join('indices', name[0], name[1]))).absolute
        const comparisons = this._comparisons(extraction)
        await fs.mkdir(path.join(directory, 'store'))
        await fs.writeFile(path.join(directory, 'key.json'), JSON.stringify({ comparisons, options }))
        await this._memento._index(new Set, name, directory, true)
    }

    // TODO Would need to close completely, then rename and reopen.
    async rename (from, to) {
        if (Array.isArray(from)) {
            Memento.Error.assert(from[0] == to[0], 'INVALID_RENAME')
            Memento.Error.assert(this._memento._stores[from[0]] != null, [ 'DOES_NOT_EXIST', 'store' ])
            Memento.Error.assert(this._memento._stores[from[0]].indices[from[1]] != null, [ 'DOES_NOT_EXIST', 'index' ])
            Memento.Error.assert(this._memento._stores[to[0]].indices[to[1]] == null, [ 'ALREADY_EXISTS', 'index' ])
            const store = this._memento._stores[to[0]].indices[to[1]] =
                this._memento._stores[from[0]].indices[from[1]]
            delete this._memento._stores[from[0]].indices[from[1]]
            await this._journalist.rename(path.join('indices', from[0], from[1]), path.join('indices', to[0], to[1]))
        } else {
            Memento.Error.assert(this._memento._stores[from] != null, [ 'DOES_NOT_EXIST', 'store' ])
            Memento.Error.assert(this._memento._stores[to] == null, [ 'ALREADY_EXISTS', 'store' ])
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
        this.cache = new Cache
        this._versions = { '0': true }
        this.directory = options.directory
        this._locker = new Locker({ heft: coalesce(options.heft, 1024 * 1024) })
        this._locker.on('amalgamated', (exclusive, inclusive) => {
            this._destructible.commits.ephemeral('amalgamated', this._amalgamated(exclusive, inclusive))
        })
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
            mutators: open.durable('mutators'),
            commits: open.durable('collector')
        }
        memento._destructible.commits.increment()
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
                await memento._destructible.commits.drain()
                memento._destructible.commits.destroy()
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
        const subdirs = [ 'versions', 'stores', 'indices', 'commits' ].sort()
        const dirs = await list()
        const commits = {
            directory: path.resolve(memento.directory, 'commits'),
            cache: memento.cache,
            comparator: (left, right) => left - right,
            serializer: 'json',
            create: dirs.length == 0
        }
        if (commits.create) {
            for (const dir of subdirs) {
                await fs.mkdir(path.resolve(memento.directory, dir))
            }
            await fs.mkdir(path.resolve(memento.directory, './versions/0'))
            memento._commits = await Strata.open(memento._destructible.commits.durable('strata'), commits)
        } else {
            memento._commits = await Strata.open(memento._destructible.commits.durable('strata'), commits)
            const versions = new Set
            const iterator = mvcc.riffle(memento._commits, Strata.MIN)
            const trampoline = new Trampoline
            while (! iterator.done) {
                iterator.next(trampoline, items => {
                    for (const item of items) {
                        versions.add(item.parts[0])
                    }
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            for (const store of (await fs.readdir(path.join(directory, 'stores')))) {
                await memento._store(versions, store, path.join(directory, 'stores', store))
            }
            for (const store of (await fs.readdir(path.join(directory, 'indices')))) {
                for (const index of (await fs.readdir(path.join(directory, 'indices', store)))) {
                    await memento._index(versions, [ store, index ], path.join(directory, 'indices', store, index))
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
                await memento._destructible.open.destroy().rejected
                await journalist.mkdir(path.join('versions', String(version)))
                await journalist.write()
                await Journalist.prepare(journalist)
                await Journalist.commit(journalist)
                await journalist.dispose()
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

    async _amalgamated (exclusive, inclusive) {
        const scope = { right: exclusive + 1 }, writes = {}
        const trampoline = new Trampoline
        while (scope.right != null) {
            this._commits.search(trampoline, scope.right, cursor => {
                const i = cursor.index
                while (i < cursor.page.items.length) {
                    if (cursor.page.items[i].key > inclusive ) {
                        scope.right = null
                        return
                    }
                    cursor.remove(i, writes)
                }
                scope.right = cursor.page.right
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        await Strata.flush(writes)
    }

    async _store (versions, name, directory, create = false) {
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

        const amalgamator = await Amalgamator.open(destructible, {
            locker: this._locker,
            directory: path.join(directory, 'store'),
            cache: this.cache,
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
                    const foo = JSON.parse(parts[0].toString())
                    return foo
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

        await amalgamator.recover(versions)

        this._stores[name] = { destructible, amalgamator, indices: {}, comparisons }
    }

    async _index (versions, [ storeName, name ], directory, create = false) {
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

        const amalgamator = await Amalgamator.open(destructible, {
            locker: this._locker,
            directory: path.join(directory, 'store'),
            cache: this.cache,
            key: {
                compare: comparator,
                extract: parts => parts[0],
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

        await amalgamator.recover(versions)

        store.indices[name] = {
            destructible, amalgamator, comparator, extractor, keyLength: key.comparisons.length
        }
    }

    async snapshot (block) {
        const snapshot = new Snapshot(this)
        try {
            await block(snapshot)
        } finally {
            snapshot.release()
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
