// # Memento

// Welcome back. Here's the stuff that will take some time to load into
// programmer memory.

//  * Store and index rename and delete are always tricky.
//  * Inner iteration of mutators and why you're brilliant idea on how to make
//  it simpler or more elegant is probably worthless.
//  * Where joins are stored in memory for mutators and how upsert and delete
//  makes them a particularly difficult.
//  * Updating joins at commit is always a delete and insert operation.
//  * Maps are generally unused and unexplored in applications, so there's a lot
//  hiding in there.
//  * A lot of the commit and rollback complexity is in Amalgamator which is
//  pretty well understood and therefore pretty easy to load.

//

// Node.js API.
const fs = require('fs').promises
const path = require('path')
const assert = require('assert')

const Destructible = require('destructible')
const Trampoline = require('reciprocate')
const Interrupt = require('interrupt')
const Keyify = require('keyify')


const Strata = require('b-tree')
const Magazine = require('magazine')

const Fracture = require('fracture')
const Rotator = require('amalgamate/rotator')
const Amalgamator = require('amalgamate')
const Journalist = require('journalist')
const WriteAhead = require('writeahead')
const Operation = require('operation')
const Turnstile = require('turnstile')
const Verbatim = require('verbatim')

const rescue = require('rescue')

const { coalesce } = require('extant')

const ascension = require('ascension')
const whittle = require('whittle')

const ROLLBACK = Symbol('rollback')

const mvcc = {
    satiate: require('satiate'),
    constrain: require('constrain/iterator')
}

// **TODO** You should only need to keep one of these.
const find2 = function () {
    const find = require('b-tree/find')
    return function (comparator, array, key, low, high) {
        const index = find(comparator, array, key, low, high)
        return index < 0
            ? { index: ~index, found: false }
            : { index: index, found: true }
    }
} ()

const find = function () {
    const find = require('b-tree/find')
    return function (comparator, array, key, low, high) {
        const index = find(function (left, right) {
            return comparator(left.slice(0, 1), right.slice(0, 1))
        }, array, key, low, high)
        return index < 0
            ? { index: ~index, found: false }
            : { index: index, found: true }
    }
} ()
//

// Public interface to the synchronous methods of an iterator.

//
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
//

// Public interface to the asynchronous methods of an iterator.

//
class OuterIterator {
    constructor (iterator) {
        this._iterator = iterator
    }

    next () {
        return this._iterator.outer()
    }
}

// Base class containing the functionality common to both mutator and snapshot
// iterators.
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
        this.comparator = manipulation.store.amalgamator.comparator.stage.key
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

    _join (value, next) {
        const values = [ value ], keys = [], { trampoline } = this
        const join = (i) => {
            if (i == this.joins.length) {
                trampoline.sync(() => next(values, keys))
            } else {
                const { name, using } = this.joins[i]
                const key = using(values)
                keys.push(key)
                this.transaction._get(name, key, trampoline, item => {
                    values.push(item == null ? null : item.parts[1])
                    trampoline.sync(() => join(i + 1))
                })
            }
        }
        join(0)
    }

    async outer () {
        const { trampoline } = this, scope = { items: null, converted: null }
        // If we do this its because we optimistically tried to find a joined
        // value synchronously in the inner iterator but the trampoline reports
        // a necessary async call.
        if (trampoline.seek()) {
            return { done: false, value: new InnerIterator(this) }
        }
        if (this.done) {
            return { done: true, value: null }
        }
        // We have begun merge, meaning we need to add the old in-memory array
        // to our MVCC iterator, or finished merge, meaning we need to recreate
        // our iterator so it excludes the old in-memory array and includes our
        // merged data. (Do we really need to so the latter? The in-memory array
        // will always have what we merged and it really ought to only matter
        // when we shift a new in-memory array to the head. While you think
        // about this recall that there are only ever two in-memory stages.)
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
//

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
//

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

// Worse, the user may perform a set or unset of a value into the collection
// we're iterating. This will be a new entry and we won't have a value for its
// insert index in the join map. We will not have done the trampoline join to
// lookup the stored value for that new record and we need an `async` function
// in which to run the trampoline.

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

// We'll start with the problem of doing a join against...

// To do a join normally, we take the array we've created and add values to the
// join array in each object in the array. We return the value or skip it

//
class MutatorIterator extends AmalgamatorIterator {
    constructor (options) {
        super(options)
        this.comparator = options.manipulation.store.amalgamator.comparator.stage.key
        this.trampoline = new Trampoline
    }
    //

    // When must search our in memory stage for each inner iteration because the
    // user can synchronously upsert or delete at any time changing the
    // in-memory stage. We are constantly updating our `key` for search.
    // **TODO** create a `_previous` property. Considered using a node based
    // structure so iteration could be done by reference, but the in-memory
    // stage can be replaced synchronously at any moment. Wait, doesn't that
    // mean we can lose track of it? Yes, we can. **TODO** When we rotate our
    // in-memory stage we need to increment the series so that we force a new
    // amalgamator iterator generation.
    //
    // **TODO** Note that, because we only ever insert records into the
    // in-memory stage, we can probably exclude the bottom or top that we've
    // already traversed when performing a subsequent search.
    //
    // The problem with advancing over our in-memory or file backed stage is
    // that there may be writes to the stage that are greater than the last
    // value returned, but less than any of the values that have been sliced
    // into memory. Advacing indexes into the in-memory stage for inserts
    // doesn't help. If the last value returned from our primary tree is 'a',
    // and the index of the in-memory stage is pointing at 'z', then if we've
    // inserted 'b' since our last `next()` we are not going to see it, we will
    // continue from 'z' even if we've been adjusting our index for the inserts.
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
            const comparator = this.manipulation.store.amalgamator.comparator.stage.key
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
                    const { index, found } = find(comparator.key, array, [ join.keys[i] ], 0, array.length - 1)
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
}

class IteratorBuilder {
    constructor (reversed, options) {
        this._options = options
        this._options.joins = []
        this._joins = []
        this._reversed = reversed
    }

    join (name, using) {
        this._joins.push({ name, using, inner: true })
        return this
    }

    outer (name, using) {
        this._joins.push({ name, using, inner: false })
        return this
    }

    iterator () {
        const options = {
            ...this._options,
            direction: this._reversed ? 'reverse' : 'forward',
            joins: this._joins.slice()
        }
        return {
            [Symbol.asyncIterator] () {
                return new OuterIterator(new (options.Iterator)(options))
            }
        }
    }

    reverse () {
        this._reversed = ! this._reversed
        return this
    }

    async array () {
        const array = []
        for await (const items of this.iterator()) {
            for (const item of items) {
                array.push(item)
            }
        }
        return array
    }
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

    __iterator (name, args, direction) {
        const reversed = direction == 'reverse'
        if (Array.isArray(name)) {
            const manipulation = this._manipulation(name)
            return new IteratorBuilder(reversed, {
                Iterator: this._Iterator,
                transaction: this,
                manipulation: manipulation,
                key: args.key,
                inclusive: args.inclusive,
                converter: (trampoline, items, consume) => {
                    this._memento.pages.purge(this._cacheSize)
                    const converted = []
                    let i = 0
                    const get = () => {
                        if (i == items.length) {
                            consume(converted)
                        } else {
                            const key = items[i].key[0].slice(manipulation.store.keyLength)
                            this._get(name[0], key, trampoline, item => {
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
        return new IteratorBuilder(reversed, {
            Iterator: this._Iterator,
            transaction: this,
            manipulation: this._manipulation(name),
            key: args.key,
            incluslive: args.inclusive,
            converter: (trampoline, items, consume) => {
                this._memento.pages.purge(this._cacheSize)
                consume(items.map(item => {
                    return { key: item.key, value: item.parts[1], join: [] }
                }))
            }
        })
    }

    _iterator (name, vargs, direction) {
        this._memento.pages.purge(this._cacheSize)
        vargs.reverse()
        const args = { inclusive: true, key: null, filters: [], slurp: null, limit: -1 }
        while (vargs.length != 0) {
            const varg = vargs.shift()
            switch (typeof varg) {
            case 'boolean':
                args.inclusive = varg
                break
            case 'number':
                args.limit = varg
                break
            case 'function':
                args.filters.push(varg)
                break
            case 'object':
                assert(Array.isArray(varg))
                if (varg.length == 0) {
                    args.slurp = varg
                } else {
                    args.key = varg
                }
            }
        }
        const iterator = this.__iterator(name, args, direction)
        if (args.slurp) {
            return async function () {
                const slurp = []
                for await (const items of iterator) {
                    for (const item of items) {
                        if (args.filters.every(filter => filter(item))) {
                            slurp.push(item)
                        }
                    }
                }
                return slurp
            } ()
        }
        return iterator
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
                                if (items[i].items[j].parts[0].method == 'remove') {
                                    j++
                                    trampoline.sync(() => get())
                                } else {
                                    const key = items[i].items[j].key[0].slice(manipulation.store.keyLength)
                                    this._get(name[0], key, trampoline, foreign => {
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

    cursor (name, ...vargs) {
        return this._iterator(name, vargs, 'forward')
    }

    get (name, key, trampoline = new Trampoline, consume = value => trampoline.set(value)) {
        this._memento.pages.purge(this._cacheSize)
        this._get(name, key, trampoline, item => {
            consume(item == null ? null : item.parts[1])
        })
        return trampoline
    }
}

class Snapshot extends Transaction {
    constructor (memento) {
        super(SnapshotIterator, memento, memento._rotator.locker.snapshot())
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

    _get (name, key, trampoline, consume) {
        if (Array.isArray(name)) {
            const { amalgamator, keyLength, comparator } = this._memento._stores[name[0]].indices[name[1]]
            const iterator = mvcc.satiate(mvcc.constrain(amalgamator.iterator(this._transaction, 'forward', key, true), item => {
                return comparator(item.key[0].slice(0, keyLength), key) != 0
            }), 1)
            let got = false
            iterator.next(trampoline, items => {
                got = true
                const key = items[0].key[0].slice(keyLength)
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
        return trampoline
    }

    release () {
        this._memento._rotator.locker.release(this._transaction)
    }
}
//

// The mutator class has a confusing internal structure called a mutation which
// is easy to confuse with an Amalgamator mutator or this Mutator class,
// (**TODO** so let's rename it).

// Contains an Amalgamtor mutator and an Amalgamator snapshot. The snapshot is
// used for isolation of queries, the mutator is making its decisions based on
// this snapshot. Amalgamator snapshot and mutator creation are atomic so they
// are guaranteed to reference the same version of the database.

// Isolation is done by inserting updates into an in-memory array that is
// occasionally rotated out into the amalgamator staging tree. This in-memory
// tree is isolated from other Memento snapshots and mutators. The staged
// writes are isolated by the Amalgamtor MVCC version logic.

// Because upserts and deletes are in-memory, they are synchronous.

//
class Mutator extends Transaction {
    // Instance is used to create unique keys when we insert merges into the
    // merge fracture, **TODO** but we can use the locker version. **TODO**
    // Locker mutator and snapshot, shouldn't they be first-class objects by
    // now?

    //
    static instance = 0
    //

    // Private constructor creates a mutator for the given memento. Our
    // super-class uses our Amalgamator mutator to perform get and iteration
    // while our snapshot is kept locally. The snapshot is used to obtain the
    // existing version of a store object so we can delete the index entries
    // based on the values extracted from the existing object. **TODO** Which is
    // where we left off when we went diving into the dependencies in *gulp*
    // November.

    //
    constructor (memento) {
        super(MutatorIterator, memento, memento._rotator.locker.mutator())
        this._snapshot = memento._rotator.locker.snapshot()
        this._mutations = {}
        this._index = 0
        this._references = []
        this._promises = new Set
    }
    //

    // Internal construction of a "mutator." **TODO** Rename this, please.

    //
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
    //

    // **TODO** This is ready to take the place of the overloaded `_mutator`.

    //
    _manipulation (name) {
        if (Array.isArray(name)) {
            return this._mutator(name[0]).indices[name[1]]
        }
        return this._mutator(name)
    }
    //

    // **TODO** Implement in-memory max based on record heft. We duplicate the
    // memory of the buffer and the object in the stage, so this is truly a
    // relative property. Strata does expose serialization and does accept the
    // record you serialized for insert and delete. Splice will have to be
    // updated to accept a buffer from its transform. We would have

    // When we shift an new append array, we are no longer going to a candidate
    // for merge, so we do it immediately, synchronously once we decide to
    // merge.

    //
    _maybeMerge (stack, mutation, max) {
        assert(stack instanceof Fracture.Stack)
        const { appends } = mutation
        if (appends[0].length >= max && appends.length == 1) {
            mutation.appends.unshift([])
            const { indices } = mutation
            for (const name in indices) {
                indices[name].appends.unshift([])
            }
            // **TODO** This is new.
            mutation.series++
            const key = Keyify.stringify([ 'merge', this._transaction.mutation.version, mutation.qualifier ])
            this._promises.add(this._memento._fracture.enqueue(stack, key, value => {
                value.merges.push({
                    transaction: this._transaction,
                    snapshot: this._snapshot,
                    mutation: mutation
                })
            }))
        }
        return null
    }
    //

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
        const comparator = mutation.store.amalgamator.comparator.stage.key
        const { index, found } = find(comparator, array, compound, 0, array.length - 1)
        if (found) {
            array[index] = { key: compound, parts, value, join: null }
        } else {
            array.splice(index, 0, { key: compound, parts, value, join: null })
        }
    }

    set (name, record) {
        const mutation = this._mutator(name)
        const key = mutation.store.amalgamator.primary.storage.extractor([ record ])
        this._append(mutation, 'insert', key, record, record)
        for (const name in mutation.indices) {
            const index = mutation.indices[name]
            const key = index.store.extractor([ record ])
            this._append(index, 'insert', key, key, record)
        }
        this._maybeMerge(Fracture.stack(), mutation, 1024)
    }

    // **NOTE**: We don't unset the index record. We just accept that when we
    // lookup the actual value from the index it will be `null`. This is why you
    // can't assume that there will always be an indexed record for every index
    // record in your index based lookups. You are going to forget this.
    //
    // We probably do need to remove them, won't the get inserted otherwise?
    //
    // We are going to do our deletes of index entries when we actually merge
    // since we need to delete to both remove and update and update is based on
    // the value we have at the time of merge.

    //
    unset (name, key) {
        this._append(this._mutator(name), 'remove', key, key, null)
    }

    _getFromMemory (mutation, key) {
        const { amalgamator, getter } = mutation.store
        for (const array of mutation.appends) {
            const { index, found } = find2(getter, array, [ key ], 0, array.length - 1)
            if (found) {
                return array[index]
            }
        }
    }

    _getIndex (mutation, trampoline, key, consume) {
        const got = this._getFromMemory(mutation, key)
        if (got != null) {
            consume(got)
        } else {
            const { amalgamator, comparator, keyLength, getter } = mutation.store
            const iterator = mvcc.satiate(mvcc.constrain(amalgamator.iterator(this._transaction, 'forward', key, true), item => {
                return getter != 0
            }), 1)
            let got = false
            iterator.next(trampoline, items => {
                got = true
                consume(items[0])
            }, {
                set done (value) {
                    if (!got) {
                        consume(null)
                    }
                }
            })
        }
    }

    _get (name, key, trampoline = new Trampoline, consume = value => trampoline.set(value)) {
        // **TODO** Maybe we have `_mutator` return based on store or index?
        const mutation = this._manipulation(name)
        if (Array.isArray(name)) {
            this._getIndex(mutation, trampoline, key, item => {
                if (item != null) {
                    this._get(name[0], item.key[0].slice(mutation.store.keyLength), trampoline, consume)
                } else {
                    consume(null)
                }
            })
        } else {
            const got = this._getFromMemory(mutation, key)
            if (got != null) {
                consume(got.parts[0].method == 'remove' ? null : got)
            } else {
                mutation.store.amalgamator.get(this._transaction, trampoline, key, consume)
            }
        }
    }
    //

    // We run maybe merge with a value of zero to force a merge and that merge
    // could have nothing to merge, but we need to flush all the merges for all
    // our mutations out of our work queue.

    // If any of our merges are conflicted we rollback and return false.

    // Otherwise we commit first to our write-ahead log and then to memory. The
    // write-ahead log first because our application makes no decisions based on
    // what is in the write-ahead log. Therefore don't have to think about race
    // conditions because the in-memory commit is atomic and the application is
    // only aware of the in-memory commit which happens after the logged commit.

    //
    async _commit (persistent = true) {
        const stack = Fracture.stack()
        // All Memento mutations created by this Mutator.
        const mutations = Object.keys(this._mutations).map(name => this._mutations[name])
        // Enqueue merges for each mutation.
        for (const mutation of mutations) {
            this._maybeMerge(stack, mutation, 1)
        }
        // Wait for them all to finish merging.
        for (const promise of this._promises) {
            await promise
            this._promises.delete(promise)
        }
        if (this._memento._rotator.locker.conflicted(this._transaction)) {
            this._memento._rotator.locker.rollback(this._transaction)
            return false
        }
        // Here is your logged commit.
        if (persistent) {
            await this._memento._rotator.commit(stack, this._transaction.mutation.version)
        }
        // No more destructible decrementing, we're using Fracture now.
        this._memento._rotator.locker.commit(this._transaction)
        this._memento._rotator.locker.release(this._snapshot)
        return true
    }
    //

    // The user rolls back by calling this method which will raise an exception
    // which is merely a symbol we catch. This will stop forwward progress it
    // the user function, which may make for cleaner code. We throw a symbol to
    // forgo the generation of a stack trace. `try`/`catch` is probably still
    // expensive, but rollback is probably not a frequent operation.

    //
    rollback () {
        throw ROLLBACK
    }
    //

    // Actual rollback still writes out all the mutations. Not sure why, though.
    // Seems like it could surrender or just forget.

    //
    async _rollback () {
        const stack = Fracture.stack()
        // All Memento mutations created by this Mutator.
        const mutations = Object.keys(this._mutations).map(name => this._mutations[name])
        // Enqueue merges for each mutation.
        for (const mutation of mutations) {
            this._maybeMerge(stack, mutation, 0)
        }
        // Wait for them all to finish merging.
        for (const promise of this._promises) {
            await promise
            this._promises.delete(promise)
        }
        // Rollback in-memory, no commit logging of course.
        this._memento._rotator.locker.rollback(this._transaction)
        this._memento._rotator.locker.release(this._snapshot)
    }
}

const ASCENSION_TYPE = [ String, Number, BigInt ]

class Schema extends Mutator {
    constructor (memento, version, options) {
        super(memento)
        this.version = version
        this._operations = []
        this._temporary = 0
        this._options = options
        this._indices = {}
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
        const qualifier = path.join('staging', `store.${this._temporary++}`)
        const comparisons = this._comparisons(extraction)
        const directory = this._memento.directory
        await fs.mkdir(path.join(directory, qualifier, 'tree'), { recursive: true })
        await fs.mkdir(path.join(directory, qualifier, 'indices'), { recursive: true })
        this._operations.push({ method: 'create', type: 'store', qualifier, name })
        await fs.writeFile(path.join(directory, qualifier, 'key.json'), JSON.stringify(comparisons))
        const store = await this._memento._store({
            name: name,
            qualifier: qualifier,
            options: this._options,
            create: true
        })
    }

    async index (name, extraction, options = {}) {
        Memento.Error.assert(this._memento._stores[name[0]] != null, [ 'DOES_NOT_EXIST', 'store' ])
        Memento.Error.assert(this._memento._stores[name[0]].indices[name[1]] == null, [ 'ALREADY_EXISTS', 'index' ])
        const qualifier = path.join('staging', `index.${this._temporary++}`)
        const directory = this._memento.directory
        const comparisons = this._comparisons(extraction)
        const store = this._memento._stores[name[0]]
        await Memento.Error.resolve(fs.mkdir(path.join(directory, qualifier, 'tree'), { recursive: true }), 'IO_ERROR')
        this._operations.push({ method: 'create', type: 'index', qualifier, name: name })
        await fs.writeFile(path.join(directory, qualifier, 'key.json'), JSON.stringify({ comparisons, options }))
        const index = await this._memento._index({
            name: name,
            qualifier: qualifier,
            options: this._options,
            create: true
        })
        const mutation = this._mutator(name[0])
        const _index = mutation.indices[name[1]] = {
            series: 1,
            appends: [[]],
            store: store.indices[name[1]],
            qualifier: [ name[0], name[1] ]
        }
        // TODO This will be slow now, but I want to get it working. What I want
        // to do is use the size of the returned items array to get sets as
        // large as the largest page in the store and commit those in a chunk,
        // but I don't believe that the slice size is forwarded to amalgamate
        // yet.
        for await (const items of this.cursor(name[0]).iterator()) {
            const appends = []
            for (const item of items) {
                const key = _index.store.extractor([ item ])
                appends.push({ key: [ key ], parts: [ { method: 'insert' }, key ] })
            }
            await _index.store.amalgamator.merge(Fracture.stack(), this._transaction, appends)
        }
    }

    // TODO Would need to close completely, then rename and reopen.
    async rename (from, to) {
        if (Array.isArray(from)) {
            Memento.Error.assert(from[0] == to[0], 'INVALID_RENAME')
            Memento.Error.assert(this._memento._stores[from[0]] != null, [ 'DOES_NOT_EXIST', 'store' ])
            Memento.Error.assert(this._memento._stores[from[0]].indices[from[1]] != null, [ 'DOES_NOT_EXIST', 'index' ])
            Memento.Error.assert(this._memento._stores[to[0]].indices[to[1]] == null, [ 'ALREADY_EXISTS', 'index' ])
            this._memento._stores[to[0]].indices[to[1]] = this._memento._stores[from[0]].indices[from[1]]
            delete this._memento._stores[from[0]].indices[from[1]]
            this._operations.push({ method: 'rename', type: 'index', from, to })
        } else {
            Memento.Error.assert(this._memento._stores[from] != null, [ 'DOES_NOT_EXIST', 'store' ])
            Memento.Error.assert(this._memento._stores[to] == null, [ 'ALREADY_EXISTS', 'store' ])
            this._memento._stores[to] = this._memento._stores[from]
            delete this._memento._stores[from]
            this._operations.push({ method: 'rename', type: 'store', from, to })
        }
    }

    async remove (name) {
        if (Array.isArray(name)) {
            Memento.Error.assert(this._memento._stores[name[0]] != null, [ 'DOES_NOT_EXIST', 'store' ])
            Memento.Error.assert(this._memento._stores[name[0]].indices[name[1]] != null, [ 'DOES_NOT_EXIST', 'index' ])
            const qualifier = path.join('staging', `index.${this._temporary++}`)
            this._operations.push({ method: 'remove', type: 'index', name, qualifier })
            delete this._memento._stores[name]
        } else {
            Memento.Error.assert(this._memento._stores[name] != null, [ 'DOES_NOT_EXIST', 'store' ])
            const qualifier = path.join('staging', `store.${this._temporary++}`)
            this._operations.push({ method: 'remove', type: 'store', name, qualifier })
            delete this._memento._stores[name]
        }
    }
}

class Memento {
    static ASC = Symbol('ascending')

    static DSC = Symbol('decending')

    static Error = Interrupt.create('Memento.Error', {
        IO_ERROR: 'i/o error',
        ALREADY_EXISTS: '%s already exists',
        DOES_NOT_EXIST: '%s does not exist',
        INVALID_RENAME: 'the stores for an index rename must be the same',
        ROLLBACK: 'transaction rolled back'
    })

    // Going to step back back from syntax bashing until I've actually put a
    // feature like joins to use. Instead I'll have some static functions that
    // implement the features I want, because I'm considering interpretations
    // that do not account for joins. Ultimately, we are going to want to apply
    // limits and filters prior to applying joins, though.

    //
    static async slurp (iterator) {
        const slurp = []
        for await (const items of iterator) {
            for (const item of items) {
                slurp.push(item)
            }
        }
        return items
    }

    // This will be enough to implement `min`/`max` where needed.
    static async first (iterator) {
        for await (const items of iterator) {
            for (const item of items) {
                return item
            }
        }
        return null
    }

    constructor (destructible, options) {
        this.destructible = destructible
        this.deferrable = destructible.durable($ => $(), { countdown: 1 }, 'deferrable')
        this.destructible.destruct(() => this.deferrable.decrement())
        // **TODO** Need to wait for mutators and snapshots to complete before
        // we completely decrement. Could we just increment and decrement
        // deferrable to do that?
        this._stores = {}
        assert(options.pages)
        this.pages = options.pages
        this._cacheSize = 1024 * 1024 * 256
        const directory = this.directory = options.directory
        this._rotator = options.rotator
        this._rotator.deferrable.increment()
        this._comparators = coalesce(options.comparators, {})
        this._fracture = new Fracture(destructible.durable($ => $(), 'merger'), {
            turnstile: options.turnstile,
            value: () => ({ merges: [] }),
            worker: this._merge.bind()
        })
        this._fracture.deferrable.increment()
        this.deferrable.destruct(() => {
            this.deferrable.ephemeral($ => $(), 'shutdown', async () => {
                if (this._fracture)
                await this._fracture.drain()
                this._fracture.deferrable.decrement()
                this._rotator.deferrable.decrement()
            })
        })
        // **TODO** Amalgamator should share it's choose a branch leaf size logic.
    }

    static async _open (destructible, { directory, turnstile, version, comparators, options }) {
        const writeahead = new WriteAhead(destructible.durable($ => $(), 'writeahead'), turnstile, await WriteAhead.open({ directory: path.resolve(directory, 'wal') }))
        const rotator = new Rotator(destructible.durable($ => $(), 'rotator'), await Rotator.open(writeahead), { size: 1024 * 1024 / 4 })
        const memento = new Memento(destructible, { turnstile, directory, rotator, comparators, pages: options.pages })
        for (const dir of (await fs.readdir(path.join(directory, 'stores')))) {
            const name = [ dir ]
            await memento._store({
                name: name[0],
                qualifier: path.join('stores', name[0]),
                options: options,
                create: false
            })
            for (const dir of (await fs.readdir(path.join(directory, 'stores', name[0], 'indices')))) {
                name[1] = dir
                await memento._index({
                    name: name,
                    qualifier: path.join('stores', name[0], 'indices', name[1]),
                    options: options,
                    create: false
                })
            }
        }
        return memento
    }
    //

    // Open runs the upgrade system. Version numbers are kept on the file system
    // as directories in the versions directory. No other place to keep them.
    // The write-ahead log is always rotating. It just doens't belong in any of
    // the trees. Not sure what to do with users who delete just some of the
    // stuff in their database directory. They used to be directories, but now
    // they are just empty files so that no one decides to prune them.

    // **TODO** Ensure that we rotate and rotate good to get all the temporary
    // keys out of the write-ahead log when we run a schema upgrade.

    //
    static async open ({
        destructible = new Destructible($ => $(), 'memento'),
        turnstile = new Turnstile(destructible.durable($ => $(), { isolated: true }, 'turnstile')),
        directory,
        version = 1,
        comparators = {}
    } = {}, upgrade) {
        // Run a recovery of a failed schema change, or any schema change since
        // we always for schema changes into recovery to exercise the system.
        const journalist = await Journalist.create(directory)
        const messages = journalist.messages.slice(0)
        if (journalist.state == Journalist.COMMITTING) {
            await journalist.commit()
        } else {
            await journalist.dispose()
        }
        // Now we can hold our breath and obliterate the temporary directory.
        await fs.rmdir(path.join(directory, 'staging'), { recursive: true })
        // We determine if this is a Memento directory by looking at the
        // directory contents. It a strict match, but I won't know what to say
        // to users who mess with the database directory until I meet them and
        // hear what they have to say for themselves.
        const list = async () => {
            try {
                return await fs.readdir(directory)
            } catch (error) {
                rescue(error, [{ code: 'ENOENT' }])
                await fs.mdkir(directory, { recursive: true })
                return await list()
            }
        }
        const dirs = await list()
        const subdirs = [ 'stores', 'indices', 'wal', 'schema' ].sort()
        if (dirs.length == 0) {
            for (const dir of subdirs) {
                await fs.mkdir(path.resolve(directory, dir))
            }
            await fs.writeFile(path.resolve(directory, 'version.json'), JSON.stringify(0))
        }
        // Versions are stored as files and we use the file name to determine
        // the most recent version of the database. We do something similar with
        // Strata file-system storage to track instance, however if we where to
        // lose those files, we could scan the directories to find the next
        // instance. Here, if the user loses the instance we're not going to
        // know what version of the database we're working with. It will still
        // open, I suppose, but we won't know how to upgrade it, so all it not
        // lost, it's just a programming task for the user to fish out their
        // data into a new database. Any SQL database would be in a similar
        // bind, possibly worse, if their schema tables where corrupted.
        //
        // Perhaps we ought to have a `version.json` and that will give the user
        // pause before they decide to tidy it away.
        const latest = JSON.parse(await Memento.Error.resolve(fs.readFile(path.join(directory, 'version.json'), 'utf8'), 'IO_ERROR'))
        const options = {
            handles: new Operation.Cache(new Magazine),
            turnstile: turnstile,
            pages: new Magazine
        }
        // Schema upgrades use a mutator that has the creation, rename and
        // delete functions. When we create a new store or index we create a new
        // Amalgamator in a `staging` directory. If we rollback we can just
        // delete the staging directory when we reopen. Otherwise, we suffle
        // these files out of the `staging` directory and into their new homes
        // in the `stores` and `indices` directory.
        if (latest < version) {
            const memento = await Memento._open(destructible.ephemeral($ => $(), 'schema'), {
                turnstile, directory, version, comparators, options
            })
            const schema = new Schema(memento, { target: version, current: latest }, options)
            try {
                await upgrade(schema)
            } catch (error) {
                await schema._rollback()
                await memento.destructible.destroy().promise
                rescue(error, [ Symbol, ROLLBACK ])
                throw new Memento.Error('ROLLBACK')
            }
            // Proceding with commit. We pass false to `Schema._commit` so that
            // it does not write the version to the write-ahead log.
            // TODO Why?
            await schema._commit(false)

            // We want to rotate the staging trees of the Amalgamators into
            // their primary directory-based b-trees. This has to do with the
            // fact that the relevant blocks in the write-ahead log for a
            // paritcular Amalgamator are keyed on a key of our choosing and we
            // chose that key based on the directory name in which the
            // Amalgamator is stored. We're about to change that directory name
            // so we clear out the write-ahead log so we don't lose anything
            // with the rename.
            await memento._rotator.locker.rotate().promise
            //

            // And one more time for good measure.
            await memento._rotator.locker.rotate().promise
            //

            // Now we wait for the temporary Memento to shutdown which will
            // flush the rotations.
            await memento.destructible.destroy().promise
            //

            // Journalist is a utility that will perform a series of atomic file
            // operations as part of a single atomic operation.
            const journalist = await Journalist.create(directory)
            // We don't want to run an ordinary mutator commit. The file shuffle
            // prepare could still fail. The upserts and deletes of the schema
            // change should only take effect if we successfully prepare our
            // file suffle.
            //
            // We write the version for the schema as part of the atomic file
            // suffle operation.
            journalist.message(Buffer.from(JSON.stringify(schema._transaction.mutation.version)))
            // We now move our temporary files into place.
            for (const operation of schema._operations) {
                switch (operation.method) {
                case 'create': {
                        const { type, qualifier, name } = operation
                        if (type == 'store') {
                            journalist.mkdir(path.join('stores', name))
                            journalist.mkdir(path.join('stores', name, 'indices'))
                            journalist.rename(path.join(qualifier, 'tree'), path.join('stores', name, 'tree'))
                            journalist.rename(path.join(qualifier, 'key.json'), path.join('stores', name, 'key.json'))
                        } else {
                            journalist.rename(qualifier, path.join('stores', name[0], 'indices', name[1]))
                        }
                    }
                    break
                case 'rename': {
                        const { type, from, to } = operation
                        if (type == 'store') {
                            journalist.rename(path.join('stores', from), path.join('stores', to))
                        } else {
                            journalist.rename(path.join('stores', from[0], 'indices', from[1]), path.join('stores', to[0], 'indices', to[1]))
                        }
                    }
                    break
                case 'remove': {
                        const { type, name, qualifier } = operation
                        if (type == 'store') {
                            journalist.rename(path.join('stores', name), qualifier)
                        } else {
                            journalist.rename(path.join('stores', name[0], 'indices', name[1]), qualifier)
                        }
                    }
                    break
                }
            }
            //

            // We create a new version file and we'll rotate it into place.
            await fs.mkdir(path.join(directory, 'staging'), { recursive: true })
            await fs.writeFile(path.join(directory, 'staging', 'version.json'), JSON.stringify(version))
            journalist.unlink('version.json')
            journalist.rename(path.join('staging', 'version.json'), 'version.json')
            //

            // We prepare our commit, but we then reopen the database so it gets
            // run as a recovery as an exercise.
            await journalist.prepare()
            // **TODO** Belongs in Memento shutdown.
            await options.handles.shrink(0)
            // **TODO** Here we call open again. The journal is run as if it
            // where a recovery and the upgrade section is skipped because are
            // up-to-date after the journal runs. (We should probably flag
            // whether this is expected or not and warn the user that we
            // recovered.)
            return Memento.open({ destructible, turnstile, directory, version, comparators })
        }
        const memento = await Memento._open(destructible.ephemeral($ => $(), 'memento'), { directory, turnstile, version, comparators, options })
        if (journalist.messages.length) {
            const version = JSON.parse(String(journalist.messages.shift()))
            await memento._rotator.commit(Fracture.stack(), version)
            await journalist.dispose()
        }
        return memento
    }

    indices (name) {
        return Object.keys(this._stores[name].indices)
    }

    //

    // TODO Okay, so how do we say that any iterators should recalculate with a
    // new `Amalgamate.iterator()`? Use a count.

    //
    async _merge ({ stack, value: { merges } }) {
        for (const { mutation, snapshot, transaction } of merges) {
            assert(mutation.qualifier.length == 1)
            // For indexes we use our Amalgamator map iterator to iterate over
            // the snapshot we took using keys from the records in the in-memory
            // stage. The in memory stage is already sorted so the map iterator
            // should only ever have to visit any page once when iterating the
            // snapshot. With that iterator, for each index, we extract the
            // existing value from the snapshot iterator item and the new value
            // from the in-memory array value. If it has changed we create a
            // deletion record. You'll notice that we create the in-memory stage
            // for the index here, in one go, in a one liner that prepends all
            // the deletes to existing appends array.
            if (Object.keys(mutation.indices).length != 0) {
                const iterator = mutation.store.amalgamator.map(snapshot, mutation.appends[1], {
                    extractor: entry => {
                        return entry.key[0]
                    }
                })
                const trampoline = new Trampoline
                while (! iterator.done) {
                    iterator.next(trampoline, entries => {
                        for (const name in mutation.indices) {
                            const deletions = []
                            const { amalgamator, extractor, comparator, keyLength } = mutation.indices[name].store
                            for (const entry of entries) {
                                for (const item of entry.items) {
                                    // **TODO** Why do I have to encase this in
                                    // an array?
                                    const previous = extractor([ item.parts[1] ])
                                    if (entry.value.parts[0].method == 'remove' ||
                                        comparator(extractor([ entry.value.value ]).slice(0, keyLength), previous.slice(0, keyLength)) != 0
                                    ) {
                                        deletions.push({
                                            key: [ previous ],
                                            parts: [{ method: 'remove' }]
                                        })
                                    }
                                }
                            }
                            mutation.indices[name].appends[1] = deletions.concat(mutation.indices[name].appends[1])
                        }
                    })
                    while (trampoline.seek()) {
                        await trampoline.shift()
                    }
                }
            }
            // Now we can perform our merges. Because we are merging into the
            // write-ahead only staging tree, we do not deal with any fractured
            // procedures and do not displace ourselves (Fracture talk.) Nor do
            // we wait on any Fracture futures.
            await mutation.store.amalgamator.merge(stack, transaction, mutation.appends[1])
            const { indices } = mutation
            for (const name in indices) {
                await indices[name].store.amalgamator.merge(stack, transaction, indices[name].appends[1])
            }
            mutation.appends.pop()
            for (const name in indices) {
                indices[name].appends.pop()
            }
            mutation.series++
        }
    }

    async _store ({ name, qualifier, create, options }) {
        const directory = this.directory

        const comparisons = JSON.parse(await fs.readFile(path.join(directory, qualifier, 'key.json'), 'utf8'))

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
                typeof part.type == 'string' ? this._comparators[part.type] : ASCENSION_TYPE[part.type],
                part.direction
            ]
        }))

        const amalgamator = await this._rotator.open(Fracture.stack(), qualifier.replace('/', '.'), {
            handles: options.handles.subordinate(),
            directory: path.join(directory, qualifier, 'tree'),
            create: create,
            key: qualifier,
            checksum: () => '0',
            extractor: extractor,
            // **TODO** Remove wrapper functions.
            serializer: {
                key: {
                    serialize: key => Verbatim.serialize(key),
                    deserialize: parts => Verbatim.deserialize(parts)
                },
                parts: {
                    serialize: parts => Verbatim.serialize(parts),
                    deserialize: parts => Verbatim.deserialize(parts)
                }
            }
        }, {
            pages: options.pages.subordinate(),
            turnstile: options.turnstile,
            comparator: comparator,
            transformer: function (operation) {
                if (operation.parts[0].method == 'insert') {
                    return {
                        method: 'insert',
                        key: operation.key[0],
                        parts: [ operation.value ]
                    }
                }
                return {
                    method: 'remove',
                    key: operation.key[0]
                }
            },
            primary: options.primary || {
                leaf: { split: 256, merge: 32 },
                branch: { split: 256, merge: 32 },
            },
            stage: options.stage || {
                leaf: { split: 256, merge: 32 },
                branch: { split: 256, merge: 32 },
            }
        })
        this._stores[name] = {
            qualifier,
            amalgamator,
            indices: {},
            comparisons,
            comparator,
            getter: whittle(comparator, key => key[0])
        }
    }

    async _index ({ name, qualifier, create, options }) {
        const directory = this.directory

        const store = this._stores[name[0]]

        const key = JSON.parse(await fs.readFile(path.join(directory, qualifier, 'key.json'), 'utf8'))

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
        }))

        const amalgamator = await this._rotator.open(Fracture.stack(), qualifier.replace('/', '.'), {
            handles: options.handles.subordinate(),
            directory: path.join(directory, qualifier, 'tree'),
            create: create,
            key: qualifier,
            checksum: () => '0',
            extractor: parts => parts[0],
            // **TODO** Remove wrapper functions.
            serializer: {
                key: {
                    serialize: key => Verbatim.serialize(key),
                    deserialize: parts => Verbatim.deserialize(parts)
                },
                parts: {
                    serialize: parts => Verbatim.serialize(parts),
                    deserialize: parts => Verbatim.deserialize(parts)
                }
            }
        }, {
            pages: options.pages.subordinate(),
            turnstile: options.turnstile,
            comparator: comparator,
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
            primary: options.primary || {
                leaf: { split: 256, merge: 32 },
                branch: { split: 256, merge: 32 },
            },
            stage: options.stage || {
                leaf: { split: 256, merge: 32 },
                branch: { split: 256, merge: 32 },
            }
        })

        const partials = []
        for (let i = 0; i < key.comparisons.length; i++) {
            partials.push(function (end) {
                return whittle(comparator, key => key[0].slice(0, end))
            } (i + 1))
        }

        this._stores[name[0]].indices[name[1]] = {
            amalgamator, comparator, extractor, partials, qualifier,
            keyLength: key.comparisons.length,
            getter: partials[key.comparisons.length - 1]
        }
    }

    async snapshot (block) {
        this.deferrable.increment()
        const snapshot = new Snapshot(this)
        try {
            return await block(snapshot)
        } finally {
            snapshot.release()
            this.deferrable.decrement()
        }
    }

    async mutator (block) {
        this.deferrable.increment()
        const mutator = new Mutator(this)
        let result
        try {
            do {
                try {
                    result = await block(mutator)
                } catch (error) {
                    await mutator._rollback()
                    rescue(error, [ Symbol, ROLLBACK ])
                    break
                }
            } while (! await mutator._commit())
        } finally {
            this.deferrable.decrement()
        }
        return result
    }

    close () {
        return this.destructible.destroy().promise
    }
}

module.exports = Memento
