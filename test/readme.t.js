// [![Actions Status](https://github.com/bigeasy/memento/workflows/Node%20CI/badge.svg)](https://github.com/bigeasy/memento/actions)
// [![codecov](https://codecov.io/gh/bigeasy/memento/branch/master/graph/badge.svg)](https://codecov.io/gh/bigeasy/memento)
// [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
//
// A pure-JavaScript `async`/`await` indexed, persistant database.
//
// | What          | Where                                         |
// | --- | --- |
// | Discussion    | https://github.com/bigeasy/memento/issues/1   |
// | Documentation | https://bigeasy.github.io/memento             |
// | Source        | https://github.com/bigeasy/memento            |
// | Issues        | https://github.com/bigeasy/memento/issues     |
// | CI            | https://travis-ci.org/bigeasy/memento         |
// | Coverage:     | https://codecov.io/gh/bigeasy/memento         |
// | License:      | MIT                                           |
//
// Memento installs from NPM.

// Memento is a database that supports atomic, isolated transactions, written to a
// write-ahead log and synced for durability in the event of system crash, and
// merged into b-trees for fast retrieval. It reads from an in memory page cache
// and evicts pages from the cache when they reach a user specified memory limit.
//
// Memento is written in pure JavaScript.
//
// Memento provides a contemporary `async`/`await` interface.
//
// This `README.md` is also a unit test using the
// [Proof](https://github.com/bigeasy/proof) unit test framework. We'll use the
// Proof `okay` function to assert out statements in the readme. A Proof unit test
// generally looks like this.

require('proof')(14, async okay => {
    const path = require('path')
    const fs = require('fs').promises
    const { coalesce } = require('extant')

    const directory = path.resolve(__dirname, './tmp/readme')
    await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
    await fs.mkdir(directory, { recursive: true })

    {
        // We create a database object with the static `async Memento.open` function. It
        // returns an open database ready for use.
        //
        // The first argument to `async Memento.open()` is an options object.
        //
        // The second argument is an `async` database upgrade function. You are only able
        // to create new stores and indices in the update function. Once the database is
        // open you're not allowed to make any schema changes.

        const directory = path.resolve(__dirname, './tmp/readme')
        const memento = await Memento.open({ directory }, async schema => {
            switch (schema.version.current + 1) {
            case 1:
                await schema.create('president', { lastName: String, firstName: String })
            }
        })

        // In order to add or remove data from the database you invoke `memento.mutator()`
        // with an `async` mutation callback function. The mutation function will be called
        // with a `Mutator` object.
        //
        // The mutator function represents an atomic transaction against the database.
        // Changes made within the function are only visible within the function. They only
        // become visible outside of the function when the function returns successfully.
        //
        // If the function raises and exception, the changes are rolled back.

        await memento.mutator(async mutator => {
            mutator.set('president', { firstName: 'George', lastName: 'Washington', state: 'VA' })
            const got = await mutator.get('president', [ 'Washington', 'George' ])
            okay(got, {
                firstName: 'George', lastName: 'Washington', state: 'VA'
            }, 'isolated view of inserted record')
        })

        // When we only want to read the database we use a `mutator.snapshot()` with an
        // `async` snapshot callback function. The snapshot function will be called with a
        // `Snapshot` object.
        //
        // Use the `Snapshot`, the snapshot function can perform read-only requests on the
        // database. The `Snapshot` will have a point in time view of the database. Any
        // changes made by mutators that commit after the snapshot callback function begins
        // will not be visible to the snapshot function.

        await memento.snapshot(async snapshot => {
            const got = await snapshot.get('president', [ 'Washington', 'George' ])
            okay(got, {
                firstName: 'George', lastName: 'Washington', state: 'VA'
            }, 'snapshot view of inserted record')
        })

        // When you are done with Memento you close it.

        await memento.close()
    }

    // ### Create, Retrieve, Update and Delete
    //
    // Let's create a database.

    {
        // Insert is done with `set`.
        //
        // We're going to reopen the database. Usually you'll only have one spot in your
        // program where you open your database and it will have a schema function that
        // builds or migrates the database. In our example we are reopening an existing
        // database that has already been built to the schema function should not be
        // called. In our test here, we're going to assert that it is not called with an
        // exception.

        const directory = path.resolve(__dirname, './tmp/readme')
        const memento = await Memento.open({ directory }, async schema => {
            throw new Error('should not be called')
        })

        // We use `set` to insert or update a record into the database. We use `get` to get
        // a single record out of the database.

        await memento.mutator(async mutator => {
            mutator.set('president', { firstName: 'Jack', lastName: 'Adams', state: 'NY' })
            const get = await mutator.get('president', [ 'Adams', 'Jack' ])
            okay(get, { firstName: 'Jack', lastName: 'Adams', state: 'NY' }, 'isolated retrieve')
        })

        // Once the mutator is complete other snapshots and mutators will see the written
        // record.

        await memento.snapshot(async mutator => {
            const get = await mutator.get('president', [ 'Adams', 'Jack' ])
            okay(get, { firstName: 'Jack', lastName: 'Adams', state: 'NY' }, 'snapshot retrieve')
        })

        // We update records using `set` as well. Let's fix the home state of John Adams.

        await memento.mutator(async mutator => {
            mutator.set('president', { firstName: 'Jack', lastName: 'Adams', state: 'MA' })
            const get = await mutator.get('president', [ 'Adams', 'Jack' ])
            okay(get, { firstName: 'Jack', lastName: 'Adams', state: 'MA' }, 'retrieve')
        })

        // Note that we can't change the name of one of our presidents because we are using
        // last name and first name as the key. You would have to delete and insert.
        //
        // Let's delete "Jack Adams".

        await memento.mutator(async mutator => {
            mutator.unset('president', [ 'Adams', 'Jack' ])
            const get = await mutator.get('president', [ 'Adams', 'Jack' ])
            okay(get, null, 'deleted')
        })

        // Still removed when we search in a subsequent snapshot.

        await memento.snapshot(async mutator => {
            const get = await mutator.get('president', [ 'Adams', 'Jack' ])
            okay(get, null, 'deleted')
        })

        // Let's insert John Adams.

        await memento.mutator(async mutator => {
            mutator.set('president', { lastName: 'Adams', firstName: 'John', state: 'MA' })
        })


        // Close the database.

        await memento.close()
    }

    // ### Stores and Indices
    //
    // Index example.

    {
        const directory = path.resolve(__dirname, './tmp/readme')
        const memento = await Memento.open({ directory, version: 2 }, async schema => {
            switch (schema.version.current + 1) {
            case 1:
                await schema.create('president', { lastName: String, firstName: String })
            case 2:
                await schema.create([ 'president', 'state' ], { state: String })
            }
        })

        // We can use our index immediately after creating it.

        await memento.snapshot(async snapshot => {
            const got = await snapshot.get([ 'president', 'state' ], [ 'MA' ])
            okay(got, { firstName: 'John', lastName: 'Adams', state: 'MA' }, 'get by index')
        })

        // When you get by an index the index can match multiple records. The first one is
        // returned.

        await memento.mutator(async mutator => {
            mutator.set('president', { firstName: 'Thomas', lastName: 'Jefferson', state: 'VA' })
            const got = await mutator.get([ 'president', 'state' ], [ 'VA' ])
            okay(got, { firstName: 'Thomas', lastName: 'Jefferson', state: 'VA' }, 'get first by index')
        })

        // TODO Make a point of closing here and opening in the next section.
        //
        // ### Cursors and Iteration
        //
        // So far we've only seen get which returns a single entry.

        await memento.snapshot(async snapshot => {
            const gathered = []
            for await (const presidents of snapshot.cursor('president')) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, [{
                firstName: 'John', lastName: 'Adams', state: 'MA'
            }, {
                firstName: 'Thomas', lastName: 'Jefferson', state: 'VA'
            }, {
                firstName: 'George', lastName: 'Washington', state: 'VA'
            }], 'cursor')
        })

        // Let's add some more presidents for the sake of the index search.

        await memento.mutator(async mutator => {
            mutator.set('president', { firstName: 'James', lastName: 'Monroe', state: 'VA' })
            mutator.set('president', { firstName: 'John Quincy', lastName: 'Adams', state: 'MA' })
        })

        // Iterating over an index.

        await memento.snapshot(async snapshot => {
            const gathered = []
            for await (const presidents of snapshot.cursor([ 'president', 'state' ], [ 'VA' ])) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, [{
                firstName: 'Thomas', lastName: 'Jefferson', state: 'VA'
            }, {
                firstName: 'James', lastName: 'Monroe', state: 'VA'
            }, {
                firstName: 'George', lastName: 'Washington', state: 'VA'
            }], 'cursor')
        })

        // Reversing an index.

        await memento.snapshot(async snapshot => {
            const gathered = []
            for await (const presidents of snapshot.cursor([ 'president', 'state' ], [ 'MA' ]).reverse()) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, [{
                firstName: 'John Quincy', lastName: 'Adams', state: 'MA'
            }, {
                firstName: 'John', lastName: 'Adams', state: 'MA'
            }], 'cursor')
        })

        // We'll shutdown the database before moving onto isolation with snapshots and
        // mutators.

        await memento.close()
    }

    // ### Snapshots versus Mutators
    //
    // You use mutators to change data. None of the changes made by the mutator are
    // visible to any of the other snapshots or mutators until the mutator returns.

    {
        // Let's reopen the database. In our program we'll have a single open stanza for a
        // database so we'll repeat the schema update block here.
        //
        // **TODO** Maybe have an `openVersion1` and `openVersion2` function example.

        const directory = path.resolve(__dirname, './tmp/readme')
        const memento = await Memento.open({ directory, version: 2 }, async schema => {
            switch (schema.version.current + 1) {
            case 1:
                await schema.create('president', { lastName: String, firstName: String })
            case 2:
                await schema.create([ 'president', 'state' ], { state: String })
            }
        })

        const resolves = {}

        const promises = {
            wrote: new Promise(resolve => resolves.wrote = resolve),
            reading: new Promise(resolve => resolves.reading = resolve)
        }

        const promise = memento.mutator(async mutator => {
            await promises.reading
            mutator.set('president', { firstName: 'Andrew', lastName: 'Jackson', state: 'SC' })
            resolves.wrote()
        })

        await memento.snapshot(async snapshot => {
            resolves.reading()
            await promises.wrote
            okay(await snapshot.get('president', [ 'Jackson', 'Andrew' ]), null, 'isolated write not visible')
        })

        // TODO Show that we can now do a snapshot and Andrew Jackson is present.
        //
        // We'll close the database before moving onto inner and outer joins.

        await memento.close()
    }

    // ### Inner and Outer Joins

    {
        const directory = path.resolve(__dirname, './tmp/readme')
        const memento = await Memento.open({ directory, version: 3 }, async schema => {
            switch (schema.version.current + 1) {
            case 1:
                await schema.create('president', { lastName: String, firstName: String })
            case 2:
                await schema.create([ 'president', 'state' ], { state: String })
            case 3:
                await schema.create('state', { code: String })
                schema.set('state', { code: 'MA', name: 'Massachusettes' })
                schema.set('state', { code: 'NY', name: 'New York' })
                schema.set('state', { code: 'SC', name: 'South Carolina' })
                schema.set('state', { code: 'VA', name: 'Virginia' })
            }
        })

        await memento.snapshot(async snapshot => {
            const join = snapshot.cursor('president', [ 'Jackson', 'Andrew' ]).join('state', $ => [ $[0].state ])
            const gathered = []
            for await (const items of join) {
                for (const item of items) {
                    gathered.push(item)
                }
            }
            okay(gathered, [[{
                firstName: 'Andrew', lastName: 'Jackson', state: 'SC'
            }, {
                code: 'SC', name: 'South Carolina'
            }], [{
                firstName: 'Thomas', lastName: 'Jefferson', state: 'VA'
            }, {
                code: 'VA', name: 'Virginia'
            }], [{
                firstName: 'James', lastName: 'Monroe', state: 'VA'
            }, {
                code: 'VA', name: 'Virginia'
            }], [{
                firstName: 'George', lastName: 'Washington', state: 'VA'
            }, {
                code: 'VA', name: 'Virginia'
            }]], 'inner join')
        })

        // We'll close the database before moving onto migrations.

        await memento.close()
    }
})

// You can run this unit test yourself to see the output from the various
// code sections of the readme.

// The `'memento'` module exports a single `Memento` object.

const Memento = require('..')

// You'll notice that the `mutator.set()` method is a synchronous function. This is
// because we want inserts and deletes to be fast. Rather than performing
// asynchronous file operations for each insert and delete, we cache the changes in
// memory and write them out in batches.
//
// The `mutator.get()` method on the other hand is an `async` function. We have to
// go and check the database to see if the value is there and compare it with our
// write cache. Checking the database may require a read operation, or it may not,
// depending on the database cache.
//
// So, `mutator.set()` ought to be pretty quick, making batch inserts relatively
// painless. `async mutator.get()` not so quick because it has to go out through
// the `Promise`s event loop.
//
// We'll make up for this discrepancy when we look at ranged queries, iterators,
// and joins.
//
// Note that the `Snapshot` object is only valid during the invocation of the
// snapshot callback function. If you attempt to save it and use later you will get
// undefined behavior. Currently, there are no assertions to keep you from doing
// this, just don't do it.

// ### Migrations
//
// ### API
//
// `memento = Memento.open(options, async upgrade => {})`
//
//  * `options`
//      * `destructible` &mdash; Optional instance of Destructible for structured
//         concurrency management of the Memento instance.
//      * `turnstile` &mdash; Optional Turnstile to manage parallel writes to file
//         system.
//      * `directory` &mdash; Directory in which to store data files.
//      * `version` &mdash; Migration version.
//      * `comparisons` &mdash; Optional of one or more comparision functions to use
//         for collation.
//  * `upgrade` &mdash;
//
// `memento.snapshot(async snapshot => {})`
//
// `memento.mutator(async mutator => {})`
