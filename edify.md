[![Actions Status](https://github.com/bigeasy/memento/workflows/Node%20CI/badge.svg)](https://github.com/bigeasy/memento/actions)
[![codecov](https://codecov.io/gh/bigeasy/memento/branch/master/graph/badge.svg)](https://codecov.io/gh/bigeasy/memento)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A pure-JavaScript `async`/`await` indexed, persistant database.

| What          | Where                                         |
| --- | --- |
| Discussion    | https://github.com/bigeasy/memento/issues/1   |
| Documentation | https://bigeasy.github.io/memento             |
| Source        | https://github.com/bigeasy/memento            |
| Issues        | https://github.com/bigeasy/memento/issues     |
| CI            | https://travis-ci.org/bigeasy/memento         |
| Coverage:     | https://codecov.io/gh/bigeasy/memento         |
| License:      | MIT                                           |

Memento installs from NPM.

```text
//{ "mode": "text" }
npm install memento
```
Memento is a database that supports atomic, isolated transactions, written to a
write-ahead log and synced for durability in the event of system crash, and
merged into b-trees for fast retrieval. It reads from an in memory page cache
and evicts pages from the cache when they reach a user specified memory limit.

Memento is written in pure JavaScript.

Memento provides a contemporary `async`/`await` interface.

This `README.md` is also a unit test using the
[Proof](https://github.com/bigeasy/proof) unit test framework. We'll use the
Proof `okay` function to assert out statements in the readme. A Proof unit test
generally looks like this.

```javascript
//{ "code": { "tests": 2 }, "text": { "tests": 4  } }
require('proof')(%(tests)d, async okay => {
    //{ "include": "test" }
    //{ "include": "testDisplay" }
})
```

```javascript
//{ "name": "testDisplay", "mode": "text" }
okay('always okay')
okay(true, 'okay if true')
okay(1, 1, 'okay if equal')
okay({ value: 1 }, { value: 1 }, 'okay if deep strict equal')
```

You can run this unit test yourself to see the output from the various
code sections of the readme.

```text
//{ "mode": "text" }
git clone git@github.com:bigeasy/memento.git
cd memento
npm install --no-package-lock --no-save
node test/readme.t.js
```

The `'memento'` module exports a single `Memento` object.

```javascript
//{ "name": "displayedRequire", "mode": "text" }
const Memento = require('memento')
```

```javascript
//{ "name": "test", "mode": "code" }
const Memento = require('..')

const path = require('path')
const fs = require('fs').promises
const { coalesce } = require('extant')

const directory = path.resolve(__dirname, './tmp/readme')
await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
await fs.mkdir(directory, { recursive: true })
```

```javascript
//{ "name": "test", "mode": "code" }
{
    //{ "include": "introduction" }
}
```

We create a database object with the static `async Memento.open` function. It
returns an open database ready for use.

The first argument to `async Memento.open()` is an options object.

The second argument is an `async` database upgrade function. You are only able
to create new stores and indices in the update function. Once the database is
open you're not allowed to make any schema changes.

```javascript
//{ "name": "introduction" }
const directory = path.resolve(__dirname, './tmp/readme')
const memento = await Memento.open({ directory }, async schema => {
    switch (schema.version.target) {
    case 1:
        await schema.store('president', { lastName: String, firstName: String })
        break
    }
})
```

In order to add or remove data from the database you invoke `memento.mutator()`
with an `async` mutation callback function. The mutation function will be called
with a `Mutator` object.

The mutator function represents an atomic transaction against the database.
Changes made within the function are only visible within the function. They only
become visible outside of the function when the function returns successfully.

If the function raises and exception, the changes are rolled back.

```javascript
//{ "name": "introduction" }
await memento.mutator(async mutator => {
    mutator.set('president', { firstName: 'George', lastName: 'Washington' })
    const got = await mutator.get('president', [ 'Washington', 'George' ])
    okay(got, {
        firstName: 'George', lastName: 'Washington'
    }, 'isolated view of inserted record')
})
```

You'll notice that the `mutator.set()` method is a synchronous function. This is
because we want inserts and deletes to be fast. Rather than performing
asynchronous file operations for each insert and delete, we cache the changes in
memory and write them out in batches.

The `mutator.get()` method on the other hand is an `async` function. We have to
go and check the database to see if the value is there and compare it with our
write cache. Checking the database may require a read operation, or it may not,
depending on the database cache.

So, `mutator.set()` ought to be pretty quick, making batch inserts relatively
painless. `async mutator.get()` not so quick because it has to go out through
the `Promise`s event loop.

We'll make up for this discrepancy when we look at ranged queries, iterators,
and joins.

Note that the `Snapshot` object is only valid during the invocation of the
snapshot callback function. If you attempt to save it and use later you will get
undefined behavior. Currently, there are no assertions to keep you from doing
this, just don't do it.

```javascript
//{ "mode": "text" }
let evilMutator
await memento.mutator(async mutator => {
    evilMutator = mutator
})
// No!
evilMutator.set('president', {
    firstName: 'John',
    lastName: 'Adams',
    terms: [ 1 ]
})
```

When we only want to read the database we use a `mutator.snapshot()` with an
`async` snapshot callback function. The snapshot function will be called with a
`Snapshot` object.

Use the `Snapshot`, the snapshot function can perform read-only requests on the
database. The `Snapshot` will have a point in time view of the database. Any
changes made by mutators that commit after the snapshot callback function begins
will not be visible to the snapshot function.

```javascript
//{ "name": "introduction" }
await memento.snapshot(async snapshot => {
    const got = await snapshot.get('president', [ 'Washington', 'George' ])
    okay(got, {
        firstName: 'George', lastName: 'Washington'
    }, 'snapshot view of inserted record')
})
```

When you are done with Memento you close it.

```javascript
//{ "name": "introduction" }
await memento.close()
```
