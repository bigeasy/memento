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
npm install memento
```

Memento is a pure JavaScript database. Is is an actual database. Writes data to
file, pages it in and out of memory as needed. Memento is concurrent, indexed
and persistent with a contemporary `async`/`await` interface.

```javascript
async main () {
    const Memento = require('memento')

    await memento = Memento.open({
        directory: './memento',
        version: 1
    }, async schema => {
        switch (schema.version) {
        case 1:
            await schema.store('president', { lastName: String, fristName: String })
            await schema.index([ 'president', 'state' ], { state: String })
            break
        }
    })

    await memento.mutator(async mutator => {
        mutator.set('president', {
            firstName: 'George',
            lastName: 'Washington',
            state: 'VA',
            order: [ 1 ]
        })
        mutator.set('president', {
            firstName: 'John',
            lastName: 'Adams',
            state: 'MA',
            order: [ 2 ]
        })
        mutator.set('president', {
            firstName: 'Thomas',
            lastName: 'Jefferson',
            state: 'VA',
            order: [ 3 ]
        })
        for await (const presidents of mutator.forward('president')) {
            for (const president of presidents) {
                console.log(`${president.lastName}, ${president.firstName}`)
            }
        }
        for await (const presidents of mutator.forward([ 'president', 'state' ], [ 'VA' ])) {
            for (const president of presidents) {
                console.log(`${president.lastName}, ${president.firstName} is from ${president.state}`)
            }
        }
    })

    await memento.snapshot(async snapshot => {
        for await (const presidents of snapshot.forward('president')) {
            for (const president of presidents) {
                console.log(`${president.lastName}, ${president.firstName}`)
            }
        }
        for await (const presidents of snapshot.forward([ 'president', 'state' ], [ 'VA' ])) {
            for (const president of presidents) {
                console.log(`${president.lastName}, ${president.firstName} is from ${president.state}`)
            }
        }
    })

    await memento.close()
}

main()
```

Memento concepts to document.

 * Schemas and schema versions.
 * Auto-commit.
 * Iteration.
 * Snapshots versus mutators.

## Interface


### `async Memento.open(options, updater)`

Opens a Memento data store creating it if necessary.

The properties of `options` include.

 * `directory` &mdash; Directory on the file system in which to store the data
 store.
 * `version` &mdash; _Optional_ desired version of the data store. If the value
 of the `version` property is greater than the `version` of the last successful
 invocation of an `updater` function the given `updater` function is inovked
 with the a `Schema` whose `Schema.version` is set to the given version value.
 If not given the default value is `1`.
 * `destructible` &mdash; _Optional_ instance of `Destructible` to use to manage
 the shutdown of the concurrent operations of the database. If not provided one
 will be created and the data store can be closed with a call to `close()`.

The given updater function provides a database migration facility and is
required because the only time new collections or indices can be created is
during this function. If no `version` property is provided in the `options`, the
`updater` function is called with a version number of `1`. After a successful
call of the to the `updater` function the version number is recorded. Subsequent
calls to open the data store will not invoke the `updater` unless the `options`
contains a `version` property that is greater than the `version` of the last
successful call to `updater`.
