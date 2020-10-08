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


```
npm install memento
```

```javascript
async main () {
    const Memento = require('memento')
    const memento = new Memento({ directory: './memento' })

    await memento.open(async schema => {
        switch (schema.version) {
        case 1:
            await schema.store('president', { lastName: String, fristName: String })
            await schema.index([ 'president', 'state' ], { state: String })
            break
        }
    }, 1)

    await memento.mutator(async mutator => {
        mutator.set('employee', {
            firstName: 'George',
            lastName: 'Washington',
            state: 'VA',
            order: [ 1 ]
        })
        mutator.set('employee', {
            firstName: 'John',
            lastName: 'Adams',
            state: 'MA',
            order: [ 2 ]
        })
        mutator.set('employee', {
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
                console.log(`${president.lastName}, ${president.firstName}`)
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
                console.log(`${president.lastName}, ${president.firstName}`)
            }
        }
    })
}

main()
```
