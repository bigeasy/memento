// # Memento
//
// Memento is a pure JavaScript database. Is is an actual database. Writes data
// to file, pages it in and out of memory as needed. Memento is concurrent,
// indexed, transactional and persistent with a contemporary `async`/`await`
// interface.
//
// This unit test represents a tour of Memento and is a stub for some actual
// documentation that I may write someday. It is part of Memento's unit test
// suite and lives in the Memento repository. You can run this readme yourself.
//
// ```text
// git clone git@github.com:bigeasy/memento.git
// cd memento
// npm install --no-package-lock --no-save
// node test/readme.t.js
// ```
//
// Note that you should run `node test/readme.t.js` and not `npm test` to see
// the output from this walk-through.
//
// This walk-through uses the [Proof](https://github.com/bigeasy/proof) unit
// test framework. It will setup an `async` function that will catch and report
// any exceptions. We'll use the `okay` function to assert the points we make
// about Memento.

//
require('proof')(2, async okay => {
    // To use Memento in your project it you'll want to install it from NPM.
    //
    // ```text
    // npm install memento
    // ```
    //
    // You can then include Memento in your program with `require`.
    //
    // ```javascript
    // const Memento = require('memento')
    // ```
    //
    // But, because we're running in the Memento project we have to use a
    // relative path to the root index. Use the above, not the below.

    //
    const Memento = require('..')

    okay(Memento != null, 'require')

    // We're going to do some file manipulation in this walk-though.
    const path = require('path')
    const fs = require('fs').promises

    // We're going to reset our example directory.
    const directory = path.resolve(__dirname, './tmp/readme')
    await fs.rmdir(directory, { recursive: true })
    await fs.mkdir(directory, { recursive: true })

    // Memento has a lot of options. **TODO** come back and write a function
    // call of this.
    //
    // We create a database object with the static `async Memento.open`
    // function. It returns an open database ready for use.
    //
    // The first argument to `async Memento.open()` is an options object.
    //
    // The second argument is an `async` database upgrade function. You are only
    // able to create new stores and indices in the update function. Once the
    // database is open you're not allowed to make any schema changes.

    //
    let memento = await Memento.open({ directory }, async schema => {
        switch (schema.version) {
        case 1:
            await schema.store('president', { lastName: String, firstName: String })
            break
        }
    })

    // In order to add or remove data from the database you invoke
    // `Memento.mutator()` with an `async` mutation function.

    // The mutator function represents an atomic transaction against the
    // database. Changes made within the function are only visible within the
    // function. They only become visible outside of the function when the
    // function returns successfully.

    // If the function raises and exception, the changes are rolled back.

    //
    await memento.mutator(async mutator => {
        mutator.set('president', { firstName: 'George', lastName: 'Washington' })
        const got = await mutator.get('president', [ 'Washington', 'George' ])
        okay(got, {
            firstName: 'George', lastName: 'Washington'
        }, 'isolated view of inserted record')
    })

    // You'll notice that the `mutator.set()` method is a synchronous function.
    // This is because we want inserts and deletes to be fast. Rather than
    // performing asynchronous file operations for each insert and delete, we
    // cache the changes in memory and write them out in batches.
    //
    // The `mutator.get()` method on the other hand is an `async` function. We
    // have to go and check the database to see if the value is there and
    // compare it with our write cache. Checking the database may require a read
    // operation, or it may not, depending on the database cache.
    //
    // So, `mutator.set()` ought to be pretty quick, making batch inserts
    // relatively painless. `async mutator.get()` not so quick because it has to
    // go out through the `Promise`s event loop.
    //
    // We'll make up for this discrepancy when we look at ranged queries,
    // iterators, and joins.

    // **TODO** A snapshot.

    await memento.close()
})
