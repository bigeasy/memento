## Tue Oct 20 21:35:33 CDT 2020

Need to find a way to delete from the commit record. I see now that I'd
implemented rotation in Amalgamate such that rotation causes all collections to
merge with optimizations for collections that do not require a merge. The
versions are grouped in the locker so after rotation the Locker can emit a
message with a set of all the versions that have been flushed by rotation.

We can delete these.

There is also the problem of a recovery where we don't have a perfect set. At
recovery we rotate to force anything that that was not cleared out at at close
to get flushed. Perhaps we simply return the versions that where given to us,
since they would have come from out commit storage.

## Sun Oct 11 00:30:36 CDT 2020

Documentation resources and inspiration. Writing documentation is hard. What is
compelling? What will make people want to use Memento? What makes documentation
easy to follow and understand?

Need to not only convey the simplicity of the interface, but also the concepts
like auto-commit, rollback, iteration, asynchronous versus synchronous,
snapshots versus mutators, schema and schema versions.

This diary entry will link to documentation that I thought was well written.

## Sun Oct 11 00:12:37 CDT 2020

Would appreciate some sort of fast join, one that doesn't require an `async` get
inside a loop. Could return an array of records.

```javascript
await memento.mutator(async mutator => {
    const iterator = memento.join('presidents', 'state', 'president.state')
                            .join('state', 'bird', 'state.bird', { outer: true })
    for await (const record of iterator) {
        for (const [ president, state, bird ] of reocords) {
            console.log(bird)
        }
    }
    const iterator = memento.forward('term', [ 'positional' ])
                            .join('term', [ 'inverse', $ => $[0].documentId ], $ => {
                                return $[0].position + 1 == $[1].position
                            })
                            .join('term', [ 'index', $ => $[0].documentId ], $=> {
                                return $[1].position + 1 == $[2].position
                            })
                            .join('document', [ $ => $[0].documentId ])
    for await (const records of iterator) {
        for (const record of records) {
            console.log(record.pop().text)
        }
    }
})
```

With such a join I wouldn't have to expose as much of the interface to the user
to do fast gets since those are going to mostly be done for the sake of joins in
any case. Still might expose a `Trampoline` based `get` though.
