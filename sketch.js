async function main () {
    const memento = new Memento(path.join(__dirname, 'test/tmp/sketch'))

    const employees = memento.collection('employees')

    await employees.store('name', { lastName: Memento.ASC, firstName: Memento.ASC })
    await employees.store('name', { lastName: [ String, Memento.ASC ], firstName: [ String, Memento.ASC ] })
    await employees.store('name', [
        [ 'person/lastName', String ],
        [ 'person/firstName', String ]
    ])

    await employees.index('state', { state: Memento.ASC }, { unique: false })

    const mutator = collection.mutator()

    await mutator.insert({ firstName: 'George', lastName: 'Washington', state: 'VA' })
    await mutator.insert({ firstName: 'John', lastName: 'Adams', state: 'MA' })
    await mutator.insert({ firstName: 'Thomas', lastName: 'Jefferson', state: 'VA' })

    await mutator.commit()

    const snapshot = employees.snapshot()

    for await (const employees of snapshot.index('name', Memento.MIN)) {
        for (const employee of employees) {
            console.log(employee)
        }
    }

    snapshot.release()
}
