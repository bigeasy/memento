async function main () {
    const memento = new Memento({
        directory: path.join(__dirname, 'test/tmp/sketch')
        comparators: {
            'custom': (left, right) => +left - +right
        }
    })

    const employees = memento.collection('employees')

    await employees.store('name', { lastName: Memento.ASC, firstName: Memento.ASC })
    await employees.store('name', { lastName: [ String, Memento.ASC ], firstName: [ String, Memento.ASC ] })
    await employees.store('name', [
        [ 'person/lastName', String ],
        [ 'person/firstName', String ]
    ])

    await employees.index('state', { state: Memento.ASC }, { unique: false })

    const mutator = collection.mutator()

    mutator.set('employee', { firstName: 'George', lastName: 'Washington', state: 'WV' })
    mutator.set('employee', { firstName: 'John', lastName: 'Adams', state: 'MA' })
    mutator.set('employee', { firstName: 'Thomas', lastName: 'Jefferson', state: 'VA' })

    await mutator.commit()

    const snapshot = employees.snapshot()

    for await (const employees of snapshot.forward('state')) {
        for (const employee of employees) {
            console.log(employee)
            if (employee.state != 'MA') {
                break
            }
        }
    }

    const mutator = collection.mutator()

    for await (const employees of mutator.forward('employee')) {
        while (!employees.done()) {
            if (employee.key.lastName == 'Washington') {
                const { firstName, lastName } = employee.key.value
                employee.set({ firstName, lastName, state: 'VA' })
                await mutator.flush()
            }
        }
    }

    await mutator.commit()
}
