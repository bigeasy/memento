async function main () {
    const memento = new Memento({
        directory: path.join(__dirname, 'test/tmp/sketch')
        comparators: {
            'custom': (left, right) => +left - +right
        }
    })

    const presidents = memento.open('presidents', async schema => {
        switch (schema.version) {
        case 1:
            await schema.store('presidents', { firstTerm: Number })
            await schema.index('presidents', 'name', { lastName: 1, firstName 1 })
            schema.set('presidents', {
                firstName: 'George', lastName: 'Washington', firstTerm: 1, state: 'VA'
            })
            break
        }
    }, 1)

    presidents.mutate(async mutator => {
        mutator.set('employee', { firstName: 'John', lastName: 'Adams', state: 'MA' })
        mutator.set('employee', { firstName: 'Thomas', lastName: 'Jefferson', state: 'VA' })
        mutator.rollback()
    })

    presidents.mutate(async mutator => {
        mutator.set('employee', { firstName: 'John', lastName: 'Adams', state: 'MA' })
        mutator.set('employee', { firstName: 'Thomas', lastName: 'Jefferson', state: 'VA' })
        for await (const presidents of mutator.forward([ 'presidents', 'name' ])) {
            for (const president of presidents) {
                if (employee.state != 'MA') {
                    break
                }
            }
        }
    })

    presidents.snapshot(async snapshot => {
        for await (const presidents of snapshot.forward([ 'presidents', 'name' ])) {
            for (const president of presidents) {
                if (employee.state != 'MA') {
                    break
                }
            }
        }
    })
}
