require('proof')(41, async okay => {
    const Interrupt = require('interrupt')

    const presidents = function () {
        const presidencies = `George, Washington, VA
        John, Adams, MA
        Thomas, Jefferson, VA
        James, Madison, VA
        James, Monroe, VA
        John Quincy, Adams, MA
        Andrew, Jackson, SC
        Martin, Van Buren, NY
        William Henry, Harrison, VA
        John, Tyler, VA
        James K., Polk, NC
        Zachary, Taylor, VA
        Millard, Fillmore, NY
        Franklin, Pierce, NH
        James, Buchanan, PA
        Abraham, Lincoln, KY
        Andrew, Johnson, NC
        Ulysses S., Grant, OH
        Rutherford B., Hayes, OH
        James A., Garfield, OH
        Chester A., Arthur, VT
        Grover, Cleveland, NJ
        Benjamin, Harrison, OH
        Grover, Cleveland, NJ
        William, McKinley, OH
        Theodore, Roosevelt, NY
        William H., Taft, OH
        Woodrow, Wilson, VA
        Warren G., Harding, OH
        Calvin, Coolidge, VH
        Herbert, Hoover, IA
        Franklin D., Roosevelt, NY
        Harry S., Truman, MO
        Dwight D., Eisenhower, TX
        John F., Kennedy, MA
        Lyndon B., Johnson, TX
        Richard, Nixon, CA
        Gerald, Ford, NE
        Jimmy, Carter, GA
        Ronald, Reagan, IL
        George H. W., Bush, MA
        Bill, Clinton, AR
        George W., Bush, CT
        Barack, Obama, HI
        Donald, Trump, NY`.split(/\n/).map(line => {
            return line.trim()
        })
        const seen = {}
        return presidencies.map((line, index) => {
            const parts = line.split(/,\s/)
            if (seen[line] == null) {
                return seen[line] = {
                    firstName: parts[0],
                    lastName: parts[1],
                    state: parts[2],
                    terms: [ index + 1 ]
                }
            } else {
                // Ugh. Cleveland!
                seen[line].terms.push(index + 1)
            }
        }).filter(president => president != null)
    } ()

    const states = function () {
        const states = `AL Alabama
        AK Alaska
        AZ Arizona
        AR Arkansas
        CA California
        CO Colorado
        CT Connecticut
        DE Delaware
        FL Florida
        GA Georgia
        HI Hawaii
        ID Idaho
        IL Illinois
        IN Indiana
        IA Iowa
        KS Kansas
        KY Kentucky
        LA Louisiana
        ME Maine
        MD Maryland
        MA Massachusetts
        MI Michigan
        MN Minnesota
        MS Mississippi
        MO Missouri
        MT Montana
        NE Nebraska
        NV Nevada
        NH New Hampshire
        NJ New Jersey
        NM New Mexico
        NY New York
        NC North Carolina
        ND North Dakota
        OH Ohio
        OK Oklahoma
        OR Oregon
        PA Pennsylvania
        RI Rhode Island
        SC South Carolina
        SD South Dakota
        TN Tennessee
        TX Texas
        UT Utah
        VT Vermont
        VA Virginia
        WA Washington
        WV West Virginia
        WI Wisconsin
        WY Wyoming`
        return states.split(/\n/).map(line => {
            const [ , code, name ] = /^.*([A-Z]{2})\s(.*)/.exec(line)
            return { code, name }
        })
    } ()

    const fs = require('fs').promises
    const path = require('path')

    const Destructible = require('destructible')

    const Memento = require('..')

    okay(Memento, 'require')

    const directory = path.resolve(__dirname, './tmp/memento')

    await fs.rmdir(directory, { recursive: true })
    await fs.mkdir(directory, { recursive: true })

    const destructible = new Destructible(1000, 'memento.t')
    function createMemento (version = 1, rollback = false) {
        return Memento.open({
            version: version,
            destructible: destructible.ephemeral('memento'),
            directory: directory,
            comparators: {
                text: (left, right) => (left > right) - (left < right)
            }
        }, async (schema) => {
            switch (schema.version) {
            case 1:
                await schema.store('employee', { 'terms.0': Number })
                await schema.index([ 'employee', 'moniker' ], {
                    lastName: Memento.ASC, firstName: [ 'text' ]
                })
                await schema.rename('employee', 'president')
                await schema.rename(['president', 'moniker' ], [ 'president', 'name' ])
                break
            case 2:
                await schema.store('state', { code: String })
                for (const state of states) {
                    schema.set('state', state)
                }
                break
            }
            if (rollback) {
                schema.rollback()
            }
        })
    }

    const errors = []
    try {
        await createMemento(1, true)
    } catch (error) {
        errors.push(/^rollback$/m.test(error.message))
    }
    okay(errors, [ true ], 'rollback open')

    destructible.terminal('test', Destructible.rescue(async function () {
        let memento = await createMemento()

        const insert = presidents.slice(0)

        {
            const test = []
            try {
                await memento.mutator(() => {
                    throw new Error('error')
                })
            } catch (error) {
                test.push(error.message)
            }
            okay(test, [ 'error' ], 'rethrow error')
        }

        const latch = { promise: null, resolve: null }
        latch.promise = new Promise(resolve => latch.resolve = resolve)

        let snapshot = memento.snapshot(async function (snapshot) {
            await latch.promise

            const gathered = []
            for await (const presidents of snapshot.forward('president')) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }

            okay(gathered, [], 'snapshot store empty')

            gathered.length = 0
            for await (const presidents of snapshot.forward([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }

            okay(gathered, [], 'snapshot index empty')

            okay(await snapshot.get('president', [ 1 ]), null, 'get store empty')
            okay(await snapshot.get([ 'president', 'name' ], [ 'Washington', 'George' ]), null, 'get index empty')
        })

        await memento.mutator(async function (mutator) {
            mutator.set('president', insert.shift())

            // TODO Get index.
            okay(await mutator.get('president', [ 1 ]), presidents[0], 'get')

            const gathered = []
            for await (const employees of mutator.forward('president')) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local reverse')

            gathered.length = 0
            for await (const presidents of mutator.reverse('president')) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local reverse')

            gathered.length = 0
            for await (const employees of mutator.forward([ 'president', 'name' ])) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local index')

            gathered.length = 0
            for await (const employees of mutator.reverse([ 'president', 'name' ])) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local index')
        })

        latch.resolve()
        await snapshot

        await memento.snapshot(async snapshot => {
            okay(await snapshot.get('president', [ 1 ]), presidents[0], 'get store snapshot')
            okay(await snapshot.get([ 'president', 'name' ], [ 'Washington', 'George' ]), presidents[0], 'get index snapshot')

            const gathered = []
            for await (const presidents of snapshot.forward('president')) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, presidents.slice(0, 1), 'forward store snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.reverse('president')) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, presidents.slice(0, 1), 'reverse store snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.forward([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, presidents.slice(0, 1), 'forward index snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.forward([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, presidents.slice(0, 1), 'reverse index snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.map('president', [[ 1 ], [ 2 ]])) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, [{
                key: [ 1 ],
                value: [ 1 ],
                items: [{ key: [ 1 ], value: presidents[0] }]
            }, {
                key: [ 2 ],
                value: [ 2 ],
                items: []
            }], 'store map')

            gathered.length = 0
            for await (const presidents of snapshot.map([ 'president', 'name' ], [[ 'Washington', 'George' ], [ 'Adams', 'John' ]])) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }
            okay(gathered, [{
                key: [ 'Washington', 'George' ],
                value: [ 'Washington', 'George' ],
                items: [{
                    key: [ 'Washington', 'George', 1 ],
                    value: presidents[0]
                }]
            }, {
                key: [ 'Adams', 'John' ],
                value: [ 'Adams', 'John' ],
                items: []
            }], 'index map')
        })

        await memento.mutator(async function (mutator) {
            okay(await mutator.get('president', [ 1 ]), presidents[0], 'get staged')

            const gathered = []
            for await (const employees of mutator.forward('president')) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'forward staged')

            gathered.length = 0
            for await (const employees of mutator.forward([ 'president', 'name' ])) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'forward staged index')

            mutator.rollback()
        })

        await memento.mutator(async mutator => {
            for (let i = 0; i < 15; i++) {
                mutator.set('president', insert.shift())
            }

            const gathered = []
            for await (const presidents of mutator.forward('president')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            const expected = presidents.slice(0, 16).map(president => president.lastName)
            okay(gathered, expected, 'insert and interate many forward')

            gathered.length = 0
            for await (const presidents of mutator.reverse('president')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            okay(gathered, expected.slice(0).reverse(), 'insert and interate many reverse')

            gathered.length = 0
            for await (const presidents of mutator.forward([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            okay(gathered, expected.slice(0).sort(), 'insert and interate many index forward')

            gathered.length = 0
            for await (const presidents of mutator.reverse([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            okay(gathered, expected.slice(0).sort().reverse(), 'insert and interate many index reverse')

            gathered.length = 0
            for await (const presidents of mutator.forward('president')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
                if (gathered.length == 16) {
                    presidents.reversed = true
                }
            }
            okay(gathered, expected.concat(expected.slice(0).reverse().slice(1)), 'iterator reversal')

            gathered.length = 0
            memento.cache.purge(0)
            for await (const presidents of mutator.forward([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
                if (gathered.length == 16) {
                    presidents.reversed = true
                }
            }
            okay(gathered, expected.slice(0).sort().concat(expected.slice(0).sort().reverse().slice(1)), 'index iterator reversal')
        })

        await memento.snapshot(async snapshot => {
            const gathered = []
            for await (const presidents of snapshot.forward('president')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            const expected = presidents.slice(0, 16).map(president => president.lastName)
            okay(gathered, expected, 'store many forward snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.reverse('president')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            okay(gathered, expected.slice(0).reverse(), 'store many reverse snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.forward([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            okay(gathered, expected.slice(0).sort(), 'index many forward snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.reverse([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            okay(gathered, expected.slice(0).sort().reverse(), 'index many reverse snapshot')

            gathered.length = 0
            for await (const presidents of snapshot.forward('president')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
                if (gathered.length == 16) {
                    presidents.reversed = true
                }
            }
            okay(gathered, expected.concat(expected.slice(0).reverse().slice(1)), 'iterator reversal snapshot')

            gathered.length = 0
            memento.cache.purge(0)
            for await (const presidents of snapshot.forward([ 'president', 'name' ])) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
                if (gathered.length == 16) {
                    presidents.reversed = true
                }
            }
            okay(gathered,
            expected.slice(0).sort().concat(expected.slice(0).sort().reverse().slice(1)), 'index iterator reversal snapshot')
        })

        try {
            await memento.close()
        } catch (error) {
            console.log(error.stack)
        }

        memento = await createMemento(2)

        await memento.snapshot(async snapshot => {
            const gathered = []
            let select = snapshot.forward('president').join('state', $ => [ $[0].state ])
            for await (const items of select) {
                for (const [ president, state ] of items) {
                    gathered.push([ president.lastName, state.name ])
                }
            }
            let expected = presidents.slice(0, 16).map(president => {
                const name = states.filter(state => state.code == president.state).pop().name
                return [ president.lastName,  name ]
            })
            okay(gathered, expected, 'inner join stored')
        })

        await memento.mutator(async mutator => {
            const gathered = []
            let select = mutator.forward('president').join('state', $ => [ $[0].state ])
            for await (const items of select) {
                for (const [ president, state ] of items) {
                    gathered.push([ president.lastName, state.name ])
                }
            }
            let expected = presidents.slice(0, 16).map(president => {
                const name = states.filter(state => state.code == president.state).pop().name
                return [ president.lastName,  name ]
            })
            okay(gathered, expected, 'inner join stored')
            mutator.set('president', presidents[16])
            mutator.set('president', presidents[17])
            gathered.length = 0
            select = mutator.forward('president').join('state', $ => [ $[0].state ])
            for await (const items of select) {
                memento.cache.purge(0)
                for (const [ president, state ] of items) {
                    gathered.push([ president.lastName, state.name ])
                }
            }
            expected = presidents.slice(0, 18).map(president => {
                const name = states.filter(state => state.code == president.state).pop().name
                return [ president.lastName,  name ]
            })
            okay(gathered, expected, 'inner join appened records')
            mutator.set('state', { code: 'OH', name: 'Ohio 2' })
            gathered.length = 0
            select = mutator.forward('president').join('state', $ => [ $[0].state ])
            for await (const items of select) {
                memento.cache.purge(0)
                for (const [ president, state ] of items) {
                    gathered.push([ president.lastName, state.name ])
                }
            }
            expected = presidents.slice(0, 17).map(president => {
                const name = states.filter(state => state.code == president.state).pop().name
                return [ president.lastName,  name ]
            }).concat([ [ 'Grant', 'Ohio 2' ] ])
            okay(gathered, expected, 'inner join target changed')
        })

        await memento.mutator(async mutator => {
            mutator.set('president', {
                firstName: 'Fred',
                lastName: 'Washington',
                state: 'VA',
                terms: [ 1 ]
            })
        })

        await memento.snapshot(async snapshot => {
            okay(await snapshot.get([ 'president', 'name' ], [ 'Washington', 'George' ]), null, 'get index unset')
            okay(await snapshot.get([ 'president', 'name' ], [ 'Washington', 'Fred' ]), { ...presidents[0], firstName: 'Fred' }, 'get index changed')
        })

        await memento.close()
    }))

    await destructible.rejected
})
