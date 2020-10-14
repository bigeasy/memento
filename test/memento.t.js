require('proof')(16, async okay => {
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
        // Ugh. Cleveland!
        const seen = {}
        return presidencies.map((line, index) => {
            const parts = line.split(/,\s/)
            if (seen[line] == null) {
                return seen[line] = {
                    firstName: parts[0],
                    lastName: parts[1],
                    state: parts[2],
                    terms: [ index + 1 ],
                    firstTerm: index + 1
                }
            } else {
                seen[line].terms.push(index + 1)
            }
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

    const destructible = new Destructible(5000, 'memento.t')
    function createMemento (rollback = false) {
        return Memento.open({
            version: 1,
            destructible: destructible.durable('memento'),
            directory: directory,
            comparators: {
                text: (left, right) => (left > right) - (left < right)
            }
        }, async (schema) => {
            switch (schema.version) {
            case 1:
                await schema.store('employee', { lastName: [ 'text' ], firstName: Memento.ASC })
                await schema.index([ 'employee', 'state' ], { state: String })
                if (rollback) {
                    schema.rollback()
                }
                break
            }
        })
    }

    const errors = []
    try {
        await createMemento(true)
    } catch (error) {
        errors.push(/^rollback$/m.test(error.message))
    }
    okay(errors, [ true ], 'rollback open')
    const memento = await createMemento()

    destructible.durable('test', Destructible.rescue(async function () {
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

        await memento.mutator(async function (mutator) {

            mutator.set('employee', insert.shift())

            okay(await mutator.get('employee', [ 'Washington', 'George' ]), presidents[0], 'get')

            const gathered = []
            for await (const employees of mutator.forward('employee')) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local reverse')

            gathered.length = 0
            for await (const presidents of mutator.reverse('employee')) {
                for (const president of presidents) {
                    gathered.push(president)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local reverse')

            gathered.length = 0
            for await (const employees of mutator.forward([ 'employee', 'state' ])) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local index')

            gathered.length = 0
            for await (const employees of mutator.reverse([ 'employee', 'state' ])) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local index')
        })

        await memento.mutator(async function (mutator) {
            const gathered = []

            okay(await mutator.get('employee', [ 'Washington', 'George' ]), presidents[0], 'get')
            for await (const employees of mutator.forward('employee')) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'staged')

            gathered.length = 0
            for await (const employees of mutator.forward([ 'employee', 'state' ])) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'staged index')

            mutator.rollback()
        })

        await memento.mutator(async mutator => {
            for (let i = 0; i < 15; i++) {
                mutator.set('employee', insert.shift())
            }

            const gathered = []
            for await (const presidents of mutator.forward('employee')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            const states = presidents.slice(0, 16).map(president => president.state)
            const expected = {
                names: presidents.slice(0, 16).map(president => president.lastName).sort(),
                states: states.filter((state, index) => states.indexOf(state) == index).sort()
            }
            okay(gathered, expected.names, 'insert and interate many forward')

            gathered.length = 0
            for await (const presidents of mutator.reverse('employee')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
            }
            okay(gathered, expected.names.slice(0).reverse(), 'insert and interate many reverse')

            gathered.length = 0
            for await (const presidents of mutator.forward([ 'employee', 'state' ])) {
                for (const president of presidents) {
                    gathered.push(president.state)
                }
            }
            okay(gathered.filter((state, index) => {
                return gathered.indexOf(state) == index
            }), expected.states, 'insert and interate many index forward')

            gathered.length = 0
            for await (const presidents of mutator.reverse([ 'employee', 'state' ])) {
                for (const president of presidents) {
                    gathered.push(president.state)
                }
            }
            okay(gathered.filter((state, index) => {
                return gathered.indexOf(state) == index
            }), expected.states.slice(0).reverse(), 'insert and interate many index reverse')

            gathered.length = 0
            for await (const presidents of mutator.forward('employee')) {
                for (const president of presidents) {
                    gathered.push(president.lastName)
                }
                if (gathered.length == 16) {
                    presidents.reversed = true
                }
            }
            okay(gathered, expected.names.concat(expected.names.slice(0).reverse()), 'insert and interate many forward')
        })

        try {
            await memento.close()
        } catch (error) {
            console.log(error.stack)
        }
    }))

    await destructible.rejected
})
