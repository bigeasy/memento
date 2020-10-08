require('proof')(7, async okay => {
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

    const destructible = new Destructible(1000, 'memento.t')
    const memento = new Memento(destructible.durable('memento'), {
        directory: directory,
        comparators: {
            text: (left, right) => (left < right) - (left > right)
        }
    })
    await memento.open(async (schema, version) => {
        switch (version) {
        case 1:
            await schema.store('employee', { lastName: [ 'text' ], firstName: Memento.ASC })
            await schema.index([ 'employee', 'state' ], { state: String })
            break
        }
    }, 1)

    destructible.durable('test', Destructible.rescue(async function () {
        const insert = presidents.slice(0)

        await memento.mutate(async function (mutator) {

            mutator.set('employee', insert.shift())

            okay(await mutator.get('employee', [ 'Washington', 'George' ]), presidents[0], 'get')

            const gathered = []
            for await (const employees of mutator.forward('employee')) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local')

            gathered.length = 0
            for await (const employees of mutator.forward([ 'employee', 'state' ])) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }

            okay(gathered, presidents.slice(0, 1), 'local index')
        })

        await memento.mutate(async function (mutator) {
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
    }))

    await memento.destructible.rejected
})
