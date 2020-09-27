require('proof')(3, async okay => {
    const presidents = `George, Washington, VA
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
        const parts = line.trim().split(/,\s/)
        return { firstName: parts[0], lastName: parts[1], state: parts[2] }
    })

    const fs = require('fs').promises
    const path = require('path')

    const Destructible = require('destructible')

    const Memento = require('..')


    okay(Memento, 'require')

    const directory = path.resolve(__dirname, './tmp/memento')

    await fs.rmdir(directory, { recursive: true })
    await fs.mkdir(directory, { recursive: true })

    const destructible = new Destructible(5000, 'memento.t')
    const memento = new Memento(destructible.durable('memento'), directory)
    await memento.open(async (version) => {
        switch (version) {
        case 1:
            await memento.store('employee', { lastName: Memento.ASC, firstName: Memento.ASC })
            break
        }
    }, 1)

    destructible.durable('test', Destructible.rescue(async function () {
        const insert = presidents.slice(0)

        {
            const gathered = []

            const mutator = memento.mutator()

            do {
                gathered.length = 0

                mutator.set('employee', insert.shift())

                for await (const employees of mutator.forward('employee')) {
                    for (const employee of employees) {
                        gathered.push(employee)
                    }
                }
            } while (! await mutator.commit())

            okay(gathered, presidents.slice(0, 1), 'local')
        }

        {
            const mutator = memento.mutator()
            const gathered = []
            for await (const employees of mutator.forward('employee')) {
                for (const employee of employees) {
                    gathered.push(employee)
                }
            }
            mutator.rollback()
            okay(gathered, presidents.slice(0, 1), 'staged')
        }
    }))

    await memento.destructible.rejected
})
