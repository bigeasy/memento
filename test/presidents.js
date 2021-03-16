module.exports = function () {
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
