const Benchmark = require('benchmark')

const suite = new Benchmark.Suite('async')

function integer (left, right) {
    return left - right
}

function floating (left, right) {
    return left - right
}

for (let i = 1; i <= 4; i++)  {
    suite.add({
        name: 'integer  ' + i,
        fn: function () {
            for (let i = 0; i <= 500; i++) {
                integer(0xffffffff, 1)
            }
        }
    })
    suite.add({
        name: 'floating ' + i,
        fn: function () {
            for (let i = 0; i <= 500; i++) {
                floating(Math.MAX_SAFE_INTEGER, 1)
            }
        }
    })
}

suite.on('cycle', function(event) {
    console.log(String(event.target));
})

suite.on('complete', function() {
    console.log('Fastest is ' + this.filter('fastest').map('name'));
})

suite.run()
