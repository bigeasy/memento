class Riffled {
    constructor (riffle) {
        this._riffle = riffle[Symbol.asyncIterator]()
        this.riffled = 0
    }

    [Symbol.asyncIterator] () {
        return this
    }

    async next () {
        const next = this._iterator.next()
        this.riffled++
        return next
    }
}

class HomogenizeOuter {
    async next () {
        const next = this._iterator.next()
        if (this._riffle.count != this._riffleCount) {
            this._riffleCount = this._riffle.count
            this._queue.riffled(this)
        }
    }
}

class HomogenizeInner {
    next () {
        if (this._items.length == this._index) {
            this._queue.uninner(this)
            return null
        }
        let value = this._queue.value
        while (value != null && this._comparator.call(null, value, this._previous) < 0) {
            this._queue.next()
            value = this._queue.value
        }
        if (value == null) {
            return this._previous = this._items[this._index++]
        }
        const compare = this._comparator.call(null, this._items[this._index], value)
        if (compare == 0) {
            this._queue.next()
            this._index++
            return this._previous = value
        }
        if (compare < 0) {
            return this._previous = this._items[this._index++]
        }
        this._queue.next()
        return this._previous = value
    }
}

class ForwardIterator {
    constructor (queue) {
        this._queue.reference(this)
        this._index = 0
        this._eoa = this._queue._queues.array.length == 0
    }

    get value () {
        const array = this._queue._queues[0].array
        if (this._index < array.length) {
            return array[index]
        }
        return null
    }

    next () {
        this._index++
    }
}

module.exports = Queue
