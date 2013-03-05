var EventEmitter = require('events').EventEmitter, util = require('util');

var slice = [].slice;

function die () {
  console.log.apply(console, slice.call(arguments, 0));
  process.exit(1);
}

function say () { console.log.apply(console, slice.call(arguments, 0)) }

function Store () {
}
util.inherits(Store, EventEmitter);

function createStore (directory) {
  var store = new Store(directory);
  process.nextTick(function () { store.emit('ready') });
  return store;
}

Store.prototype.close = function () {
  var store = this;
  process.nextTick(function () { store.emit('close') });
}

exports.createStore = createStore;
