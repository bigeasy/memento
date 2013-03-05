#!/usr/bin/env node

require('./proof')(1, function (step, tmp, ok) {
  var memento = require('../..');
  var store = memento.createStore(tmp);
  store.on('ready', function () {
    store.close(); 
    store.on('close', function () { ok(1, 'created') });
  });
  store.on('error', function (e) { throw e });
});
