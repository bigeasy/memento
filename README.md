# Memento

Memento is an Multi-Version Concurrency Control database written in CoffeeScript
for Node.js. It is based on [Strata](http://bigeasy.github.com/strata/), an
evented I/O b&#x2011;tree.

Memento is in development.

## Open and Shut

Memento creates a store which is an `EventEmitter`. You can create a new store
by providing a directory for the store to `createStore`.

```javascript
#!/usr/bin/env node

var memento = require('memento');
var store = memento.createStore(tmp);

store.on('ready', function () {
  store.close(); 
  store.on('close', function () { ok(1, 'created') });
});

store.on('error', function (e) { throw e });
```

The directory must be empty.
