# Memento Design Diary

What is the minimal useful Memento library? Probably simply a key value store
with a single index.

I'm going to have a `callback` interface, with a policy of no event handlers,
they can be bolted on later, an `EventEmitter` interface, as a separate module.
