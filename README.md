# ReSched - super simple Redis scheduling in Python

Resched is a minimal library for working with durable Queues and Schedules on Redis.

*Queues*: Resched has a Queue class that, by default, has a 2-stage pop.
This means that when you pop something off the top of the queue, it's kept
in another queue until you say you're done with it for real.

*Schedulers*: Schedulers let you solve the problem of having an event take
place at a set time in the future.  A Scheduler functions essentially like
a Queue, but inserted items get a timestamp after which they become "due",
and a `pop` operation will only return non-`None` when the first item's
due date is in the past.  This is accomplished using Redis's Sorted Set
data structure, and as such is a constant time operation.  It also uses
the 2-phased pop convention that the Resched Queue does.

*ContentType*: a really simple way to store data of various types in a
Resched collection: including JSON, ints, longs, etc.  All casting/
parsing/ etc. is handled within Resched, so you can just throw `dict`s at it
without having to think too much.

Built in NYC by the folks at GameChanger, in particular kiril (kiril@gc.io / http://github.com/kiril).
